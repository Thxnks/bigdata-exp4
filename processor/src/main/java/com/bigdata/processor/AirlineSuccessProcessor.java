package com.bigdata.processor;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

// Spark 处理程序入口，负责读取日志、聚合统计结果并写入 MySQL。
public class AirlineSuccessProcessor {
    private static final String MODE_LOCAL = "local";
    private static final String MODE_KAFKA = "kafka";

    // 程序入口，根据参数选择本地文件模式或 Kafka 流模式。
    public static void main(String[] args) throws Exception {
        // 从 application.properties 读取 Kafka、MySQL 和 Spark 配置。
        Properties config = loadConfig();
        // 默认使用本地文件模式，便于在 Kafka 未启动时测试。
        String mode = getArg(args, "--mode", MODE_LOCAL);

        // 创建 SparkSession 作为 Spark 任务的执行入口。
        SparkSession spark = SparkSession.builder()
                .appName(config.getProperty("spark.app.name"))
                .getOrCreate();

        // 设置 Spark 日志级别，减少运行时无关输出。
        spark.sparkContext().setLogLevel(config.getProperty("spark.log.level", "WARN"));

        // Kafka 模式从消息队列持续读取日志。
        if (MODE_KAFKA.equalsIgnoreCase(mode)) {
            runKafkaStream(spark, config);
        // 本地模式从文本文件读取日志用于测试。
        } else if (MODE_LOCAL.equalsIgnoreCase(mode)) {
            runLocalFileBatch(spark, config, getArg(args, "--input", null));
        } else {
            throw new IllegalArgumentException("Unsupported mode: " + mode + ". Use local or kafka.");
        }
    }

    private static void runKafkaStream(SparkSession spark, Properties config) throws Exception {
        // 从 Kafka topic 读取原始日志消息，并把 value 转成字符串。
        Dataset<Row> kafkaLines = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getProperty("kafka.bootstrap.servers"))
                .option("subscribe", config.getProperty("kafka.topic"))
                .option("startingOffsets", config.getProperty("kafka.starting.offsets", "latest"))
                .load()
                .selectExpr("CAST(value AS STRING) AS raw_line");

        // 将原始日志行解析为 success 明细记录。
        Dataset<AirlineSuccessRecord> successRecords = parseLines(kafkaLines);

        // 每个微批次内先聚合增量结果，再写入 MySQL。
        StreamingQuery query = successRecords.writeStream()
                .foreachBatch((batchDataset, batchId) -> {
                    // 按 stat_hour 和 airline_code 统计当前微批次的 success_count。
                    Dataset<Row> statResult = aggregate(batchDataset);
                    if (!statResult.isEmpty()) {
                        // 将当前微批次统计结果写入 MySQL 表。
                        writeToMysql(statResult, config);
                    }
                })
                // checkpoint 用于记录流处理进度。
                .option("checkpointLocation", config.getProperty("spark.checkpoint.location"))
                // 设置 Spark Structured Streaming 的微批触发间隔。
                .trigger(Trigger.ProcessingTime(config.getProperty("spark.trigger.interval")))
                .start();

        // 保持流任务持续运行。
        query.awaitTermination();
    }

    private static void runLocalFileBatch(SparkSession spark, Properties config, String inputPath) {
        // 解析本地输入文件路径，未传入时使用默认路径或样例文件。
        String resolvedInputPath = inputPath == null || inputPath.trim().isEmpty()
                ? resolveDefaultInputPath(config)
                : inputPath;

        // 读取本地文本文件，每一行作为一条原始日志。
        Dataset<Row> localLines = spark.read()
                .text(resolvedInputPath)
                .toDF("raw_line");

        // 复用同一套解析和聚合逻辑生成统计结果。
        Dataset<Row> statResult = aggregate(parseLines(localLines));
        // 在控制台展示本地测试的统计结果。
        statResult.orderBy("stat_hour", "airline_code").show(200, false);
        // 批处理为完整统计结果，同时落三种存储：MySQL + HBase + HDFS。
        writeToMysql(statResult, config);
        writeToHBase(statResult, config);
        writeToHdfs(statResult, config);
    }

    private static Dataset<AirlineSuccessRecord> parseLines(Dataset<Row> lines) {
        // 将 raw_line 字段转成字符串数据集，再逐行解析为 success 明细。
        return lines.select("raw_line")
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, AirlineSuccessRecord>) rawLine -> {
                    // LogParser 负责过滤 ITARES 并解析 success 信息。
                    LogParser parser = new LogParser();
                    List<AirlineSuccessRecord> records = parser.parseSuccessRecords(rawLine);
                    return records.iterator();
                }, Encoders.bean(AirlineSuccessRecord.class));
    }

    private static Dataset<Row> aggregate(Dataset<AirlineSuccessRecord> successRecords) {
        // 按小时和航空公司分组统计 success 数量。
        return successRecords
                .groupBy("statHour", "airlineCode")
                .count()
                // 将 Java Bean 字段名转换为 README 约定的输出字段名。
                .withColumnRenamed("statHour", "stat_hour")
                .withColumnRenamed("airlineCode", "airline_code")
                .withColumnRenamed("count", "success_count")
                // 只保留写入 MySQL 所需的三个统计字段。
                .select("stat_hour", "airline_code", "success_count");
    }

    private static void writeToMysql(Dataset<Row> statResult, Properties config) {
        // 每个 Spark 分区使用一个数据库连接写入统计结果。
        statResult.foreachPartition(rows -> {
            // 加载 MySQL JDBC 驱动。
            Class.forName(config.getProperty("mysql.driver"));
            try (Connection connection = DriverManager.getConnection(
                    config.getProperty("mysql.url"),
                    config.getProperty("mysql.username"),
                    config.getProperty("mysql.password"));
                 PreparedStatement updateStatement = connection.prepareStatement(
                         "UPDATE " + config.getProperty("mysql.table")
                                 + " SET success_count = success_count + ?"
                                 + " WHERE stat_hour = ? AND airline_code = ?");
                 PreparedStatement insertStatement = connection.prepareStatement(
                         "INSERT INTO " + config.getProperty("mysql.table")
                                 + " (stat_hour, airline_code, success_count)"
                                 + " VALUES (?, ?, ?)")) {

                // 关闭自动提交，分区内多条写入统一提交。
                connection.setAutoCommit(false);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    // 从 Spark Row 中取出 MySQL 需要的三个字段。
                    String statHour = row.getAs("stat_hour");
                    String airlineCode = row.getAs("airline_code");
                    Number successCountValue = row.getAs("success_count");
                    long successCount = successCountValue.longValue();

                    // 先尝试按 stat_hour 和 airline_code 累加已有记录。
                    updateStatement.setLong(1, successCount);
                    updateStatement.setString(2, statHour);
                    updateStatement.setString(3, airlineCode);
                    int updatedRows = updateStatement.executeUpdate();

                    // 如果没有已有记录，则插入新的统计行。
                    if (updatedRows == 0) {
                        insertStatement.setString(1, statHour);
                        insertStatement.setString(2, airlineCode);
                        insertStatement.setLong(3, successCount);
                        insertStatement.executeUpdate();
                    }
                }
                // 提交当前分区的全部写入。
                connection.commit();
            }
        });
    }

    // 将完整统计结果写入 HBase 表(rowkey = stat_hour#airline_code)，作为 HDFS/列式存储加分项。
    private static void writeToHBase(Dataset<Row> statResult, Properties config) {
        String quorum = config.getProperty("hbase.zookeeper.quorum");
        String tableName = config.getProperty("hbase.table");
        // 未配置 HBase 时跳过，不影响 MySQL 主流程。
        if (quorum == null || quorum.trim().isEmpty() || tableName == null || tableName.trim().isEmpty()) {
            return;
        }
        String zkPort = config.getProperty("hbase.zookeeper.port", "2181");
        // 每个分区一个 HBase 连接，批量 Put 写入。
        statResult.foreachPartition(rows -> {
            org.apache.hadoop.conf.Configuration hconf = org.apache.hadoop.hbase.HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum", quorum);
            hconf.set("hbase.zookeeper.property.clientPort", zkPort);
            try (org.apache.hadoop.hbase.client.Connection conn =
                         org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hconf);
                 org.apache.hadoop.hbase.client.Table table =
                         conn.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))) {
                byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes("cf");
                List<org.apache.hadoop.hbase.client.Put> puts = new ArrayList<>();
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String statHour = row.getAs("stat_hour");
                    String airlineCode = row.getAs("airline_code");
                    long successCount = ((Number) row.getAs("success_count")).longValue();
                    // rowkey 用 小时#航司 组合，保证唯一且便于 scan。
                    org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(
                            org.apache.hadoop.hbase.util.Bytes.toBytes(statHour + "#" + airlineCode));
                    put.addColumn(cf, org.apache.hadoop.hbase.util.Bytes.toBytes("stat_hour"),
                            org.apache.hadoop.hbase.util.Bytes.toBytes(statHour));
                    put.addColumn(cf, org.apache.hadoop.hbase.util.Bytes.toBytes("airline_code"),
                            org.apache.hadoop.hbase.util.Bytes.toBytes(airlineCode));
                    put.addColumn(cf, org.apache.hadoop.hbase.util.Bytes.toBytes("success_count"),
                            org.apache.hadoop.hbase.util.Bytes.toBytes(String.valueOf(successCount)));
                    puts.add(put);
                }
                if (!puts.isEmpty()) {
                    table.put(puts);
                }
            }
        });
        System.out.println("[HBase] 统计结果已写入表 " + tableName);
    }

    // 将完整统计结果以 CSV 写入 HDFS，满足"结果存储到 HDFS"的实验要求。
    private static void writeToHdfs(Dataset<Row> statResult, Properties config) {
        String path = config.getProperty("hdfs.output.path");
        // 未配置 HDFS 输出路径时跳过。
        if (path == null || path.trim().isEmpty()) {
            return;
        }
        // 合并为单文件、带表头、覆盖旧输出，便于 hdfs dfs -cat 查看。
        statResult.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(path);
        System.out.println("[HDFS] 统计结果已写入 " + path);
    }

    private static Properties loadConfig() throws Exception {
        // 从 classpath 中读取 application.properties 配置文件。
        Properties properties = new Properties();
        try (InputStream inputStream = AirlineSuccessProcessor.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (inputStream == null) {
                throw new IllegalStateException("application.properties not found in classpath.");
            }
            properties.load(inputStream);
        }
        return properties;
    }

    private static String resolveDefaultInputPath(Properties config) {
        // 优先使用项目统一原始数据文件，不存在时使用 processor 样例文件。
        String primary = config.getProperty("local.input.primary");
        String fallback = config.getProperty("local.input.fallback");

        if (primary != null && new java.io.File(primary).exists()) {
            return primary;
        }
        return fallback;
    }

    private static String getArg(String[] args, String name, String defaultValue) {
        // 从命令行参数中读取指定参数值。
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
