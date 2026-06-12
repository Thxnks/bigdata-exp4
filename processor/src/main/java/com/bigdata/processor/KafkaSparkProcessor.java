package com.bigdata.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

/**
 * 角色 B：Spark Structured Streaming 消费 Kafka，统计每小时每个航空公司预订成功数，写入 MySQL。
 *
 * 数据流：Kafka(gds-log-topic) -> 解析 ITARES 日志的 "航司:success" -> 按(小时,航司)聚合 -> MySQL stat_result
 *
 * 日志格式(逗号分隔)，示例：
 *   TB.P1780,ITARES,20180830,19,19:45:36:257,,,1,CA:success;CA:success;
 *   字段[1]=日志类型(只取 ITARES)，[2]=日期20180830，[3]=小时19，行尾=航司:success 对(分号分隔)
 *   stat_hour 截断到小时，如 "2018-08-30 19"
 *
 * 运行(在 node1，spark-submit)：
 *   /export/server/spark/bin/spark-submit \
 *     --class com.bigdata.processor.KafkaSparkProcessor \
 *     --master yarn --deploy-mode client \
 *     processor.jar [bootstrap] [jdbcUrl] [checkpointDir]
 */
public class KafkaSparkProcessor {

    private static final String TOPIC = "gds-log-topic";
    // 只匹配 ITARES 日志尾部的 "两位航司码:success"，分组1为航司码。数据干净，无需 lookbehind。
    private static final String SUCCESS_REGEX = "([A-Za-z0-9]{2}):success";

    public static void main(String[] args) throws Exception {
        String bootstrap  = args.length > 0 ? args[0] : "node1:9092";
        String jdbcUrl    = args.length > 1 ? args[1]
                : "jdbc:mysql://node1:3306/bigdata_exp4?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        String checkpoint = args.length > 2 ? args[2] : "/home/hadoop/spark-ckpt-gds";
        // 演示用：每个微批最多读多少条 Kafka 消息(空=不限)；批次间隔秒(空=尽快)。
        // 设了这两个，数据即使已全在 Kafka，也会一小批一小批处理，前端可见渐进增长。
        String maxPerTrigger = args.length > 3 ? args[3] : "";
        String triggerSec    = args.length > 4 ? args[4] : "";

        final String url = jdbcUrl;

        SparkSession spark = SparkSession.builder()
                .appName("GdsKafkaSparkProcessor")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // 1. 从 Kafka 读流，取消息体为字符串
        DataStreamReader reader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest");
        if (!maxPerTrigger.isEmpty()) {
            reader = reader.option("maxOffsetsPerTrigger", maxPerTrigger);  // 每批限流
        }
        Dataset<Row> kafka = reader.load();

        Dataset<Row> lines = kafka.selectExpr("CAST(value AS STRING) AS line");

        // 2. 解析 + 过滤 + 拆出每条 success，按(小时,航司)聚合
        Dataset<Row> stats = lines
                .withColumn("arr", split(col("line"), ","))
                .filter(expr("element_at(arr, 2) = 'ITARES'"))          // 只要 ITARES 日志
                .withColumn("d", expr("element_at(arr, 3)"))            // 日期 20180830
                .withColumn("h", expr("element_at(arr, 4)"))            // 小时 19
                .filter(expr("length(d) = 8 AND h is not null"))
                .withColumn("stat_hour",
                        expr("concat(substr(d,1,4),'-',substr(d,5,2),'-',substr(d,7,2),' ',h)"))
                .withColumn("airline_code",
                        explode(expr("regexp_extract_all(line, '" + SUCCESS_REGEX + "', 1)")))
                .groupBy(col("stat_hour"), col("airline_code"))
                .count()
                .withColumnRenamed("count", "success_count");

        // 3. 每个微批把当前完整统计结果覆盖写入 MySQL(truncate 保留表结构, 不 drop)
        DataStreamWriter<Row> writer = stats.writeStream()
                .outputMode("complete")
                .foreachBatch((Dataset<Row> batchDF, Long batchId) -> {
                    long n = batchDF.count();
                    System.out.println("[batch " + batchId + "] 写入 " + n + " 条 (小时,航司) 统计 -> MySQL stat_result");
                    batchDF.select(
                                    col("stat_hour"),
                                    col("airline_code"),
                                    col("success_count").cast("int").as("success_count"))
                            .write()
                            .mode(SaveMode.Overwrite)
                            .format("jdbc")
                            .option("url", url)
                            .option("dbtable", "stat_result")
                            .option("user", "root")
                            .option("password", "root")
                            .option("driver", "com.mysql.cj.jdbc.Driver")
                            .option("truncate", "true")   // 用 TRUNCATE 而非 DROP，保住 id/created_at 列
                            .save();
                })
                .option("checkpointLocation", checkpoint);
        if (!triggerSec.isEmpty()) {
            writer = writer.trigger(Trigger.ProcessingTime(triggerSec + " seconds"));  // 每隔 N 秒一个微批
        }
        StreamingQuery query = writer.start();

        query.awaitTermination();
    }
}
