# Processor 模块

本目录是 `bigdata-exp4` 项目中的 B 部分，主要负责 Spark/MapReduce 数据处理。目前主要实现方式是 Java + Spark。

## 功能说明

Processor 模块负责读取原始航空订票日志数据，只保留 `ITARES` 类型的记录，解析其中的订票成功信息，然后按照“小时”和“航空公司”进行统计，最后将统计结果写入 MySQL 数据库。

最终数据流如下：

```text
Kafka gds-log-topic
-> Java + Spark processor
-> MySQL bigdata_exp4.stat_result
-> 后端 / 前端页面展示
```

本模块只负责数据处理部分，不包含 `producer/`、`backend/`、`sql/` 或 `frontend/` 的实现。

## 输入字段说明

每一行日志数据使用英文逗号分隔。程序解析时主要使用以下字段，下标从 0 开始：

```text
fields[1]  日志类型，只处理 ITARES
fields[2]  日期，例如 20180830
fields[3]  小时，例如 19
fields[8]  成功订票记录，例如 CA:success;CA:success;
```

`stat_hour` 的格式为：

```text
2018-08-30 19
```

同一行中重复出现的 success 记录会被重复统计。例如：

```text
CA:success;CA:success;
```

会被统计为 `CA` 航空公司成功订票 2 次。

## Kafka 输入

Kafka 配置遵循项目 README 中统一约定：

```text
bootstrap servers: 192.168.88.101:9092
topic: gds-log-topic
```

Kafka 中的一条消息对应一行原始日志数据。

## MySQL 输出

Processor 模块将统计结果写入以下 MySQL 表：

```text
host: 192.168.88.101
port: 3306
database: bigdata_exp4
table: stat_result
username: root
password: root
```

输出字段如下：

```text
stat_hour
airline_code
success_count
```

## 本地文件测试模式

Java 程序支持本地批处理测试模式，方便在 Kafka 还没有准备好时先测试数据处理逻辑。

默认情况下，程序会优先读取：

```text
data/kafka采集数据实验.txt
```

如果该文件不存在，则会读取备用样例文件：

```text
processor/data/sample_log.txt
```

也可以通过 `--input` 参数手动指定输入文件路径。

## 打包方式

在 `processor/` 目录下执行：

```bash
mvn clean package
```

打包后的 jar 文件会生成在：

```text
processor/target/
```

## 运行本地批处理模式

在项目根目录下执行：

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode local
```

如果要指定输入文件，可以执行：

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode local \
  --input processor/data/sample_log.txt
```

本地模式会在控制台打印聚合后的统计结果，并将相同字段写入 MySQL 的 `bigdata_exp4.stat_result` 表中。

## 运行 Kafka 流处理模式

在 Kafka 和 MySQL 都启动后，执行：

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode kafka
```

Kafka 流处理模式会从 `gds-log-topic` 中读取数据，按照每个微批次对 `stat_hour` 和 `airline_code` 进行分组统计，并将 `stat_hour`、`airline_code`、`success_count` 写入 `stat_result` 表。

如果 MySQL 中已经存在相同 `stat_hour` 和 `airline_code` 的记录，程序会更新对应的 `success_count`；如果不存在，则会插入一条新的统计记录。
