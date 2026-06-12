# Processor Module

This directory is the B part of `bigdata-exp4`: Spark/MapReduce data
processing. The current main implementation uses Java + Spark.

## Function

The processor reads raw airline booking log lines, keeps only `ITARES` records,
parses booking success entries, counts successful bookings by hour and airline,
and writes the result to MySQL.

Final data flow:

```text
Kafka gds-log-topic
-> Java + Spark processor
-> MySQL bigdata_exp4.stat_result
-> backend/frontend display
```

This module only handles data processing. It does not implement `producer/`,
`backend/`, `sql/`, or `frontend/`.

## Input Fields

Each log line is comma-separated. The parser uses these zero-based fields:

```text
fields[1]  log type, only ITARES is processed
fields[2]  date, for example 20180830
fields[3]  hour, for example 19
fields[8]  success entries, for example CA:success;CA:success;
```

`stat_hour` is formatted as:

```text
2018-08-30 19
```

Repeated success entries in the same line are counted repeatedly. For example,
`CA:success;CA:success;` is counted as 2 successful bookings for `CA`.

## Kafka Input

The Kafka settings follow the project README unified convention:

```text
bootstrap servers: 192.168.88.101:9092
topic: gds-log-topic
```

One Kafka message should contain one raw log line.

## MySQL Output

The processor writes to:

```text
host: 192.168.88.101
port: 3306
database: bigdata_exp4
table: stat_result
username: root
password: root
```

Output fields:

```text
stat_hour
airline_code
success_count
```

## Local File Test

The Java program supports a local batch mode for testing before Kafka is ready.
By default, it first tries:

```text
data/kafka采集数据实验.txt
```

If that file does not exist, it falls back to:

```text
processor/data/sample_log.txt
```

You can also pass a file path explicitly with `--input`.

## Build

Run from the `processor/` directory:

```bash
mvn clean package
```

The packaged jar is generated under:

```text
processor/target/
```

## Run Local Batch Mode

Run from the project root:

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode local
```

Run with an explicit input file:

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode local \
  --input processor/data/sample_log.txt
```

Local mode prints the aggregated result and writes the same fields to
`bigdata_exp4.stat_result`.

## Run Kafka Streaming Mode

After Kafka and MySQL are ready:

```bash
spark-submit \
  --class com.bigdata.processor.AirlineSuccessProcessor \
  processor/target/processor-1.0.0.jar \
  --mode kafka
```

Kafka streaming mode reads `gds-log-topic`, aggregates each micro-batch by
`stat_hour` and `airline_code`, and writes `stat_hour, airline_code,
success_count` to `stat_result`. If a row for the same `stat_hour` and
`airline_code` already exists, the processor updates `success_count`; otherwise
it inserts a new row.
