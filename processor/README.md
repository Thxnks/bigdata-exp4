# Processor Module

This directory contains the B part of the project: Spark Structured Streaming
processing for airline booking success statistics.

## Function

The final runtime flow is:

```text
Kafka topic flight_log
-> Spark Structured Streaming
-> parse ITARES success records
-> count success by hour and airline
-> upsert incremental counts into MySQL airline_success_stat
```

The program counts repeated success entries separately. For example:

```text
CA:success;CA:success;
```

is counted as 2 successes for airline `CA`.

## Input Log Fields

Each Kafka message should be one raw comma-separated log line. The parser uses
these zero-based fields:

```text
fields[1]  log type, only ITARES is processed
fields[2]  date, for example 20180830
fields[3]  hour, for example 19
fields[8]  success entries, for example CA:success;CA:success;
```

The output hour format is:

```text
2018-08-30 19
```

## MySQL Output

The output table is `airline_success_stat`.

```text
stat_hour
airline_code
success_count
updated_at
```

The primary key is:

```text
(stat_hour, airline_code)
```

Each streaming micro-batch is grouped inside `foreachBatch`, then written with:

```sql
INSERT ... ON DUPLICATE KEY UPDATE
success_count = success_count + VALUES(success_count)
```

This avoids inserting duplicate rows and avoids adding cumulative Spark results
multiple times.

## Local Test Without Kafka

Run the parser and aggregation test with the sample log file:

```bash
python processor/local_batch_test.py --limit 1000
```

Use `--limit 0` to process the whole sample file:

```bash
python processor/local_batch_test.py --limit 0
```

## Streaming Run Later

Copy the example config and fill in local values. Do not commit the real config.

```bash
cp processor/config/processor_config.example.json processor/config/processor_config.json
```

Create the MySQL table:

```bash
mysql -u <user> -p <database> < processor/sql/airline_success_stat.sql
```

After Kafka is ready, run the streaming processor with Spark. Version numbers
must match the Spark and Scala versions on the Ubuntu VM.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version> \
  processor/spark_streaming_airline_success.py \
  --config processor/config/processor_config.json
```

PyMySQL must be available on the Spark driver and executors:

```bash
pip install pymysql
```

## Interfaces To Confirm

Confirm with A:

```text
Kafka bootstrap server address
topic name flight_log
one message equals one raw log line
message value is plain UTF-8 text
starting offset for demo, latest or earliest
```

Confirm with C/D:

```text
MySQL host, database, user, and permission scope
whether frontend expects stat_hour as 2018-08-30 19
whether table name airline_success_stat is final
```
