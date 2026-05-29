#!/usr/bin/env python3
"""
Spark Structured Streaming processor for airline booking success statistics.

Input:
    Kafka topic containing one raw log line per message.

Output:
    Incremental upserts into MySQL table airline_success_stat.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "processor_config.json"

AIRLINE_CODE_PATTERN = re.compile(r"^[A-Z0-9]{2,3}$")


def parse_success_records(raw_line: str | None) -> List[Tuple[str, str]]:
    """Parse one raw log line into success detail records.

    A single ITARES line may contain repeated success entries, for example
    "CA:success;CA:success;". Repeated entries are returned repeatedly so the
    downstream aggregation counts them as 2 successes.
    """
    if not raw_line:
        return []

    fields = raw_line.rstrip("\n").split(",")
    if len(fields) <= 8:
        return []

    log_type = fields[1].strip()
    if log_type != "ITARES":
        return []

    date_value = fields[2].strip()
    hour_value = fields[3].strip().zfill(2)
    success_value = fields[8].strip()

    if len(date_value) != 8 or not date_value.isdigit():
        return []
    if not hour_value.isdigit() or len(hour_value) != 2:
        return []

    stat_hour = f"{date_value[0:4]}-{date_value[4:6]}-{date_value[6:8]} {hour_value}"

    records: List[Tuple[str, str]] = []
    for item in success_value.split(";"):
        item = item.strip()
        if not item or ":" not in item:
            continue

        airline_code, status = item.split(":", 1)
        airline_code = airline_code.strip().upper()
        status = status.strip().lower()

        if status == "success" and AIRLINE_CODE_PATTERN.match(airline_code):
            records.append((stat_hour, airline_code))

    return records


def load_config(config_path: Path) -> Dict:
    with config_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def build_success_detail_stream(spark: "SparkSession", config: Dict) -> "DataFrame":
    from pyspark.sql.functions import col, explode, udf
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    success_record_schema = ArrayType(
        StructType(
            [
                StructField("stat_hour", StringType(), nullable=False),
                StructField("airline_code", StringType(), nullable=False),
            ]
        )
    )

    kafka_config = config["kafka"]
    parse_success_udf = udf(parse_success_records, success_record_schema)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", kafka_config.get("topic", "flight_log"))
        .option("startingOffsets", kafka_config.get("starting_offsets", "latest"))
        .load()
    )

    return (
        raw_stream.selectExpr("CAST(value AS STRING) AS raw_line")
        .select(explode(parse_success_udf(col("raw_line"))).alias("success_record"))
        .select(
            col("success_record.stat_hour").alias("stat_hour"),
            col("success_record.airline_code").alias("airline_code"),
        )
    )


def _iter_partition_rows(rows: Iterable) -> List[Tuple[str, str, int]]:
    return [
        (row["stat_hour"], row["airline_code"], int(row["success_count"]))
        for row in rows
    ]


def upsert_partition(rows: Iterable, mysql_config: Dict) -> None:
    batch_rows = _iter_partition_rows(rows)
    if not batch_rows:
        return

    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError(
            "PyMySQL is required on the Spark driver and executors. "
            "Install it with: pip install pymysql"
        ) from exc

    connection = pymysql.connect(
        host=mysql_config["host"],
        port=int(mysql_config.get("port", 3306)),
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        charset=mysql_config.get("charset", "utf8mb4"),
        autocommit=False,
    )

    table_name = mysql_config.get("table", "airline_success_stat")
    sql = f"""
        INSERT INTO {table_name} (stat_hour, airline_code, success_count)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            success_count = success_count + VALUES(success_count),
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, batch_rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def write_micro_batch(batch_df: "DataFrame", batch_id: int, mysql_config: Dict) -> None:
    if batch_df.rdd.isEmpty():
        return

    increment_df = batch_df.groupBy("stat_hour", "airline_code").count()
    increment_df = increment_df.withColumnRenamed("count", "success_count")

    increment_df.foreachPartition(
        lambda rows: upsert_partition(rows, mysql_config)
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream flight log success statistics from Kafka to MySQL."
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to processor_config.json. Copy the example config and fill it locally.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    config = load_config(Path(args.config))

    spark_config = config.get("spark", {})
    spark = (
        SparkSession.builder.appName(
            spark_config.get("app_name", "AirlineSuccessStructuredStreaming")
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(spark_config.get("log_level", "WARN"))

    success_detail_stream = build_success_detail_stream(spark, config)

    query = (
        success_detail_stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_micro_batch(
                batch_df, batch_id, config["mysql"]
            )
        )
        .option("checkpointLocation", config["spark"]["checkpoint_location"])
        .trigger(processingTime=config["spark"].get("trigger_interval", "10 seconds"))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
