#!/usr/bin/env python3
"""
Local parser and aggregation test for processor/data/sample_log.txt.

This script does not require Kafka or MySQL. It reuses the same parser as the
Spark streaming program and prints aggregated success counts.
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Sequence

from spark_streaming_airline_success import parse_success_records


DEFAULT_SAMPLE_PATH = Path(__file__).resolve().parent / "data" / "sample_log.txt"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate airline success parsing with a local log file."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_SAMPLE_PATH),
        help="Path to the raw sample log file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only read the first N lines. Use 0 to read the whole file.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Print at most N aggregated rows. Use 0 to print all rows.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    input_path = Path(args.input)
    counts: Counter[tuple[str, str]] = Counter()

    with input_path.open("r", encoding="utf-8", errors="replace") as fp:
        for line_number, raw_line in enumerate(fp, start=1):
            if args.limit and line_number > args.limit:
                break
            for stat_hour, airline_code in parse_success_records(raw_line):
                counts[(stat_hour, airline_code)] += 1

    rows = sorted(
        (
            (stat_hour, airline_code, success_count)
            for (stat_hour, airline_code), success_count in counts.items()
        ),
        key=lambda item: (item[0], item[1]),
    )

    print("stat_hour,airline_code,success_count")
    for index, (stat_hour, airline_code, success_count) in enumerate(rows, start=1):
        if args.top and index > args.top:
            break
        print(f"{stat_hour},{airline_code},{success_count}")

    print(f"\naggregated_rows={len(rows)}")
    print(f"total_success_count={sum(counts.values())}")


if __name__ == "__main__":
    main()
