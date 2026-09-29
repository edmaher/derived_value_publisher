"""Stream CSV messages into a TickConsumer."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from csv_reader import CsvReader
from derived_value_publisher import TickConsumer


OUTPUT_FIELDS = (
    "timestamp",
    "instrument",
    "derived_value",
    "base_rate",
    "spread",
    "adjustment",
    "latency",
)

def output_path(input_path: Path, timestamp: datetime | None = None) -> Path:
    """Build the timestamped output path beside the input file."""
    timestamp = timestamp or datetime.now()
    suffix = timestamp.strftime("_%Y%m%d_%H%M%S")
    return input_path.with_name(f"{input_path.stem}{suffix}{input_path.suffix}")


class CsvEntryWriter:
    """Consume ready events and write them as CSV records."""

    def __init__(self, output_file: Any) -> None:
        self.writer = csv.DictWriter(output_file, fieldnames=OUTPUT_FIELDS)
        self.writer.writeheader()

    def consume(self, entry: dict[str, Any]) -> None:
        self.writer.writerow(
            {
                "timestamp": entry["last_updated_ts"],
                "instrument": entry["instrument"],
                "derived_value": entry["derived_value"],
                "base_rate": entry["base_rate"],
                "spread": entry["spread"],
                "adjustment": entry["adjustment"],
                "latency": entry["latency_ms"],
            }
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Process tick messages from a CSV file.")
    parser.add_argument("csv_file", type=Path, help="Path to the CSV input file")
    parser.add_argument(
        "--ready-threshold-ms",
        type=int,
        default=100,
        help="Simulated milliseconds between consumer-ready callbacks",
    )
    args = parser.parse_args()

    reader = CsvReader()
    result_path = output_path(args.csv_file)

    with args.csv_file.open(newline="") as csv_file, result_path.open(
        "w", newline=""
    ) as result_file:
        writer = CsvEntryWriter(result_file)
        consumer = TickConsumer(writer)
        last_consumer_ready_time = 0
        for sequence_number, message in enumerate(
            reader.read_file(csv_file), start=1
        ):
            sim_last_tick_time = message["timestamp_ms"]
            message["sequence_number"] = sequence_number
            consumer.OnMessage(message)

            if sim_last_tick_time - last_consumer_ready_time > args.ready_threshold_ms:
                consumer.OnConsumerReady()
                last_consumer_ready_time = sim_last_tick_time


if __name__ == "__main__":
    main()