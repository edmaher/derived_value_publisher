"""Header-aware, line-oriented CSV input."""

from __future__ import annotations

import csv
import io
import sys
from typing import Any, Iterator, TextIO


FIELDS = ("timestamp_ms", "instrument", "input_type", "value")


class CsvReader:
    """Parse records from a headered CSV file or one data line."""

    def read_file(self, csv_file: TextIO) -> Iterator[dict[str, Any]]:
        """Yield typed records from a headered CSV file."""
        csv_rows = csv.reader(csv_file)
        try:
            headers = next(csv_rows)
        except StopIteration:
            return

        if headers != list(FIELDS):
            raise ValueError("CSV header does not match expected fields")

        for line_number, values in enumerate(csv_rows, start=2):
            if not values or any(value == "" for value in values):
                print(
                    f"Skipping invalid CSV row {line_number}: {values}",
                    file=sys.stderr,
                )
                continue
            if len(values) != len(headers):
                print(
                    f"Skipping invalid CSV row {line_number}: {values}",
                    file=sys.stderr,
                )
                continue

            record = dict(zip(headers, values))
            try:
                yield {
                    "timestamp_ms": int(record["timestamp_ms"]),
                    "instrument": record["instrument"],
                    "input_type": record["input_type"],
                    "value": float(record["value"]),
                }
            except (TypeError, ValueError):
                print(
                    f"Skipping invalid CSV row {line_number}: {values}",
                    file=sys.stderr,
                )

    def read_line(self, line: str) -> dict[str, Any]:
        """Parse one CSV data line into a dictionary."""
        rows = list(csv.reader(io.StringIO(line)))
        if len(rows) != 1 or len(rows[0]) != len(FIELDS):
            raise ValueError("expected one CSV row with four fields")

        timestamp_ms, instrument, input_type, value = rows[0]
        if not instrument:
            raise ValueError("instrument must not be empty")
        if not input_type:
            raise ValueError("input_type must not be empty")

        return {
            "timestamp_ms": int(timestamp_ms),
            "instrument": instrument,
            "input_type": input_type,
            "value": float(value),
        }
