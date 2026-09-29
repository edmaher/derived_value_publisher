"""Line-oriented CSV input and per-instrument tick state."""

from __future__ import annotations

import math
from collections import deque
from typing import Any, Protocol

from csv_reader import CsvReader


VALUE_FIELDS = {"base_rate", "spread", "adjustment"}


class EventConsumer(Protocol):
    def consume(self, entry: dict[str, Any]) -> None:
        ...


class TickConsumer:
    """Maintain the latest values for each instrument."""

    def __init__(self, event_consumer: EventConsumer) -> None:
        self.instruments: dict[str, dict[str, Any]] = {}
        self.dirty_queue: deque[dict[str, Any]] = deque()
        self.current_time = 0
        self.event_consumer = event_consumer

    def OnConsumerReady(self) -> None:
        """Publish and remove the oldest dirty instrument entry."""
        if not self.dirty_queue:
            return

        entry = self.dirty_queue.popleft()
        entry["dirty"] = False
        entry["latency_ms"] = self.current_time - entry["last_updated_ts"]
        self.event_consumer.consume(entry)

    def OnMessage(self, message: dict[str, Any]) -> None:
        """Apply one parsed message to its instrument state."""
        instrument = message["instrument"]
        self.current_time = max(self.current_time, message["timestamp_ms"])
        entry = self.instruments.get(instrument)
        if entry is None:
            entry = self._new_entry(instrument)
            self.instruments[instrument] = entry
        entry["last_updated_ts"] = message["timestamp_ms"]
        entry["sequence_number"] = message.get("sequence_number")

        input_type = message["input_type"]
        if input_type in VALUE_FIELDS:
            entry[input_type] = message["value"]

        base_rate = entry["base_rate"]
        spread = entry["spread"]
        adjustment = entry["adjustment"]
        derived_value = base_rate + spread + adjustment
        entry["derived_value"] = derived_value

        if not math.isnan(derived_value) and not entry["dirty"]:
            entry["dirty"] = True
            self.dirty_queue.append(entry)

    @staticmethod
    def _new_entry(instrument: str) -> dict[str, Any]:
        return {
            "instrument": instrument,
            "last_updated_ts": None,
            "sequence_number": None,
            "base_rate": math.nan,
            "spread": math.nan,
            "adjustment": math.nan,
            "derived_value": math.nan,
            "valid": False,
            "dirty": False,
            "latency_ms": None,
        }