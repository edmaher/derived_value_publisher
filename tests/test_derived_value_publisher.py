import math
from datetime import datetime
from io import StringIO
from pathlib import Path
import sys

import pytest

import main
from main import CsvEntryWriter, OUTPUT_FIELDS, output_path
from csv_reader import CsvReader
from derived_value_publisher import TickConsumer


class EventCollector:
    def __init__(self):
        self.entries = []

    def consume(self, entry):
        self.entries.append(entry)


def test_output_path_adds_timestamp_before_extension():
    assert output_path(
        Path("market_inputs.csv"), datetime(2026, 9, 21, 14, 30, 5)
    ) == Path("market_inputs_20260921_143005.csv")


def test_output_fields_are_in_requested_order():
    assert OUTPUT_FIELDS == (
        "timestamp",
        "instrument",
        "derived_value",
        "base_rate",
        "spread",
        "adjustment",
        "latency",
    )


def test_csv_entry_writer_consumes_entries():
    output = StringIO()
    CsvEntryWriter(output).consume(
        {
            "last_updated_ts": 123,
            "instrument": "EURUSD",
            "derived_value": 1.65,
            "base_rate": 1.5,
            "spread": 0.25,
            "adjustment": -0.1,
            "latency_ms": 7,
        }
    )

    assert output.getvalue().splitlines() == [
        "timestamp,instrument,derived_value,base_rate,spread,adjustment,latency",
        "123,EURUSD,1.65,1.5,0.25,-0.1,7",
    ]


def test_main_assigns_sequence_and_publishes_ready_entry(tmp_path, monkeypatch):
    input_path = tmp_path / "market_inputs.csv"
    input_path.write_text(
        "timestamp_ms,instrument,input_type,value\n"
        "1,EURUSD,base_rate,1.5\n"
        "2,EURUSD,spread,0.25\n"
        "3,EURUSD,adjustment,-0.1\n"
        "104,GBPUSD,base_rate,2.0\n"
    )
    result_path = tmp_path / "result.csv"
    received_messages = []

    class RecordingConsumer(TickConsumer):
        def OnMessage(self, message):
            received_messages.append(message.copy())
            super().OnMessage(message)

    monkeypatch.setattr(main, "TickConsumer", RecordingConsumer)
    monkeypatch.setattr(main, "output_path", lambda _: result_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", str(input_path), "--ready-threshold-ms", "100"],
    )

    main.main()

    assert [message["sequence_number"] for message in received_messages] == [1, 2, 3, 4]
    assert result_path.read_text().splitlines() == [
        "timestamp,instrument,derived_value,base_rate,spread,adjustment,latency",
        "3,EURUSD,1.65,1.5,0.25,-0.1,101",
    ]


def test_reader_parses_one_line():
    message = CsvReader().read_line("123,EURUSD,base_rate,1.25")

    assert message == {
        "timestamp_ms": 123,
        "instrument": "EURUSD",
        "input_type": "base_rate",
        "value": 1.25,
    }


def test_reader_skips_header_when_reading_file():
    records = list(
        CsvReader().read_file(
            StringIO(
                "timestamp_ms,instrument,input_type,value\n"
                "123,EURUSD,base_rate,1.25\n"
            )
        )
    )

    assert records == [
        {
            "timestamp_ms": 123,
            "instrument": "EURUSD",
            "input_type": "base_rate",
            "value": 1.25,
        }
    ]


def test_reader_skips_record_with_missing_value():
    records = list(
        CsvReader().read_file(
            StringIO(
                "timestamp_ms,instrument,input_type,value\n"
                "123,EURUSD,spread,\n"
                "124,EURUSD,spread,0.25\n"
            )
        )
    )

    assert records == [
        {
            "timestamp_ms": 124,
            "instrument": "EURUSD",
            "input_type": "spread",
            "value": 0.25,
        }
    ]


def test_reader_rejects_more_than_one_row():
    with pytest.raises(ValueError, match="one CSV row"):
        CsvReader().read_line("1,A,spread,0.1\n2,A,spread,0.2")


def test_consumer_creates_and_updates_instrument_entry():
    consumer = TickConsumer(EventCollector())

    consumer.OnMessage(
        {
            "timestamp_ms": 456,
            "instrument": "EURUSD",
            "input_type": "spread",
            "value": 0.02,
        }
    )

    entry = consumer.instruments["EURUSD"]
    assert entry["instrument"] == "EURUSD"
    assert entry["last_updated_ts"] == 456
    assert entry["spread"] == 0.02
    assert math.isnan(entry["base_rate"])
    assert math.isnan(entry["adjustment"])
    assert math.isnan(entry["derived_value"])
    assert not consumer.dirty_queue
    assert entry["valid"] is False
    assert entry["dirty"] is False


def test_consumer_current_time_does_not_move_backward():
    consumer = TickConsumer(EventCollector())

    consumer.OnMessage(
        {
            "timestamp_ms": 1000,
            "instrument": "EURUSD",
            "input_type": "spread",
            "value": 0.02,
        }
    )
    consumer.OnMessage(
        {
            "timestamp_ms": 500,
            "instrument": "EURUSD",
            "input_type": "spread",
            "value": 0.03,
        }
    )

    assert consumer.current_time == 1000


def test_consumer_derives_value_from_all_components():
    consumer = TickConsumer(EventCollector())

    for timestamp_ms, input_type, value in (
        (1, "base_rate", 1.5),
        (2, "spread", 0.25),
        (3, "adjustment", -0.1),
    ):
        consumer.OnMessage(
            {
                "timestamp_ms": timestamp_ms,
                "instrument": "EURUSD",
                "input_type": input_type,
                "value": value,
            }
        )

    entry = consumer.instruments["EURUSD"]
    assert entry["derived_value"] == 1.65
    assert list(consumer.dirty_queue) == [entry]
    assert entry["dirty"] is True


def test_consumer_does_not_queue_same_instrument_twice():
    consumer = TickConsumer(EventCollector())

    for timestamp_ms, input_type, value in (
        (1, "base_rate", 1.5),
        (2, "spread", 0.25),
        (3, "adjustment", -0.1),
    ):
        consumer.OnMessage(
            {
                "timestamp_ms": timestamp_ms,
                "instrument": "EURUSD",
                "input_type": input_type,
                "value": value,
            }
        )

    assert len(consumer.dirty_queue) == 1
    queued_entry = consumer.instruments["EURUSD"]
    consumer.OnConsumerReady()
    assert queued_entry["dirty"] is False

    consumer.OnMessage(
        {
            "timestamp_ms": 4,
            "instrument": "EURUSD",
            "input_type": "spread",
            "value": 0.3,
        }
    )

    assert list(consumer.dirty_queue) == [queued_entry]
    assert queued_entry["dirty"] is True


def test_on_consumer_ready_returns_and_removes_oldest_dirty_entry():
    collector = EventCollector()
    consumer = TickConsumer(collector)
    for instrument in ("EURUSD", "GBPUSD"):
        for input_type, value in (
            ("base_rate", 1.5),
            ("spread", 0.25),
            ("adjustment", -0.1),
        ):
            consumer.OnMessage(
                {
                    "timestamp_ms": 1,
                    "instrument": instrument,
                    "input_type": input_type,
                    "value": value,
                }
            )

    first_message = consumer.instruments["EURUSD"]
    second_message = consumer.instruments["GBPUSD"]

    consumer.OnConsumerReady()
    consumer.OnConsumerReady()

    assert collector.entries == [first_message, second_message]
    assert not consumer.dirty_queue
    assert first_message["dirty"] is False
    assert second_message["dirty"] is False


def test_on_consumer_ready_returns_none_when_queue_is_empty():
    consumer = TickConsumer(EventCollector())

    assert consumer.OnConsumerReady() is None