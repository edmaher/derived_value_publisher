# DerivedValuePublisher

This project reads headered market tick data from CSV, maintains the latest state for each instrument, calculates a derived value, and publishes completed instrument entries to an output consumer.

## Setup and Usage

Create and activate a Conda environment with Python 3.12:

```bash
conda create -n dvpub python=3.12
conda activate dvpub
```

Install the test dependency and run the unit tests from the project root:

```bash
python -m pip install -r tests/requirements.txt
python -m pytest
```

Run the command-line publisher with a headered CSV input:

```bash
python main.py market_inputs.csv
```

The output CSV is written beside the input with a timestamp added to its filename. The optional `--ready-threshold-ms` argument sets the simulated time between readiness callbacks; it defaults to `100`:

```bash
python main.py market_inputs.csv --ready-threshold-ms 100
```

## Assumptions

- Input timestamps are Unix epoch timestamps in milliseconds.
- The input CSV has the headers `timestamp_ms`, `instrument`, `input_type`, and `value` in that order.
- Valid input types are `base_rate`, `spread`, and `adjustment`.
- A complete instrument state has non-`NaN` values for all three numeric components.
- Input rows are processed in file order, but the consumer's current time never moves backward.
- Rows with missing or invalid values are reported to stderr and skipped.
- The output filename is based on the wall-clock time when processing starts, not on an input record timestamp.

## Tradeoffs

- The reader streams rows instead of loading the complete file, reducing memory use for large inputs.
- Invalid rows are skipped to allow a long file to continue processing, which means malformed data does not fail the complete run.
- A per-instrument `dirty` flag prevents duplicate queue entries, but an entry can represent multiple updates by the time it is published.
- `OnConsumerReady` publishes one queued entry at a time, keeping the consumer independent of the output format while making publication timing explicit.
- The current state uses dictionaries for flexibility, at the cost of less strict typing than a dataclass or typed model would provide.

## Different Paths

- Use a dataclass for instrument entries instead of dictionaries for stronger type checking and clearer field ownership.
- Replace the in-memory `deque` with a durable queue if records must survive process failure.
- Add a JSON, database, or message-broker event consumer without changing `TickConsumer`.
- Reject malformed rows and stop processing instead of reporting and skipping them.
- Sort or window out-of-order input if event-time ordering is required rather than preserving file order.

## More Time

- Expand regression coverage for malformed rows, queue behavior, latency, and output files.
- Add configuration for input and output paths, timestamp units, threshold values, and invalid-row policy.
- Add structured logging instead of direct stderr messages and stdout record output.
- Measure throughput and memory usage against `large_test.csv`.
- Add property-based tests for monotonic current time and duplicate suppression.
- Add graceful handling for output collisions when two runs start within the same second.

## Time Log AI

| Area | Time | Notes |
| --- | ---: | --- |
| CSV parsing and header handling | ~25 min | Streaming reader, typed records, invalid-row reporting |
| Instrument state and derived values | ~25 min | Per-instrument dictionary state and dirty queue |
| Event publication refactor | ~15 min | Injected event consumer and single-event publication |
| Output CSV | ~20 min | Timestamped output path and CSV event writer |
| Test and runtime validation | ~15 min | Compilation, diagnostics, focused runtime checks |

**Estimated total active time:** ~100 minutes.

This estimate was sourced by AI from the retained prompt logs across the `tick_consolidator` and `DerivedValuePublisher` workspaces, excluding gaps longer than 20 minutes.

## Time Log Manual

| Area | Time | Notes |
| --- | ---: | --- |
| Manual Reviews | ~120 min | over several days, review of AI generated logic and review overall implementation |
| Manual design thinking | ~60 min | Authoring Brain-dump notes prior to starting in Google docs -> moved to BRAINDUMP.md |
| Manual review and refactor | ~30 min | Post-implementation review of generated code |
| Misc | ~20 min | Env validation, install and run instructions, git publish  |


## Design Choices AI

- `CsvReader.read_file` uses `csv.reader` and yields one typed record at a time.
- `TickConsumer` owns instrument state, current time, the dirty queue, and publication timing.
- `OnConsumerReady` is a control point for publishing events rather than a method that returns an event to its caller.
- The event consumer is injected into `TickConsumer`, allowing the CSV writer to be replaced by another implementation.
- `CsvEntryWriter` owns CSV header and row serialization and consumes individual published entries.

## Human choices

### Observations - input data
The structure offered in the csv has a critical weakness that can be quite dangerous in trading.
There is no ability to detect packet loss in a UDP stream, and the consequences can be quite unpleasant.
Such streams really do need a sequence number to detect/mitigate this issue.
If a single packet is dropped, the resulting Derived value will be wrong until a message is received to overwrite the missed value
If a packet is receved out of order, the resulting Derived value could be wrong if it's an update of the same value

the protocol likely also needs a heartbeat to fill in 'idle' gaps to ensure the liveness of the feed can be determined
the consumer likely also needs a mechanism to read the whole snapshot on startup to get the initial values


### Use of Python
Easy to reason about and test, and in theory is capable of the task, but also excellent for prototyping and iterating.

### Single threaded async style processing
Single threaded async style is preferred in some scenarios - we can discuss specific pros/cons, but also in Python
Mutli-threaded procesisng may introduce locks, some complexity, but can be done lock free, and in some cases can also reduce complexity

### Data structures
This is the most important part here. Because a single event in the feed does not result in an outgoing event, the state has to be stored.
The updating of the state makes it dirty and read to be published when the consumer is ready.
A multi-consumer model would need a way to determine if an update had occured since the last read - a dirty flag would not work
The time stamp of last the received event is included because often in these applications, it is possible to have unintentionally stale data published.
Publishing the timestamp of the last event helps both the end consumer, and a dummy monitoring consumer verify the health of the publisher


### Internal structure and API
The structure is intended to expose a simple API to the source of the incoming data, such as UDP, as well as maintian state and allow the consumer to
read events.
Care is needed in this structure to avoid starvation and uncontrolled flooding of a consumer
In this example, we use the simulated time as a mechanism to trigger outgoing data, in a true publisher, we need to either publish when the transmission channel is writeable or more likely at a maximum rate - noted that network buffering can give the appearance of a writeable channel, and result in a buffered data blocking other data, and therefore a stale response.
I am a fan of the (Inversion of Control) IoC structure to wire up dependencies in a flexible way - allowing components to be replaced easily for testing and alternate use-cases.


### C++ HFT solution
The HFT solution would look a little different.
In such a case, the UDP data can be coming from a low latency user-space NIC, in which case the time taken to process the packet can be critically important to avoid packet loss.
Async processing can still be a good option, but the processing time can be important
Typically, the state buffer would be maintained in a preallocated shared lock-free structure - I would use something that looks like a ring buffer with three entries for triple buffering semantics, populating entries alernately, and reading it in a loop




## AI Use

AI assistance was used to help inspect the code, implement focused refactors, generate test cases, investigate timestamp ordering, create a larger test CSV, and run validation commands. The implementation decisions, assumptions, and validation results should be reviewed against the project requirements and data contract.

## AI Observations

1. it can be a lot of work to express in text the intent, I have a bias to try to cover my intent accurately
2. it does do more than I ask for - test cases especially

