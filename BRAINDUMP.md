
Thoughts
# Thoughts

- CSV reading is a throwaway concern; it is assumed to be a test harness.
- There is overlap between decoding a CSV record and decoding a network message.

## Speed and Buffering

The speed and “buffering” considerations are similar to other cases, such as a 1-minute publisher or a consolidator commonly used to rate-limit output.

- TCP streams have buffers that can fill with backlogs of messages that cannot later be managed.
- The system is assumed not to know the universe of instruments, although some systems do.

## Consolidator Models

These models are common in trading and market data systems. They perform a range of functions and may accumulate more details or behave differently:

- Count the number of input ticks between output ticks.
- Flag specific fields as dirty or clean for the consumer.
- Filter out instruments.
- Aggregate values, such as summing trade volumes.
- Aggregate across instruments, combining liquidity and scaling prices and size (a “super book”).
- Accumulate inter-tick metrics such as OHLC.
- Take a snapshot at a point in time or on an event.

## Lock and Lock-Free Models

The overall concept of a lock-free implementation has been common in HFT environments. The general principle is that internal structures should not require locks.

The general concern is that a fast producer may not be serviced correctly if a consumer or intermediate process holds a lock. There are several solutions:

- Atomically updated variables.
- A single-threaded design with an async reactor.
- Ring buffer models.
- Triple buffering.

Historically, trading firms liked single-threaded designs because they appear not to require locks, simply because the process is only doing one thing at once. The idea works and appears simple to developers unfamiliar with threading complexity, but does not scale. It also introduces complexity around state management, particularly with multi-step protocol negotiations.

Ring buffer models can work well to offload work between a consumer and an intermediate processor. However, a ring buffer does not itself solve a mismatch between producer and consumer; it can only do so in an uncontrolled, lossy manner.

Having a double buffer on the consumer side can theoretically mitigate the mismatch if arbitrary tick loss is permissible. However, this can result in stale data being published, which is usually not the intent.

A buffer slot for the “latest” tick is usually the better choice.

In the multiple-consumer case, it may be necessary to buffer the “last tick” published so the delta or change of state between ticks can be published, as each consumer may consume at a different rate. However, the consumer could do this itself.

A pub-sub model layered over the top, where each consumer may subscribe to different sets of ticks, adds some complexity but does not necessarily change the core approach.

## Threading and Process Models

Threading models include worker threads, communication threads, and others; there are important tradeoffs:

- The single-threaded async reactor model assumes the total aggregate processing time is well below 100%. Although the process is not blocked by locks or communication, it can be blocked by doing other work. The model embeds latency by taking longer to process the previous work or message.
- Threading with locks is prone to locking concerns (other IPC/ITC models may be used), but is often dependent on kernel scheduling and introduces other latency, context-switch, and scaling concerns.
- Threading without locks generally requires spinning threads, demanding adequate CPU cores and also requiring inter-core cache-coherency traffic.

### Conclusion

It is necessary to evaluate the performance requirements. A producer that may be “very fast” may need to be serviced by a dedicated thread to avoid data loss, with a ring buffer to absorb traffic peaks.

## Data Structures

It is assumed that the data structures are static. Some extensions and options may support a more dynamic model; however, in this case, the CSV record is converted to a fixed structure.

## Proposed Solution

Use a simple loop to simulate an async reactor model:

1. Read a row from CSV.
2. Parse it to a struct.
3. Track simulated time.
4. Call `OnMsg`.
5. After a defined time, call `OnConsumerTick()`.
6. Generate an output CSV tick.

Separate concerns between receiving, consolidation, and publishing. These may then be reusable in a revised model as requirements change.

Use a class structure with simple callback functions:

- `OnMessage()` is called by the CSV reader.
- `OnConsumerTick()` is called by a pseudo-timer.

## Data Structures Needed

- Incoming tick.
- Outgoing tick.
- Read the CSV line by line to avoid failure due to a giant file and to stay closer to a real solution.
- Avoid complexity: it is the same task, but there is no need to allocate and store the entire file.




