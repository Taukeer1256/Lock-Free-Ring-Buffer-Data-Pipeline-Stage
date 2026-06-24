# Lock-Free Ring Buffer & Data Pipeline

## What This Is
A from-scratch implementation of lock-free concurrent data structures in Rust,
demonstrating systems programming skills relevant to IoT data pipelines.

## Architecture

Single Producer Single Consumer (SPSC):
┌──────────┐  push()   ┌─────────────────────┐  pop()   ┌──────────┐
│ Producer │ ────────► │  SpscRingBuffer<T>   │ ───────► │ Consumer │
│ thread   │           │  [AtomicUsize head]  │          │ thread   │
└──────────┘           │  [AtomicUsize tail]  │          └──────────┘
                       │  [UnsafeCell slots]  │
                       └─────────────────────┘

3-Stage Pipeline:
Producer → [SPSC 1024] → Filter Stage → [SPSC 1024] → Transform Stage → [SPSC 1024] → Consumer
(generate readings)     (value > 20.0)  (celsius→fahrenheit)            (record metrics)

## Why Lock-Free?
- Mutex = kernel syscall on contention = microseconds of latency
- AtomicUsize CAS = single CPU instruction = nanoseconds
- For 500M msg/sec pipelines, this difference is everything

## Memory Layout
Each slot is cache-line aligned (64 bytes) to prevent false sharing:
- head and tail on separate cache lines
- Producer only writes tail, Consumer only writes head
- Zero sharing = zero cache coherence traffic

## Run Demo
cargo run --release

## Run Tests
cargo test

## Run Benchmarks
cargo bench
# HTML report at: target/criterion/report/index.html

## Benchmark Results (placeholder)
| Benchmark                    | Throughput        |
|------------------------------|-------------------|
| SPSC ring buffer             | ~XXX M msgs/sec   |
| std::sync::mpsc              | ~XXX M msgs/sec   |
| crossbeam bounded channel    | ~XXX M msgs/sec   |
| 3-stage pipeline end-to-end  | ~XXX M msgs/sec   |

## Profiling

### Flamegraph
cargo install flamegraph
cargo flamegraph --bin ring-buffer-pipeline --release

### Cache Miss Analysis
perf stat -e cache-misses,cache-references cargo run --release

### Memory Usage
/usr/bin/time -v cargo run --release 2>&1 | grep "Maximum resident"

## Key Unsafe Code Explained
All unsafe blocks have SAFETY comments. The core invariants are:
1. SpscRingBuffer: only one thread ever writes tail, only one reads head
2. Slot access: tail - head < capacity guarantees the slot is exclusively owned
3. MaybeUninit: we only read slots we have previously written

## What I Learned
- False sharing can tank performance by 10x — cache alignment matters
- Relaxed vs Acquire/Release ordering: wrong choice = silent data races
- Lock-free != wait-free: our MPSC has a linearization point in the CAS loop
- Criterion's statistical analysis catches variance that wall-clock timing misses

## Known Limitations
- SPSC only safe with exactly 1 producer and 1 consumer thread
- No backpressure propagation upstream (producer spins on full buffer)
- MPSC push is not wait-free under high contention

## Future Improvements
- Add wait-free variant using fetch_add instead of CAS loop
- Add backpressure: block producer instead of spinning
- Implement bounded MPMC (multi-producer multi-consumer)
- Add io_uring integration for kernel bypass I/O
