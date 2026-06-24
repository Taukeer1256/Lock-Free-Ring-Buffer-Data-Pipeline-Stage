use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_channel::bounded;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;

use ring_buffer_pipeline::pipeline::Pipeline;
use ring_buffer_pipeline::spsc::{split, SpscRingBuffer};
use ring_buffer_pipeline::RingBufferError;

const BENCH_MESSAGES: u64 = 1_000_000;
const CAP: usize = 4096;

fn bench_spsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.throughput(Throughput::Elements(BENCH_MESSAGES));

    group.bench_function("spsc_custom", |b| {
        b.iter(|| {
            let buffer = Arc::new(SpscRingBuffer::<u64>::new(CAP).unwrap());
            let (tx, rx) = split(buffer);

            let t = thread::spawn(move || {
                for i in 0..BENCH_MESSAGES {
                    loop {
                        match tx.push(black_box(i)) {
                            Ok(()) => break,
                            Err(RingBufferError::BufferFull) => std::hint::spin_loop(),
                            Err(_) => unreachable!(),
                        }
                    }
                }
            });

            for _ in 0..BENCH_MESSAGES {
                loop {
                    match rx.pop() {
                        Ok(v) => {
                            black_box(v);
                            break;
                        }
                        Err(RingBufferError::BufferEmpty) => std::hint::spin_loop(),
                        Err(_) => unreachable!(),
                    }
                }
            }

            t.join().unwrap();
        });
    });
    group.finish();
}

fn bench_spsc_vs_std_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_vs_std_mpsc");
    group.throughput(Throughput::Elements(BENCH_MESSAGES));

    group.bench_function("custom_spsc", |b| {
        b.iter(|| {
            let buffer = Arc::new(SpscRingBuffer::<u64>::new(CAP).unwrap());
            let (tx, rx) = split(buffer);

            let t = thread::spawn(move || {
                for i in 0..BENCH_MESSAGES {
                    loop {
                        match tx.push(black_box(i)) {
                            Ok(()) => break,
                            Err(RingBufferError::BufferFull) => std::hint::spin_loop(),
                            Err(_) => unreachable!(),
                        }
                    }
                }
            });

            for _ in 0..BENCH_MESSAGES {
                loop {
                    if let Ok(v) = rx.pop() {
                        black_box(v);
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
            t.join().unwrap();
        });
    });

    group.bench_function("std_mpsc", |b| {
        b.iter(|| {
            let (tx, rx) = channel::<u64>();
            let t = thread::spawn(move || {
                for i in 0..BENCH_MESSAGES {
                    tx.send(black_box(i)).unwrap();
                }
            });

            for _ in 0..BENCH_MESSAGES {
                black_box(rx.recv().unwrap());
            }
            t.join().unwrap();
        });
    });
    group.finish();
}

fn bench_spsc_vs_crossbeam(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_vs_crossbeam");
    group.throughput(Throughput::Elements(BENCH_MESSAGES));

    group.bench_function("custom_spsc", |b| {
        b.iter(|| {
            let buffer = Arc::new(SpscRingBuffer::<u64>::new(CAP).unwrap());
            let (tx, rx) = split(buffer);

            let t = thread::spawn(move || {
                for i in 0..BENCH_MESSAGES {
                    loop {
                        if tx.push(black_box(i)).is_ok() {
                            break;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
            });

            for _ in 0..BENCH_MESSAGES {
                loop {
                    if let Ok(v) = rx.pop() {
                        black_box(v);
                        break;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            }
            t.join().unwrap();
        });
    });

    group.bench_function("crossbeam_bounded", |b| {
        b.iter(|| {
            let (tx, rx) = bounded::<u64>(CAP);
            let t = thread::spawn(move || {
                for i in 0..BENCH_MESSAGES {
                    tx.send(black_box(i)).unwrap();
                }
            });

            for _ in 0..BENCH_MESSAGES {
                black_box(rx.recv().unwrap());
            }
            t.join().unwrap();
        });
    });
    group.finish();
}

fn bench_topic_routing_at_capacity(c: &mut Criterion) {
    let mut group = c.benchmark_group("at_capacity_latency");

    group.bench_function("push_pop_90_percent_full", |b| {
        let buffer = Arc::new(SpscRingBuffer::<u64>::new(CAP).unwrap());
        let (tx, rx) = split(buffer);

        // Fill to 90%
        let target = (CAP as f64 * 0.9) as usize;
        for i in 0..target {
            tx.push(i as u64).unwrap();
        }

        b.iter(|| {
            // Push one, pop one (simulates routing at near-full capacity)
            tx.push(black_box(1)).unwrap();
            black_box(rx.pop().unwrap());
        });
    });
    group.finish();
}

fn bench_pipeline_3stage(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_3stage");
    group.throughput(Throughput::Elements(BENCH_MESSAGES));

    group.bench_function("end_to_end", |b| {
        b.iter(|| {
            let pipeline = Pipeline::new();
            pipeline.run(BENCH_MESSAGES);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_spsc_throughput,
    bench_spsc_vs_std_mpsc,
    bench_spsc_vs_crossbeam,
    bench_topic_routing_at_capacity,
    bench_pipeline_3stage
);
criterion_main!(benches);
