use std::sync::Arc;
use std::thread;

use ring_buffer_pipeline::mpsc::MpscQueue;
use ring_buffer_pipeline::pipeline::Pipeline;
use ring_buffer_pipeline::spsc::SpscRingBuffer;
use ring_buffer_pipeline::RingBufferError;

fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt().with_env_filter("info").init();

    println!("=== Lock-Free Ring Buffer Pipeline Demo ===\n");

    // Demo 1: Basic SPSC correctness check
    println!("--- SPSC Basic Test ---");
    let spsc = SpscRingBuffer::<u64>::new(8).unwrap();
    
    for i in 1..=8 {
        spsc.push(i).unwrap();
    }
    
    // Verify BufferFull error
    match spsc.push(9) {
        Err(RingBufferError::BufferFull) => println!("Correctly received BufferFull error"),
        _ => panic!("Expected BufferFull error!"),
    }

    print!("Popped values: ");
    for _ in 1..=8 {
        let val = spsc.pop().unwrap();
        print!("{} ", val);
    }
    println!();

    // Verify BufferEmpty error
    match spsc.pop() {
        Err(RingBufferError::BufferEmpty) => println!("Correctly received BufferEmpty error"),
        _ => panic!("Expected BufferEmpty error!"),
    }

    // Demo 2: Run full pipeline with 10_000_000 messages
    println!("\n--- Running 3-Stage Pipeline (10M messages) ---");
    let pipeline = Pipeline::new();
    pipeline.run(10_000_000);

    // Demo 3: MPSC correctness
    println!("\n--- MPSC Multi-Producer Test ---");
    let mpsc = Arc::new(MpscQueue::<u64>::new());
    
    let mut handles = vec![];
    for p in 0..4 {
        let q = Arc::clone(&mpsc);
        handles.push(thread::spawn(move || {
            let base = p * 1000;
            for i in 0..1000 {
                q.push(base + i);
            }
        }));
    }

    // Wait for producers
    for h in handles {
        h.join().unwrap();
    }

    // Consumer pops all 4000
    let mut count = 0;
    while let Some(_) = mpsc.pop() {
        count += 1;
    }
    
    println!("MPSC Queue correctly processed {} messages from 4 producers.", count);
    assert_eq!(count, 4000);
}
