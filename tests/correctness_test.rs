use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use ring_buffer_pipeline::mpsc::MpscQueue;
use ring_buffer_pipeline::pipeline::Pipeline;
use ring_buffer_pipeline::spsc::{split, SpscRingBuffer};
use ring_buffer_pipeline::RingBufferError;

#[test]
fn test_spsc_basic() {
    let buffer = SpscRingBuffer::<u64>::new(128).unwrap();
    
    for i in 1..=100 {
        buffer.push(i).unwrap();
    }

    for i in 1..=100 {
        assert_eq!(buffer.pop().unwrap(), i);
    }
    
    assert!(buffer.is_empty());
}

#[test]
fn test_spsc_capacity() {
    let buffer = SpscRingBuffer::<u64>::new(4).unwrap();
    
    assert_eq!(buffer.push(1), Ok(()));
    assert_eq!(buffer.push(2), Ok(()));
    assert_eq!(buffer.push(3), Ok(()));
    assert_eq!(buffer.push(4), Ok(()));
    
    // Fill to capacity, assert next push returns BufferFull
    assert_eq!(buffer.push(5), Err(RingBufferError::BufferFull));
    
    // Pop one
    assert_eq!(buffer.pop(), Ok(1));
    
    // Assert push succeeds again
    assert_eq!(buffer.push(6), Ok(()));
}

#[test]
fn test_spsc_power_of_two_validation() {
    let err = match SpscRingBuffer::<u64>::new(3) {
        Err(e) => e,
        Ok(_) => panic!("Expected error"),
    };
    assert_eq!(err.to_string(), RingBufferError::InvalidCapacity(3).to_string());
    assert!(SpscRingBuffer::<u64>::new(4).is_ok());
}

#[test]
fn test_spsc_concurrent() {
    let buffer = Arc::new(SpscRingBuffer::<u64>::new(1024).unwrap());
    let (tx, rx) = split(buffer);
    let total = 1_000_000;

    let t_prod = thread::spawn(move || {
        for i in 0..total {
            loop {
                if tx.push(i).is_ok() {
                    break;
                }
            }
        }
    });

    let t_cons = thread::spawn(move || {
        for expected in 0..total {
            loop {
                if let Ok(val) = rx.pop() {
                    assert_eq!(val, expected);
                    break;
                }
            }
        }
    });

    t_prod.join().unwrap();
    t_cons.join().unwrap();
}

#[test]
fn test_mpsc_concurrent() {
    let queue = Arc::new(MpscQueue::<u64>::new());
    let mut handles = vec![];
    
    // 8 threads, 10_000 items each -> 80_000 total
    let total_per_thread = 10_000;
    
    for thread_id in 0..8 {
        let q = Arc::clone(&queue);
        handles.push(thread::spawn(move || {
            for i in 0..total_per_thread {
                // Tag with thread id in upper bits to avoid overlap
                let val = (thread_id << 32) | i;
                q.push(val);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut seen = HashSet::new();
    let mut count = 0;
    
    while let Some(val) = queue.pop() {
        assert!(seen.insert(val), "Duplicate value found: {}", val);
        count += 1;
    }
    
    assert_eq!(count, 80_000);
}

#[test]
fn test_pipeline_end_to_end() {
    let pipeline = Pipeline::new();
    pipeline.run(100_000);
    
    let metrics = pipeline.metrics();
    
    let consumed = metrics.get_consumed();
    assert!(consumed > 0, "No messages consumed");
    
    let drop_rate = metrics.drop_rate();
    assert!(drop_rate < 0.01, "Drop rate too high: {}", drop_rate);
}

#[test]
fn test_spsc_drop() {
    let strong_count_tracker = Arc::new(());
    
    let buffer = SpscRingBuffer::<Arc<()>>::new(8).unwrap();
    
    for _ in 0..5 {
        buffer.push(Arc::clone(&strong_count_tracker)).unwrap();
    }
    
    // 1 original + 5 in the buffer
    assert_eq!(Arc::strong_count(&strong_count_tracker), 6);
    
    // Drop the ring buffer without popping
    drop(buffer);
    
    // Verify no memory leak
    assert_eq!(Arc::strong_count(&strong_count_tracker), 1);
}
