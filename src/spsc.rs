use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::RingBufferError;

/// Cache-line aligned to prevent false sharing between producer and consumer.
/// A typical x86/ARM cache line is 64 bytes.
#[repr(align(64))]
struct CacheAligned<T>(T);

pub struct SpscRingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    mask: usize,                     // capacity - 1 for fast modulo
    head: CacheAligned<AtomicUsize>, // consumer reads here, updates head
    tail: CacheAligned<AtomicUsize>, // producer reads here, updates tail
}

// SAFETY: SpscRingBuffer is Send + Sync because we enforce single-producer and
// single-consumer access patterns. The `UnsafeCell` allows interior mutability,
// but the atomic `head` and `tail` cursors with Acquire/Release memory ordering
// guarantee that the producer and consumer never access the same slot concurrently
// when it is in an invalid state. `T` must be `Send` to cross thread boundaries.
unsafe impl<T: Send> Send for SpscRingBuffer<T> {}
unsafe impl<T: Send> Sync for SpscRingBuffer<T> {}

impl<T> SpscRingBuffer<T> {
    /// Capacity MUST be a power of 2
    pub fn new(capacity: usize) -> Result<Self, RingBufferError> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(RingBufferError::InvalidCapacity(capacity));
        }

        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Ok(Self {
            buffer: vec.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            head: CacheAligned(AtomicUsize::new(0)),
            tail: CacheAligned(AtomicUsize::new(0)),
        })
    }

    /// Called only by the producer thread
    #[inline]
    pub fn push(&self, value: T) -> Result<(), RingBufferError> {
        // Load the current tail cursor (only the producer changes this, so Relaxed is fine)
        let tail = self.tail.0.load(Ordering::Relaxed);
        // Load the head cursor with Acquire ordering to synchronize with the consumer's Release store
        let head = self.head.0.load(Ordering::Acquire);

        // Check if full. The buffer is full if tail has wrapped around and caught up to head.
        if tail.wrapping_sub(head) >= self.capacity {
            return Err(RingBufferError::BufferFull);
        }

        let idx = tail & self.mask;

        // SAFETY: 
        // 1. We are the only producer thread.
        // 2. The (tail - head) < capacity check guarantees this slot is available.
        // 3. The consumer will not read this slot until we increment `tail`.
        unsafe {
            let slot = self.buffer.get_unchecked(idx).get();
            ptr::write(slot, MaybeUninit::new(value));
        }

        // Store the new tail with Release ordering to synchronize with the consumer's Acquire load.
        // This ensures the data written above is visible before the consumer sees the updated tail.
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    /// Called only by the consumer thread
    #[inline]
    pub fn pop(&self) -> Result<T, RingBufferError> {
        // Load the current head cursor (only the consumer changes this, so Relaxed is fine)
        let head = self.head.0.load(Ordering::Relaxed);
        // Load the tail cursor with Acquire ordering to synchronize with the producer's Release store
        let tail = self.tail.0.load(Ordering::Acquire);

        // Check if empty
        if head == tail {
            return Err(RingBufferError::BufferEmpty);
        }

        let idx = head & self.mask;

        // SAFETY:
        // 1. We are the only consumer thread.
        // 2. The `head != tail` check guarantees this slot has been written by the producer.
        // 3. The producer will not overwrite this slot until we increment `head`.
        let value = unsafe {
            let slot = self.buffer.get_unchecked(idx).get();
            ptr::read(slot).assume_init()
        };

        // Store the new head with Release ordering to synchronize with the producer's Acquire load.
        // This makes the slot available for the producer again.
        self.head.0.store(head.wrapping_add(1), Ordering::Release);

        Ok(value)
    }

    pub fn len(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Acquire);
        let head = self.head.0.load(Ordering::Acquire);
        tail.wrapping_sub(head)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for SpscRingBuffer<T> {
    fn drop(&mut self) {
        // Pop and drop all remaining items to avoid memory leaks
        while self.pop().is_ok() {}
    }
}

pub fn split<T>(buffer: Arc<SpscRingBuffer<T>>) -> (SpscProducer<T>, SpscConsumer<T>) {
    (
        SpscProducer { inner: Arc::clone(&buffer) },
        SpscConsumer { inner: buffer },
    )
}

pub struct SpscProducer<T> {
    inner: Arc<SpscRingBuffer<T>>,
}

impl<T> SpscProducer<T> {
    #[inline]
    pub fn push(&self, value: T) -> Result<(), RingBufferError> {
        self.inner.push(value)
    }
}

pub struct SpscConsumer<T> {
    inner: Arc<SpscRingBuffer<T>>,
}

impl<T> SpscConsumer<T> {
    #[inline]
    pub fn pop(&self) -> Result<T, RingBufferError> {
        self.inner.pop()
    }
}
