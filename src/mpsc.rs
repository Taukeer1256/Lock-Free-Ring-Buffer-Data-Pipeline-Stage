use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Cache-line aligned to prevent false sharing between producer and consumer.
#[repr(align(64))]
struct CacheAligned<T>(T);

struct Node<T> {
    value: Option<T>,
    next: AtomicPtr<Node<T>>,
}

pub struct MpscQueue<T> {
    head: CacheAligned<AtomicPtr<Node<T>>>, // consumer owns
    tail: CacheAligned<AtomicPtr<Node<T>>>, // producers CAS here
}

// SAFETY: MpscQueue is Send + Sync because access is synchronized via atomic
// pointers. Producers use CAS loops to safely append nodes. The consumer
// exclusively reads and consumes from the head. T must be Send.
unsafe impl<T: Send> Send for MpscQueue<T> {}
unsafe impl<T: Send> Sync for MpscQueue<T> {}

impl<T> MpscQueue<T> {
    pub fn new() -> Self {
        // Allocate a sentinel/dummy node
        let dummy = Box::into_raw(Box::new(Node {
            value: None,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        // Both head and tail point to it initially
        Self {
            head: CacheAligned(AtomicPtr::new(dummy)),
            tail: CacheAligned(AtomicPtr::new(dummy)),
        }
    }

    /// Safe to call from multiple threads simultaneously
    pub fn push(&self, value: T) {
        let new_node = Box::into_raw(Box::new(Node {
            value: Some(value),
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        // Loop: CAS tail from current to new_node
        let mut prev_tail = self.tail.0.load(Ordering::Acquire);
        loop {
            match self.tail.0.compare_exchange_weak(
                prev_tail,
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // On success: set old_tail.next to new_node with Release
                    // SAFETY: We just won the CAS, so we are the exclusive owners
                    // of updating `prev_tail`'s next pointer.
                    unsafe {
                        (*prev_tail).next.store(new_node, Ordering::Release);
                    }
                    break;
                }
                Err(current_tail) => {
                    prev_tail = current_tail;
                }
            }
        }
    }

    /// Called only from single consumer thread
    pub fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Acquire);
        
        // SAFETY: head is always a valid pointer to a Node (starting with sentinel).
        let next = unsafe { (*head).next.load(Ordering::Acquire) };

        if next.is_null() {
            return None; // empty
        }

        // We know next is not null, so there's an item to pop.
        // Update head to point to the next node.
        self.head.0.store(next, Ordering::Release);

        // Extract value from the new head (which was 'next')
        // SAFETY: We are the only consumer, and we've advanced the head pointer.
        // We own the data inside the `next` node now.
        let value = unsafe { (*next).value.take() };

        // Drop the old head node
        // SAFETY: The old head is now disconnected from the queue and we have
        // exclusive access to it.
        unsafe {
            let _ = Box::from_raw(head);
        }

        value
    }

    pub fn is_empty(&self) -> bool {
        let head = self.head.0.load(Ordering::Acquire);
        // SAFETY: head is always valid
        let next = unsafe { (*head).next.load(Ordering::Acquire) };
        next.is_null()
    }
}

impl<T> Drop for MpscQueue<T> {
    fn drop(&mut self) {
        // Walk the linked list and free all nodes
        let mut current = self.head.0.load(Ordering::Relaxed);
        while !current.is_null() {
            unsafe {
                let next = (*current).next.load(Ordering::Relaxed);
                let _ = Box::from_raw(current);
                current = next;
            }
        }
    }
}

impl<T> Default for MpscQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}
