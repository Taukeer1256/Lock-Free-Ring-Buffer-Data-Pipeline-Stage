use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub struct PipelineMetrics {
    messages_produced: AtomicU64,
    messages_consumed: AtomicU64,
    messages_dropped: AtomicU64,
    total_latency_ns: AtomicU64, // sum of all latencies
    start_time: Instant,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            messages_produced: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    #[inline]
    pub fn record_produced(&self) {
        self.messages_produced.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_consumed(&self, latency_ns: u64) {
        self.messages_consumed.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_dropped(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn throughput_msgs_per_sec(&self) -> f64 {
        let consumed = self.messages_consumed.load(Ordering::Relaxed) as f64;
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            consumed / elapsed
        } else {
            0.0
        }
    }

    pub fn avg_latency_ns(&self) -> f64 {
        let consumed = self.messages_consumed.load(Ordering::Relaxed) as f64;
        let total_latency = self.total_latency_ns.load(Ordering::Relaxed) as f64;
        if consumed > 0.0 {
            total_latency / consumed
        } else {
            0.0
        }
    }

    pub fn drop_rate(&self) -> f64 {
        let produced = self.messages_produced.load(Ordering::Relaxed) as f64;
        let dropped = self.messages_dropped.load(Ordering::Relaxed) as f64;
        if produced > 0.0 {
            dropped / produced
        } else {
            0.0
        }
    }

    pub fn print_report(&self) {
        let consumed = self.messages_consumed.load(Ordering::Relaxed);
        let produced = self.messages_produced.load(Ordering::Relaxed);
        let dropped = self.messages_dropped.load(Ordering::Relaxed);
        
        let tp = self.throughput_msgs_per_sec();
        let lat = self.avg_latency_ns();
        let dr = self.drop_rate() * 100.0;

        println!("┌───────────────────────────────────────────┐");
        println!("│          Pipeline Metrics Report          │");
        println!("├──────────────────┬────────────────────────┤");
        println!("│ Throughput       │ {:>14.2} msg/s │", tp);
        println!("│ Avg Latency      │ {:>14.2} ns    │", lat);
        println!("│ Messages Dropped │ {:>7} ({:>5.2}%) │", dropped, dr);
        println!("│ Total Produced   │ {:>14}         │", produced);
        println!("│ Total Consumed   │ {:>14}         │", consumed);
        println!("└──────────────────┴────────────────────────┘");
    }
}

// Ensure tests can read the raw values easily
impl PipelineMetrics {
    pub fn get_consumed(&self) -> u64 {
        self.messages_consumed.load(Ordering::Relaxed)
    }
}
