//! Processing statistics tracking

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Processing statistics
pub struct Stats {
    pub rows_read: AtomicUsize,
    pub rows_written: AtomicUsize,
    pub rows_failed: AtomicUsize,
    start_time: Instant,
}

impl Stats {
    /// Create new stats tracker
    pub fn new() -> Self {
        Self {
            rows_read: AtomicUsize::new(0),
            rows_written: AtomicUsize::new(0),
            rows_failed: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    /// Print progress to stderr
    pub fn print(&self, force: bool) {
        let read = self.rows_read.load(Ordering::Relaxed);
        let written = self.rows_written.load(Ordering::Relaxed);
        let failed = self.rows_failed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();

        if force || read % 500_000 == 0 {
            let rate = if elapsed.as_secs() > 0 {
                read as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            eprintln!(
                "Progress: {:>12} rows | {:>12} written | {:>8} failed | {:>10.0} rows/s",
                read, written, failed, rate
            );
        }
    }

    /// Print final summary
    pub fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total = self.rows_read.load(Ordering::Relaxed);
        let rate = if elapsed.as_secs() > 0 {
            total as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        eprintln!("\nCompleted in {:.2}s ({:.0} rows/s)", elapsed.as_secs_f64(), rate);
        eprintln!(
            "Summary: {} read | {} written | {} failed",
            self.rows_read.load(Ordering::Relaxed),
            self.rows_written.load(Ordering::Relaxed),
            self.rows_failed.load(Ordering::Relaxed)
        );
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}