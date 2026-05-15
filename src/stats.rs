//! Processing statistics tracking with progress bar

use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;
use sysinfo::System;

/// Processing statistics with progress bar
pub struct Stats {
    pub rows_read: AtomicUsize,
    pub rows_written: AtomicUsize,
    pub rows_failed: AtomicUsize,
    start_time: Instant,
    pb: ProgressBar,
    sys: Mutex<System>,
}

impl Stats {
    /// Create new stats tracker
    pub fn new(total_rows: Option<u64>) -> Self {
        let pb = if let Some(total) = total_rows {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.cyan} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:>3}% | {pos:>12}/{len:>12} | ETA: {eta:>8} | {msg}")
                    .unwrap()
                    .progress_chars("█▉▊▋▌▍▎▏  "),
            );
            pb
        } else {
            let pb = ProgressBar::hidden();
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.cyan} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes:>10}/{bytes_total:>10} | ETA: {eta:>8} | {msg}")
                    .unwrap()
                    .progress_chars("█▉▊▋▌▍▎▏  "),
            );
            pb
        };

        let sys = Mutex::new(System::new_all());

        Self {
            rows_read: AtomicUsize::new(0),
            rows_written: AtomicUsize::new(0),
            rows_failed: AtomicUsize::new(0),
            start_time: Instant::now(),
            pb,
            sys,
        }
    }

    /// Update the progress bar with current stats
    pub fn update(&self) {
        let read = self.rows_read.load(Ordering::Relaxed) as u64;
        let failed = self.rows_failed.load(Ordering::Relaxed);

        {
            let mut sys = self.sys.lock().unwrap();
            sys.refresh_memory();
        }
        let mem_mb = self.sys.lock().unwrap().used_memory() as f64 / 1_048_576.0;

        let msg = if failed > 0 {
            format!("{:.1} MB RAM | {} failed", mem_mb, failed)
        } else {
            format!("{:.1} MB RAM", mem_mb)
        };

        if self.pb.length().unwrap_or(0) > 0 {
            self.pb.set_position(read);
            self.pb.set_message(msg);
        } else {
            self.pb.set_message(msg);
        }
    }

    /// Increment rows read and update progress
    pub fn inc_read(&self, count: usize) {
        self.rows_read.fetch_add(count, Ordering::Relaxed);
        self.update();
    }

    /// Increment rows written
    pub fn inc_written(&self, count: usize) {
        self.rows_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment rows failed
    pub fn inc_failed(&self, count: usize) {
        self.rows_failed.fetch_add(count, Ordering::Relaxed);
        self.update();
    }

    /// Finish the progress bar
    pub fn finish(&self) {
        self.pb.finish_with_message("done");
    }

    /// Print final summary to stderr
    pub fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total = self.rows_read.load(Ordering::Relaxed);
        let elapsed_secs = elapsed.as_secs_f64();
        let rate = if elapsed_secs > 0.0 {
            total as f64 / elapsed_secs
        } else {
            0.0
        };

        {
            let mut sys = self.sys.lock().unwrap();
            sys.refresh_memory();
        }
        let mem_mb = self.sys.lock().unwrap().used_memory() as f64 / 1_048_576.0;

        eprintln!(
            "\nCompleted in {:.2}s ({:.0} rows/s) | Peak: {:.1} MB RAM",
            elapsed.as_secs_f64(),
            rate,
            mem_mb
        );
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
        Self::new(None)
    }
}