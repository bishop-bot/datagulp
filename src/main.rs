//! DataGulp - High-performance CSV preprocessing tool for QuestDB ingestion
//!
//! Transforms large CSV files into optimized formats for time-series data ingestion.

use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

mod transformer;

/// CLI arguments for datagulp
#[derive(Parser, Debug)]
#[command(
    name = "datagulp",
    about = "High-performance CSV preprocessing for QuestDB ingestion",
    version
)]
struct Args {
    /// Input CSV file (use - for stdin)
    #[arg(short, long)]
    input: PathBuf,

    /// Output CSV file (use - for stdout)
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    /// CSV delimiter character
    #[arg(short = 'D', long, default_value = ",")]
    delimiter: String,

    /// Date column index (0-based)
    #[arg(short = 'd', long, default_value = "0")]
    date_col: usize,

    /// Time column index (0-based)
    #[arg(short = 't', long, default_value = "1")]
    time_col: usize,

    /// Combine date+time into single timestamp column
    #[arg(long)]
    combine_datetime: bool,

    /// Output format: 'iso8601', 'unix_epoch', or 'questdb'
    #[arg(long, default_value = "questdb")]
    timestamp_format: String,

    /// Skip the first N rows (e.g., header)
    #[arg(short = 's', long, default_value = "1")]
    skip_rows: usize,

    /// Number of rows to process (0 = all)
    #[arg(short, long, default_value = "0")]
    limit: usize,

    /// Batch size for processing
    #[arg(long, default_value = "100000")]
    batch_size: usize,

    /// Number of parallel workers (0 = auto)
    #[arg(short = 'w', long, default_value = "0")]
    workers: usize,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Print progress every N rows
    #[arg(long, default_value = "50000")]
    progress_every: usize,
}

/// Custom error types for datagulp
#[derive(Error, Debug)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Parse error at row {row}: {message}")]
    Parse { row: usize, message: String },

    #[error("Invalid argument: {0}")]
    InvalidArg(String),
}

/// Processing statistics
struct Stats {
    rows_read: AtomicUsize,
    rows_written: AtomicUsize,
    rows_failed: AtomicUsize,
    start_time: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            rows_read: AtomicUsize::new(0),
            rows_written: AtomicUsize::new(0),
            rows_failed: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    fn print(&self, force: bool) {
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
}

/// Process a single batch of rows
fn process_batch(
    batch: Vec<String>,
    date_col: usize,
    time_col: usize,
    combine_datetime: bool,
    timestamp_format: &str,
) -> Vec<String> {
    batch
        .par_iter()
        .map(|row| {
            let cols: Vec<&str> = row.split(',').collect();

            if combine_datetime && date_col < cols.len() && time_col < cols.len() {
                let date = cols[date_col];
                let time = cols[time_col];

                // Parse and format timestamp
                let timestamp = transformer::Transformer::parse_datetime(date, time);
                if let Some(ts) = timestamp {
                    let formatted = transformer::Transformer::format_timestamp(&ts, timestamp_format);
                    // Replace date+time columns with formatted timestamp
                    let mut new_cols: Vec<&str> = cols.to_vec();
                    new_cols[date_col] = &formatted;
                    new_cols.remove(time_col); // Remove time column
                    return new_cols.join(",");
                }
            }

            row.to_string()
        })
        .collect()
}

/// Write processed rows to the output writer
fn write_batch<W: Write>(writer: &mut csv::Writer<W>, results: Vec<String>) -> usize {
    let mut count = 0;
    for result in results {
        writer.write_record(result.split(',')).ok();
        count += 1;
    }
    count
}

/// Main processing function
fn process_file(args: &Args) -> Result<()> {
    let start = Instant::now();

    // Setup
    let input_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input.display()))?;

    let delimiter = args.delimiter.as_bytes().first().copied().unwrap_or(b',');
    let output_path = args.output.as_ref().map(|p| p.clone()).unwrap_or_else(|| PathBuf::from("-"));

    let output_file: Box<dyn Write> = if output_path.as_os_str() == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(BufWriter::new(File::create(&output_path)?))
    };

    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_reader(BufReader::new(input_file));

    let mut writer = WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(output_file);

    let stats = Arc::new(Stats::new());
    let cancelled = Arc::new(AtomicBool::new(false));

    // Handle SIGINT for graceful shutdown
    #[cfg(unix)]
    {
        let cancelled_clone = cancelled.clone();
        std::panic::set_hook(Box::new(move |_| {
            cancelled_clone.store(true, Ordering::SeqCst);
        }));
    }

    // Process in batches
    let mut batch: Vec<String> = Vec::with_capacity(args.batch_size);
    let mut processed = 0;

    for result in reader.records() {
        if cancelled.load(Ordering::SeqCst) {
            eprintln!("\nCancelled by user");
            break;
        }

        if processed < args.skip_rows {
            processed += 1;
            continue;
        }

        match result {
            Ok(record) => {
                let row = record.iter().collect::<Vec<_>>().join(",");
                batch.push(row);
                stats.rows_read.fetch_add(1, Ordering::Relaxed);

                if batch.len() >= args.batch_size {
                    let results = process_batch(
                        std::mem::take(&mut batch),
                        args.date_col,
                        args.time_col,
                        args.combine_datetime,
                        &args.timestamp_format,
                    );

                    let written = write_batch(&mut writer, results);
                    stats.rows_written.fetch_add(written, Ordering::Relaxed);
                }

                if stats.rows_read.load(Ordering::Relaxed) % args.progress_every == 0 {
                    stats.print(false);
                }
            }
            Err(_e) => {
                stats.rows_failed.fetch_add(1, Ordering::Relaxed);
            }
        }

        if args.limit > 0 && stats.rows_read.load(Ordering::Relaxed) >= args.limit {
            break;
        }
    }

    // Process remaining batch
    if !batch.is_empty() {
        let results = process_batch(
            std::mem::take(&mut batch),
            args.date_col,
            args.time_col,
            args.combine_datetime,
            &args.timestamp_format,
        );

        let written = write_batch(&mut writer, results);
        stats.rows_written.fetch_add(written, Ordering::Relaxed);
    }

    writer.flush()?;
    stats.print(true);

    let elapsed = start.elapsed();
    let total = stats.rows_read.load(Ordering::Relaxed);
    let rate = if elapsed.as_secs() > 0 {
        total as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    eprintln!("\nCompleted in {:.2}s ({:.0} rows/s)", elapsed.as_secs_f64(), rate);
    eprintln!(
        "Summary: {} read | {} written | {} failed",
        stats.rows_read.load(Ordering::Relaxed),
        stats.rows_written.load(Ordering::Relaxed),
        stats.rows_failed.load(Ordering::Relaxed)
    );

    Ok(())
}

fn main() {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(buf, "{} - {}", record.level(), record.args())
        })
        .init();

    // Parse arguments
    let args = Args::parse();

    if args.verbose {
        eprintln!("datagulp v{} starting...", env!("CARGO_PKG_VERSION"));
        eprintln!("Input: {:?}", args.input);
        eprintln!("Output: {:?}", args.output);
        eprintln!("Batch size: {}", args.batch_size);
    }

    // Run
    if let Err(e) = process_file(&args) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}