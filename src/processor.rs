//! CSV processing logic

use crate::stats::Stats;
use crate::transformer::Transformer;
use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cli::Args;

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
                let timestamp = Transformer::parse_datetime(date, time);
                if let Some(ts) = timestamp {
                    let formatted = Transformer::format_timestamp(&ts, timestamp_format);
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

/// Process the input CSV file
pub fn process_file(args: &Args) -> Result<()> {
    // Setup
    let input_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input.display()))?;

    let delimiter = args.delimiter.as_bytes().first().copied().unwrap_or(b',');
    let output_path = args
        .output
        .as_ref()
        .map(|p| p.clone())
        .unwrap_or_else(|| PathBuf::from("-"));

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
    stats.print_summary();

    Ok(())
}