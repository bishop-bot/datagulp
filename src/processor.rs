//! CSV processing logic

use crate::stats::Stats;
use crate::transformer::Transformer;
use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cli::Args;

/// Process a single batch of rows
fn process_batch(
    batch: Vec<Vec<String>>,
    date_col: usize,
    time_col: usize,
    combine_datetime: bool,
    timestamp_format: &str,
) -> Vec<Vec<String>> {
    batch
        .into_par_iter()
        .map(|mut cols| {
            if combine_datetime && date_col < cols.len() && time_col < cols.len() {
                let date = &cols[date_col];
                let time = &cols[time_col];

                // Parse and format timestamp
                let timestamp = Transformer::parse_datetime(date, time);
                if let Some(ts) = timestamp {
                    let formatted = Transformer::format_timestamp(&ts, timestamp_format);
                    // Replace date column with formatted timestamp, remove time column
                    cols[date_col] = formatted;
                    cols.remove(time_col);
                }
            }
            cols
        })
        .collect()
}

/// Write processed rows to the output writer
fn write_batch<W: Write>(writer: &mut csv::Writer<W>, results: Vec<Vec<String>>) -> usize {
    let mut count = 0;
    for fields in results {
        writer.write_record(&fields).ok();
        count += 1;
    }
    count
}

/// Count total lines in a file (for progress bar total)
fn count_lines(path: &PathBuf) -> Option<u64> {
    let file = File::open(path).ok()?;
    let count = std::io::BufReader::new(file)
        .lines()
        .count()
        .try_into()
        .ok()?;
    Some(count)
}

/// Process the input CSV file
pub fn process_file(args: &Args) -> Result<()> {
    // Setup
    let input_file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input.display()))?;

    let delimiter = args.delimiter.as_bytes().first().copied().unwrap_or(b',');
    let output_path = args.output.clone().unwrap_or_else(|| PathBuf::from("-"));

    let output_file: Box<dyn Write> = if output_path.as_os_str() == "-" {
        Box::new(std::io::stdout())
    } else {
        Box::new(BufWriter::new(File::create(&output_path)?))
    };

    // Count total lines for progress bar (skip header row if present)
    let total_rows = count_lines(&args.input).map(|c| {
        if args.skip_rows > 0 { c.saturating_sub(args.skip_rows as u64) } else { c }
    });

    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .from_reader(BufReader::new(input_file));

    let mut writer = WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(output_file);

    let stats = Arc::new(Stats::new(total_rows));
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
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(args.batch_size);
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
                // Keep fields as proper Vec<String> to preserve CSV structure
                let fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                batch.push(fields);
                stats.inc_read(1);

                if batch.len() >= args.batch_size {
                    let results = process_batch(
                        std::mem::take(&mut batch),
                        args.date_col,
                        args.time_col,
                        args.combine_datetime,
                        &args.timestamp_format,
                    );

                    let written = write_batch(&mut writer, results);
                    stats.inc_written(written);
                }
            }
            Err(_e) => {
                stats.inc_failed(1);
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
        stats.inc_written(written);
    }

    writer.flush()?;
    stats.finish();
    stats.print_summary();

    Ok(())
}