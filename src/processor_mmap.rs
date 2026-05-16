//! High-performance CSV processing using memory-mapped files and parallelism

use crate::stats::Stats;
use anyhow::{Context, Result};
use csv::WriterBuilder;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::cli::Args;

/// Memory-mapped file processor for maximum I/O performance
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

    // Memory-map the input file for fast I/O
    let mmap = unsafe { Mmap::map(&input_file) }
        .with_context(|| format!("Failed to mmap input file: {}", args.input.display()))?;

    let file_size = mmap.len() as u64;
    let total_rows = if args.limit > 0 {
        Some(args.limit as u64)
    } else {
        Some(file_size / 50)
    };

    let stats = Arc::new(Stats::new(total_rows));
    let cancelled = Arc::new(AtomicBool::new(false));

    #[cfg(unix)]
    {
        let cancelled_clone = cancelled.clone();
        std::panic::set_hook(Box::new(move |_| {
            cancelled_clone.store(true, Ordering::SeqCst);
        }));
    }

    // Create CSV writer
    let mut writer = WriterBuilder::new()
        .delimiter(delimiter)
        .from_writer(output_file);

    // Process in large chunks
    let chunk_size = args.batch_size.max(100_000);
    let skip_rows = args.skip_rows;
    let mut offset = 0;

    while offset < mmap.len() {
        if cancelled.load(Ordering::SeqCst) {
            eprintln!("\nCancelled by user");
            break;
        }

        // Find chunk boundary
        let chunk_end = (offset + chunk_size * 50).min(mmap.len());
        let remaining = &mmap[offset..chunk_end];

        let last_newline = remaining
            .iter()
            .rev()
            .position(|&b| b == b'\n')
            .map(|p| remaining.len() - p)
            .unwrap_or(remaining.len());

        let chunk = &mmap[offset..offset + last_newline];
        let local_skip = if offset == 0 { skip_rows } else { 0 };

        // Split into lines and process
        let lines = split_lines_fast(chunk);

        // Process in parallel using Rayon
        let processed: Vec<Vec<String>> = lines
            .par_iter()
            .enumerate()
            .filter(|(idx, _)| *idx >= local_skip)
            .map(|(_, line)| {
                process_line_to_fields(line, args.date_col, args.time_col, args.combine_datetime, &args.timestamp_format)
            })
            .collect();

        let count = processed.len();
        stats.inc_read(count);

        // Write to CSV output
        if !processed.is_empty() {
            for fields in processed {
                writer.write_record(&fields).ok();
            }
            stats.inc_written(count);
        }

        offset += last_newline;

        if args.limit > 0 && stats.rows_read.load(Ordering::Relaxed) >= args.limit {
            break;
        }
    }

    writer.flush()?;
    stats.finish();
    stats.print_summary();

    Ok(())
}

/// Fast line splitting
fn split_lines_fast(data: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::with_capacity(1024);
    let mut start = 0;

    for i in 0..data.len() {
        if data[i] == b'\n' {
            if i > start {
                lines.push(&data[start..i]);
            }
            start = i + 1;
        }
    }

    if start < data.len() {
        lines.push(&data[start..]);
    }

    lines
}

/// Process a raw line and return fields
fn process_line_to_fields(
    line: &[u8],
    date_col: usize,
    time_col: usize,
    combine_datetime: bool,
    timestamp_format: &str,
) -> Vec<String> {
    // Parse fields from line
    let cols = find_columns(line, b',');
    
    // Convert to owned Strings
    let mut fields: Vec<String> = cols.iter()
        .map(|c| strip_quotes(String::from_utf8_lossy(c)))
        .collect();

    if combine_datetime && date_col < fields.len() && time_col < fields.len() {
        let date = &fields[date_col];
        let time = &fields[time_col];

        let timestamp = parse_datetime(date, time);
        if let Some(ts) = timestamp {
            let formatted = format_timestamp(&ts, timestamp_format);
            fields[date_col] = formatted;
            fields.remove(time_col);
        }
    }

    fields
}

/// Strip surrounding quotes from a string
fn strip_quotes(s: std::borrow::Cow<str>) -> String {
    let s = s.trim();
    if s.starts_with('"') && s.ends_with('"') {
        s[1..s.len()-1].to_string()
    } else {
        s.to_string()
    }
}

/// Find column boundaries, respecting quoted fields
fn find_columns(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut cols = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;

    for i in 0..line.len() {
        let byte = line[i];
        match byte {
            b'"' => {
                in_quotes = !in_quotes;
            }
            c if !in_quotes && c == delimiter => {
                if i > start {
                    cols.push(&line[start..i]);
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    // Handle last field (without trailing newline)
    let line_len = line.len();
    // Trim trailing newline if present
    let end = if line_len > 0 && line[line_len - 1] == b'\n' {
        line_len - 1
    } else {
        line_len
    };

    if start < end {
        cols.push(&line[start..end]);
    }

    cols
}

/// Parse datetime
fn parse_datetime(date: &str, time: &str) -> Option<DateTime> {
    let (month, day, year) = if date.contains('/') {
        let parts: Vec<&str> = date.split('/').collect();
        if parts.len() != 3 {
            return None;
        }
        (
            parts[0].parse::<u32>().ok()?,
            parts[1].parse::<u32>().ok()?,
            parts[2].parse::<u32>().ok()?,
        )
    } else {
        let parts: Vec<&str> = date.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        (
            parts[1].parse::<u32>().ok()?,
            parts[2].parse::<u32>().ok()?,
            parts[0].parse::<u32>().ok()?,
        )
    };

    let time = time.trim();
    let time_parts: Vec<&str> = time.split(':').collect();
    if time_parts.len() < 2 {
        return None;
    }

    let hour: u32 = time_parts[0].parse().ok()?;
    let minute: u32 = time_parts[1].parse().ok()?;

    let second = if time_parts.len() > 2 {
        let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
        sec_parts[0].parse().ok()?
    } else {
        0
    };

    let millis = if time_parts.len() > 2 {
        let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
        if sec_parts.len() > 1 {
            let ms_str = &sec_parts[1][..sec_parts[1].len().min(3)];
            ms_str.parse::<u32>().ok().unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    Some(DateTime { year, month, day, hour, minute, second, millis })
}

/// Format timestamp
fn format_timestamp(dt: &DateTime, format: &str) -> String {
    let ms = format!("{:03}", dt.millis);
    match format {
        "questdb" | "" => {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{}",
                dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, ms
            )
        }
        "iso8601" => {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{}Z",
                dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, ms
            )
        }
        _ => {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{}",
                dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, ms
            )
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DateTime {
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millis: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_lines() {
        let data = b"line1\nline2\nline3";
        let lines = split_lines_fast(data);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], b"line1");
        assert_eq!(lines[1], b"line2");
        assert_eq!(lines[2], b"line3");
    }

    #[test]
    fn test_find_columns() {
        let line = b"a,b,c,d";
        let cols = find_columns(line, b',');
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], b"a");
        assert_eq!(cols[1], b"b");
        assert_eq!(cols[2], b"c");
        assert_eq!(cols[3], b"d");
    }

    #[test]
    fn test_find_columns_with_quotes() {
        let line = b"a,\"b,c\",d";
        let cols = find_columns(line, b',');
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], b"a");
        assert_eq!(cols[1], b"\"b,c\"");
        assert_eq!(cols[2], b"d");
    }

    #[test]
    fn test_parse_datetime_mdy() {
        let result = parse_datetime("1/15/2026", "14:30:45.123");
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.month, 1);
        assert_eq!(dt.day, 15);
        assert_eq!(dt.year, 2026);
        assert_eq!(dt.hour, 14);
        assert_eq!(dt.minute, 30);
        assert_eq!(dt.second, 45);
        assert_eq!(dt.millis, 123);
    }

    #[test]
    fn test_parse_datetime_ymd() {
        let result = parse_datetime("2026-01-15", "14:30:45.123");
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.month, 1);
        assert_eq!(dt.day, 15);
        assert_eq!(dt.year, 2026);
    }

    #[test]
    fn test_process_line_to_fields() {
        let line = b"1/1/2026,17:00:00.000,6902.00,4";
        let result = process_line_to_fields(line, 0, 1, true, "iso8601");
        assert_eq!(result.len(), 3);
        assert!(result[0].starts_with("2026-01-01T"));
        assert_eq!(result[1], "6902.00");
        assert_eq!(result[2], "4");
    }
}