//! High-performance CSV processing using memory-mapped files and parallelism

use crate::stats::Stats;
use anyhow::{Context, Result};
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

    let mut output_file: Box<dyn Write> = if output_path.as_os_str() == "-" {
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

        // Process in parallel using Rayon - use byte processing for speed
        let processed: Vec<String> = lines
            .par_iter()
            .enumerate()
            .filter(|(idx, _)| *idx >= local_skip)
            .map(|(_, line)| {
                process_line_to_string(line, delimiter, args.date_col, args.time_col, args.combine_datetime, &args.timestamp_format)
            })
            .collect();

        let count = processed.len();
        stats.inc_read(count);

        // Write to output - batch write for efficiency
        if !processed.is_empty() {
            // Join all lines with newline and write in one syscall
            let mut output = Vec::with_capacity(count * 50);
            for line in &processed {
                output.extend_from_slice(line.as_bytes());
                output.push(b'\n');
            }
            output_file.write_all(&output).ok();
            stats.inc_written(count);
        }

        offset += last_newline;

        if args.limit > 0 && stats.rows_read.load(Ordering::Relaxed) >= args.limit {
            break;
        }
    }

    output_file.flush()?;
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

/// Process a raw line to String - handles quoting correctly
fn process_line_to_string(
    line: &[u8],
    delimiter: u8,
    date_col: usize,
    time_col: usize,
    combine_datetime: bool,
    timestamp_format: &str,
) -> String {
    // Parse fields from line, respecting quoted fields
    let cols = find_columns_with_quotes(line, delimiter);
    
    if !combine_datetime {
        // No transformation - return as CSV with proper quoting
        return build_csv_row(&cols, delimiter);
    }
    
    if date_col >= cols.len() || time_col >= cols.len() {
        return build_csv_row(&cols, delimiter);
    }

    let date = cols[date_col];
    let time = cols[time_col];

    // Parse datetime
    let timestamp = parse_datetime(date, time);
    
    if let Some(ts) = timestamp {
        let formatted = format_timestamp(&ts, timestamp_format);
        
        // Build result: timestamp + remaining columns (skipping time_col)
        let mut result = Vec::with_capacity(line.len());
        for (i, col) in cols.iter().enumerate() {
            if i == date_col {
                // Add timestamp (may need quoting if it contains delimiter)
                extend_with_csv_field(&mut result, formatted.as_bytes(), delimiter);
            } else if i != time_col {
                result.push(delimiter);
                extend_with_csv_field(&mut result, col, delimiter);
            }
        }
        return String::from_utf8_lossy(&result).into_owned();
    }

    build_csv_row(&cols, delimiter)
}

/// Build a properly quoted CSV row from fields
fn build_csv_row(cols: &[&[u8]], delimiter: u8) -> String {
    let mut result = Vec::with_capacity(cols.len() * 20);
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            result.push(delimiter);
        }
        extend_with_csv_field(&mut result, col, delimiter);
    }
    String::from_utf8_lossy(&result).into_owned()
}

/// Extend buffer with a CSV field, adding quotes and escaping if needed
fn extend_with_csv_field(buf: &mut Vec<u8>, field: &[u8], delimiter: u8) {
    let needs_quotes = field.iter().any(|&b| b == b'"' || b == b'\n' || b == b'\r' || b == delimiter);
    
    if needs_quotes {
        buf.push(b'"');
        for &byte in field {
            if byte == b'"' {
                buf.push(b'"');
                buf.push(b'"');
            } else {
                buf.push(byte);
            }
        }
        buf.push(b'"');
    } else {
        buf.extend_from_slice(field);
    }
}

/// Find column boundaries with proper quoted field support
/// Returns fields with quotes stripped
fn find_columns_with_quotes(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
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
                    let field = trim_field(&line[start..i]);
                    cols.push(field);
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    // Handle last field - trim trailing newlines and surrounding quotes
    let line_len = line.len();
    let mut end = line_len;
    while end > start && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
        end -= 1;
    }
    if start < end {
        let field = trim_field(&line[start..end]);
        cols.push(field);
    }

    cols
}

/// Trim whitespace and surrounding quotes from a field
fn trim_field(field: &[u8]) -> &[u8] {
    let trimmed = trim_bytes(field);
    let len = trimmed.len();
    if len >= 2 && trimmed[0] == b'"' && trimmed[len - 1] == b'"' {
        &trimmed[1..len - 1]
    } else {
        trimmed
    }
}

/// Parse datetime from byte slices
fn parse_datetime(date: &[u8], time: &[u8]) -> Option<DateTime> {
    let (month, day, year) = if date.contains(&b'/') {
        let parts = split_bytes(date, b'/');
        if parts.len() != 3 {
            return None;
        }
        (
            parse_u32(parts[0])?,
            parse_u32(parts[1])?,
            parse_u32(parts[2])?,
        )
    } else {
        let parts = split_bytes(date, b'-');
        if parts.len() != 3 {
            return None;
        }
        (
            parse_u32(parts[1])?,
            parse_u32(parts[2])?,
            parse_u32(parts[0])?,
        )
    };

    let time_trimmed = trim_bytes(time);
    let time_parts = split_bytes(time_trimmed, b':');
    if time_parts.len() < 2 {
        return None;
    }

    let hour = parse_u32(time_parts[0])?;
    let minute = parse_u32(time_parts[1])?;

    let (second, millis) = if time_parts.len() > 2 {
        let sec_parts = split_bytes(time_parts[2], b'.');
        let sec = parse_u32(sec_parts[0]).unwrap_or(0);
        let ms = if sec_parts.len() > 1 {
            parse_u32(truncate_bytes(sec_parts[1], 3)).unwrap_or(0)
        } else {
            0
        };
        (sec, ms)
    } else {
        (0, 0)
    };

    Some(DateTime { year, month, day, hour, minute, second, millis })
}

/// Split bytes by delimiter
fn split_bytes(data: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    for (i, &byte) in data.iter().enumerate() {
        if byte == delimiter {
            if i > start {
                parts.push(&data[start..i]);
            }
            start = i + 1;
        }
    }
    parts.push(&data[start..]);
    parts
}

/// Trim whitespace from bytes
fn trim_bytes(data: &[u8]) -> &[u8] {
    let start = data.iter().position(|&b| !is_whitespace(b)).unwrap_or(data.len());
    let end = data.len() - data.iter().rev().position(|&b| !is_whitespace(b)).unwrap_or(data.len());
    &data[start..end]
}

fn is_whitespace(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == b'\r' || b == b'\n'
}

/// Parse unsigned 32 from bytes
fn parse_u32(data: &[u8]) -> Option<u32> {
    let mut result: u32 = 0;
    for &byte in data {
        if byte < b'0' || byte > b'9' {
            return None;
        }
        result = result * 10 + (byte - b'0') as u32;
    }
    Some(result)
}

/// Truncate bytes to max length
fn truncate_bytes(data: &[u8], max_len: usize) -> &[u8] {
    if data.len() <= max_len {
        data
    } else {
        &data[..max_len]
    }
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
    fn test_find_columns_with_quotes() {
        let line = b"a,\"b,c\",d";
        let cols = find_columns_with_quotes(line, b',');
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], b"a");
        assert_eq!(cols[1], b"b,c");  // quotes stripped
        assert_eq!(cols[2], b"d");
    }

    #[test]
    fn test_parse_datetime_mdy() {
        let result = parse_datetime(b"1/15/2026", b"14:30:45.123");
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
        let result = parse_datetime(b"2026-01-15", b"14:30:45.123");
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.month, 1);
        assert_eq!(dt.day, 15);
        assert_eq!(dt.year, 2026);
    }

    #[test]
    fn test_process_line_to_string() {
        let line = b"1/1/2026,17:00:00.000,6902.00,4";
        let result = process_line_to_string(line, b',', 0, 1, true, "iso8601");
        assert!(result.starts_with("2026-01-01T"));
        assert!(result.contains(",6902.00,"));
    }

    #[test]
    fn test_build_csv_row() {
        let cols: Vec<&[u8]> = vec![b"a", b"b,c", b"d"];
        let result = build_csv_row(&cols, b',');
        assert_eq!(result, "a,\"b,c\",d");
    }
}