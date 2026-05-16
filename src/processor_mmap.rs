//! High-performance CSV processing using memory-mapped files and parallelism

use crate::stats::Stats;
use anyhow::{Context, Result};
use csv::{WriterBuilder, ByteRecord};
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

        // Process in parallel - return ByteRecords for efficient writing
        let processed: Vec<ByteRecord> = lines
            .par_iter()
            .enumerate()
            .filter(|(idx, _)| *idx >= local_skip)
            .map(|(_, line)| {
                process_line_to_record(line, args.date_col, args.time_col, args.combine_datetime, &args.timestamp_format)
            })
            .collect();

        let count = processed.len();
        stats.inc_read(count);

        // Write batch to CSV output
        if !processed.is_empty() {
            for record in processed {
                writer.write_byte_record(&record).ok();
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

/// Process a raw line and return ByteRecord for efficient CSV writing
/// Avoids String allocations by working with byte slices
fn process_line_to_record(
    line: &[u8],
    date_col: usize,
    time_col: usize,
    combine_datetime: bool,
    timestamp_format: &str,
) -> ByteRecord {
    // Parse fields from line as bytes (respecting quotes)
    let cols = find_columns(line, b',');
    
    if !combine_datetime {
        // No transformation needed - convert bytes directly to record
        let mut record = ByteRecord::with_capacity(cols.len(), line.len());
        for col in cols {
            record.push_field(strip_field_quotes(col));
        }
        return record;
    }
    
    if date_col >= cols.len() || time_col >= cols.len() {
        // Invalid column indices - return as-is
        let mut record = ByteRecord::with_capacity(cols.len(), line.len());
        for col in cols {
            record.push_field(strip_field_quotes(col));
        }
        return record;
    }

    let date = cols[date_col];
    let time = cols[time_col];

    // Parse datetime from bytes
    let timestamp = parse_datetime_bytes(date, time);
    
    if let Some(ts) = timestamp {
        let formatted = format_timestamp_bytes(&ts, timestamp_format);
        
        // Build record: timestamp + remaining columns (skipping time_col)
        let num_fields = cols.len() - 1;
        let mut record = ByteRecord::with_capacity(num_fields, formatted.len() + line.len());
        
        for (i, col) in cols.iter().enumerate() {
            if i == date_col {
                record.push_field(&formatted);
            } else if i != time_col {
                record.push_field(strip_field_quotes(col));
            }
        }
        return record;
    }

    // Parsing failed - return original
    let mut record = ByteRecord::with_capacity(cols.len(), line.len());
    for col in cols {
        record.push_field(strip_field_quotes(col));
    }
    record
}

/// Strip surrounding quotes from a byte slice (returns slice without quotes)
fn strip_field_quotes(field: &[u8]) -> &[u8] {
    let len = field.len();
    if len >= 2 && field[0] == b'"' && field[len - 1] == b'"' {
        &field[1..len - 1]
    } else {
        field
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

    // Handle last field - trim trailing whitespace/newlines
    let line_len = line.len();
    let mut end = line_len;
    
    // Trim trailing newlines/carriage returns
    while end > start && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
        end -= 1;
    }

    if start < end {
        cols.push(&line[start..end]);
    }

    cols
}

/// Parse datetime from byte slices (avoid String allocation)
fn parse_datetime_bytes(date: &[u8], time: &[u8]) -> Option<DateTime> {
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

    // Parse time: HH:MM:SS.mmm
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

/// Format timestamp to bytes (avoid String allocation)
fn format_timestamp_bytes(dt: &DateTime, format: &str) -> Vec<u8> {
    // Pre-allocate buffer for max timestamp size
    let mut buf = Vec::with_capacity(30);
    
    // Year (4 digits)
    push_u32_padded(&mut buf, dt.year, 4);
    buf.push(b'-');
    
    // Month (2 digits)
    push_u32_padded(&mut buf, dt.month, 2);
    buf.push(b'-');
    
    // Day (2 digits)
    push_u32_padded(&mut buf, dt.day, 2);
    
    if format == "iso8601" {
        buf.push(b'T');
    } else {
        buf.push(b' ');
    }
    
    // Hour
    push_u32_padded(&mut buf, dt.hour, 2);
    buf.push(b':');
    
    // Minute
    push_u32_padded(&mut buf, dt.minute, 2);
    buf.push(b':');
    
    // Second
    push_u32_padded(&mut buf, dt.second, 2);
    buf.push(b'.');
    
    // Milliseconds (3 digits)
    push_u32_padded(&mut buf, dt.millis, 3);
    
    if format == "iso8601" {
        buf.push(b'Z');
    }
    
    buf
}

/// Push zero-padded u32 to buffer
fn push_u32_padded(buf: &mut Vec<u8>, value: u32, width: usize) {
    let digits = if value == 0 {
        b"0".to_vec()
    } else {
        let mut d = Vec::with_capacity(10);
        let mut v = value;
        while v > 0 {
            d.push(b'0' + (v % 10) as u8);
            v /= 10;
        }
        d.reverse();
        d
    };
    
    for _ in 0..(width.saturating_sub(digits.len())) {
        buf.push(b'0');
    }
    buf.extend_from_slice(&digits);
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
        let result = parse_datetime_bytes(b"1/15/2026", b"14:30:45.123");
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
        let result = parse_datetime_bytes(b"2026-01-15", b"14:30:45.123");
        assert!(result.is_some());
        let dt = result.unwrap();
        assert_eq!(dt.month, 1);
        assert_eq!(dt.day, 15);
        assert_eq!(dt.year, 2026);
    }

    #[test]
    fn test_process_line_to_record() {
        let line = b"1/1/2026,17:00:00.000,6902.00,4";
        let result = process_line_to_record(line, 0, 1, true, "iso8601");
        assert_eq!(result.len(), 3);
        // Check first field starts with timestamp pattern
        let fields: Vec<&[u8]> = result.iter().collect();
        assert!(fields[0].starts_with(b"2026-01-01T"));
        assert_eq!(fields[1], b"6902.00");
        assert_eq!(fields[2], b"4");
    }

    #[test]
    fn test_format_timestamp_bytes() {
        let dt = DateTime {
            year: 2026, month: 1, day: 1, hour: 17, minute: 0, second: 0, millis: 123
        };
        let result = format_timestamp_bytes(&dt, "iso8601");
        assert_eq!(&result, b"2026-01-01T17:00:00.123Z");
        
        let result2 = format_timestamp_bytes(&dt, "questdb");
        assert_eq!(&result2, b"2026-01-01 17:00:00.123");
    }

    #[test]
    fn test_strip_field_quotes() {
        assert_eq!(strip_field_quotes(b"\"hello\""), b"hello");
        assert_eq!(strip_field_quotes(b"hello"), b"hello");
        assert_eq!(strip_field_quotes(b"\"a,b\""), b"a,b");
    }
}