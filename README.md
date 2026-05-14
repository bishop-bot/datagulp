# DataGulp

High-performance CLI tool for preprocessing and transforming large CSV files (10+ GB) into readily parseable CSV files for ingesting into QuestDB.

## Features

- **High throughput**: Parallel batch processing with Rayon for maximum performance
- **Flexible date/time handling**: Auto-detects multiple date/time formats
- **QuestDB-optimized output**: Formats timestamps for immediate QuestDB ingestion
- **Chunked processing**: Handles files of any size without memory issues
- **Progress reporting**: Real-time statistics on processing throughput

## Installation

```bash
cargo build --release
```

## Usage

```bash
# Transform date+time columns into QuestDB timestamp format
datagulp -i input.csv -o output.csv --combine-datetime

# Custom date/time column indices
datagulp -i input.csv -o output.csv --combine-datetime -d 2 -t 3

# Process with different timestamp formats
datagulp -i input.csv -o output.csv --combine-datetime --timestamp-format iso8601
datagulp -i input.csv -o output.csv --combine-datetime --timestamp-format unix_epoch

# Process only first N rows
datagulp -i input.csv -o output.csv --limit 100000

# Adjust batch size and workers
datagulp -i input.csv -o output.csv --batch-size 50000 -w 8
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input` | Input CSV file (use `-` for stdin) | Required |
| `-o, --output` | Output CSV file (use `-` for stdout) | stdout |
| `-D, --delimiter` | CSV delimiter character | `,` |
| `-d, --date-col` | Date column index (0-based) | `0` |
| `-t, --time-col` | Time column index (0-based) | `1` |
| `--combine-datetime` | Combine date+time into single timestamp | false |
| `--timestamp-format` | Output format: questdb, iso8601, unix_epoch | questdb |
| `-s, --skip-rows` | Skip first N rows | `1` |
| `-l, --limit` | Number of rows to process (0 = all) | `0` |
| `--batch-size` | Batch size for processing | `100000` |
| `-w, --workers` | Number of parallel workers (0 = auto) | `0` |
| `--progress-every` | Print progress every N rows | `50000` |

## Supported Date/Time Formats

### Date
- `MM/DD/YYYY` (e.g., 1/1/2026)
- `YYYY-MM-DD` (e.g., 2026-01-01)
- `DD/MM/YYYY`
- `YYYY/MM/DD`

### Time
- `HH:MM:SS.fff` (e.g., 17:00:00.000)
- `HH:MM:SS` (e.g., 17:00:00)

## Performance

Benchmarks on 1M+ row files:
- ~3.3M rows/second on M-series Mac
- Memory-efficient batch processing
- Configurable parallel workers

## License

MIT