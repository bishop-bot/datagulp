# DataGulp

High-performance CLI tool for preprocessing and transforming large CSV files (10+ GB) into readily parseable CSV files for ingesting into QuestDB.

## Features

- **High throughput**: Memory-mapped parallel processing with Rayon for maximum performance
- **Flexible date/time handling**: Auto-detects multiple date/time formats
- **QuestDB-optimized output**: Formats timestamps for immediate QuestDB ingestion
- **Chunked processing**: Handles files of any size without memory issues
- **Progress reporting**: Real-time progress bar with ETA, memory usage, and throughput metrics

## Installation

```bash
cargo build --release
```

## Usage

```bash
# Standard mode (batch processing)
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --combine-datetime

# High-performance mode for large files (10+ GB)
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --combine-datetime --mmap

# Custom date/time column indices
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --combine-datetime -d 2 -t 3

# Process with different timestamp formats
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --combine-datetime --timestamp-format iso8601
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --combine-datetime --timestamp-format unix_epoch

# Process only first N rows
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --limit 100000

# Adjust batch size and workers
datagulp -i input.csv -o output.csv --symbol ES --mic XCME --batch-size 50000 -w 8
```

## Output Format

The output CSV includes `symbol` and `mic` as the first two columns, followed by the transformed data:

```
symbol,mic,ts,price,size
ES,XCME,2026-01-01 17:00:00.000,6902.00,4
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input` | Input CSV file (use `-` for stdin) | Required |
| `-o, --output` | Output CSV file (use `-` for stdout) | stdout |
| `--symbol` | Ticker symbol (added as first column in output) | Required |
| `--mic` | Market Identifier Code - ISO 10383 | Required |
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
| `--mmap` | Use memory-mapped processing (faster for large files) | false |

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
- **Standard mode**: ~800K rows/second
- **Memory-mapped mode**: ~2M+ rows/second (3x faster)
- Configurable parallel workers
- Memory-efficient batch processing

Use `--mmap` for files over 1GB to leverage memory-mapped I/O and parallel processing.

## License

MIT