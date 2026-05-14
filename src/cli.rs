//! CLI argument parsing for datagulp

use clap::Parser;
use std::path::PathBuf;

/// CLI arguments for datagulp
#[derive(Parser, Debug)]
#[command(
    name = "datagulp",
    about = "High-performance CSV preprocessing for QuestDB ingestion",
    version
)]
pub struct Args {
    /// Input CSV file (use - for stdin)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Output CSV file (use - for stdout)
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// CSV delimiter character
    #[arg(short = 'D', long, default_value = ",")]
    pub delimiter: String,

    /// Date column index (0-based)
    #[arg(short = 'd', long, default_value = "0")]
    pub date_col: usize,

    /// Time column index (0-based)
    #[arg(short = 't', long, default_value = "1")]
    pub time_col: usize,

    /// Combine date+time into single timestamp column
    #[arg(long)]
    pub combine_datetime: bool,

    /// Output format: 'iso8601', 'unix_epoch', or 'questdb'
    #[arg(long, default_value = "questdb")]
    pub timestamp_format: String,

    /// Skip the first N rows (e.g., header)
    #[arg(short = 's', long, default_value = "1")]
    pub skip_rows: usize,

    /// Number of rows to process (0 = all)
    #[arg(short, long, default_value = "0")]
    pub limit: usize,

    /// Batch size for processing
    #[arg(long, default_value = "100000")]
    pub batch_size: usize,

    /// Number of parallel workers (0 = auto)
    #[arg(short = 'w', long, default_value = "0")]
    pub workers: usize,

    /// Enable verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Print progress every N rows
    #[arg(long, default_value = "50000")]
    pub progress_every: usize,
}