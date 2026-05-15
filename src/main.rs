//! DataGulp - High-performance CSV preprocessing tool for QuestDB ingestion
//!
//! Transforms large CSV files into optimized formats for time-series data ingestion.

mod cli;
mod processor;
mod processor_mmap;
mod stats;
mod transformer;

use clap::Parser;
use cli::Args;
use std::io::Write;

/// Main entry point
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

    // Run processor
    let result = if args.mmap {
        processor_mmap::process_file(&args)
    } else {
        processor::process_file(&args)
    };


    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}