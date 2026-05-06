use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mongo-exporter")]
#[command(about = "A beautiful CLI tool for exporting MongoDB collections")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(long_about = "
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          MongoDB Export CLI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Export MongoDB collections with ease and style. Built with Rust for performance
and reliability. Features interactive database and collection selection, custom
filtering, and multiple export formats.

EXAMPLES:
  mongo-exporter --uri mongodb://localhost:27017
  mongo-exporter -u mongodb://user:pass@cluster.mongodb.net/db

For more information, visit: https://github.com/your-username/mongo-export-cli
")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// MongoDB connection URI
    #[arg(short, long, global = true)]
    pub uri: Option<String>,
}

#[derive(Parser)]
pub struct ExportOptions {
    /// MongoDB connection URI (overrides global --uri)
    #[arg(short, long)]
    pub uri: Option<String>,

    /// Database name (required for non-interactive mode)
    #[arg(short, long)]
    pub database: Option<String>,

    /// Collection name (required for non-interactive mode)
    #[arg(short, long)]
    pub collection: Option<String>,

    /// Comma-separated list of fields to export (e.g., "name,email,age")
    #[arg(short, long)]
    pub fields: Option<String>,

    /// Maximum number of documents to export
    #[arg(short, long)]
    pub limit: Option<u64>,

    /// Number of documents to skip from the beginning
    #[arg(short, long)]
    pub skip: Option<u64>,

    /// Sort documents by field (e.g., "created_at:1" for ascending, "age:-1" for descending)
    #[arg(long)]
    pub sort: Option<String>,

    /// Export format: jsonl, json, csv, parquet, bson
    #[arg(long, value_enum)]
    pub format: Option<ExportFormatArg>,

    /// Filter query in JSON format (e.g., '{"status": "active"}')
    #[arg(short = 'q', long)]
    pub query: Option<String>,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<String>,

    /// Use configuration profile
    #[arg(short, long)]
    pub profile: Option<String>,

    /// Performance mode: balanced, memory, speed
    #[arg(long, value_enum)]
    pub perf_mode: Option<PerformanceModeArg>,

    /// Compression: none, gzip
    #[arg(long, value_enum)]
    pub compression: Option<CompressionArg>,

    /// Force resumable export (for large datasets)
    #[arg(long)]
    pub resumable: bool,

    /// Non-interactive mode (use CLI args only)
    #[arg(long)]
    pub non_interactive: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Export MongoDB collections interactively
    Export(Box<ExportOptions>),
    /// Resume a previously interrupted export
    Resume {
        /// Session ID to resume (optional - will list available sessions if not provided)
        session_id: Option<String>,
    },
    /// List available resumable export sessions
    List,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum ExportFormatArg {
    Jsonl,
    Json,
    Csv,
    Parquet,
    Bson,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum PerformanceModeArg {
    Balanced,
    Memory,
    Speed,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum CompressionArg {
    None,
    Gzip,
}
