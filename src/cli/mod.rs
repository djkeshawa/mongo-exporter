use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Public command-line contract for the v1 automation-first interface.
#[derive(Debug, Parser)]
#[command(
    name = "mongo-exporter",
    version,
    about = "Reproducible MongoDB collection exports"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Optional path to a profile configuration file. It is never created implicitly.
    #[arg(long, global = true, env = "MONGO_EXPORTER_CONFIG")]
    pub config: Option<PathBuf>,

    /// Force or disable ANSI styling for interactive output.
    #[arg(long, global = true, value_enum, default_value_t = ColorMode::Auto)]
    pub color: ColorMode,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant, clippy::enum_variant_names)]
pub enum Commands {
    /// Export one MongoDB collection without prompting.
    Export(Box<ExportArgs>),
    /// Optional guided workflow for local operators.
    Wizard(WizardArgs),
    /// Inspect source metadata without writing export data.
    #[command(subcommand)]
    Inspect(InspectCommand),
    /// Manage opt-in interrupted-export checkpoints.
    #[command(subcommand)]
    Checkpoint(CheckpointCommand),
    /// Manage non-secret local configuration.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Generate shell completion scripts.
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
}

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// MongoDB URI. Prefer --uri-env in automated jobs to avoid shell history leaks.
    #[arg(long, conflicts_with_all = ["uri_env", "profile"])]
    pub uri: Option<String>,

    /// Environment variable containing the MongoDB URI.
    #[arg(long, conflicts_with_all = ["uri", "profile"])]
    pub uri_env: Option<String>,

    /// Non-secret profile name.
    #[arg(long, conflicts_with_all = ["uri", "uri_env"])]
    pub profile: Option<String>,

    /// Database name.
    #[arg(long)]
    pub database: String,

    /// Collection name.
    #[arg(long)]
    pub collection: String,

    /// Inline MongoDB Extended JSON filter.
    #[arg(long, conflicts_with = "query_file")]
    pub query: Option<String>,

    /// File containing a MongoDB Extended JSON filter.
    #[arg(long, conflicts_with = "query")]
    pub query_file: Option<PathBuf>,

    /// Comma-separated projection fields.
    #[arg(long)]
    pub fields: Option<String>,

    /// Maximum number of documents to export.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub limit: Option<u64>,

    /// Number of documents to skip. Not valid with --checkpoint-dir.
    #[arg(long)]
    pub skip: Option<u64>,

    /// Sort specification such as `_id:1` or `created_at:-1`.
    #[arg(long)]
    pub sort: Option<String>,

    /// Output format.
    #[arg(long, value_enum)]
    pub format: ExportFormatArg,

    /// Destination path, or '-' for supported streaming formats.
    #[arg(long)]
    pub output: PathBuf,

    /// Replace an existing destination atomically.
    #[arg(long)]
    pub overwrite: bool,

    /// Create missing output directories explicitly.
    #[arg(long)]
    pub create_dirs: bool,

    /// Compression codec.
    #[arg(long, value_enum, default_value_t = CompressionArg::None)]
    pub compression: CompressionArg,

    /// Source consistency contract.
    #[arg(long, value_enum, default_value_t = ConsistencyArg::BestEffort)]
    pub consistency: ConsistencyArg,

    /// JSON representation for JSON and JSONL formats.
    #[arg(long, value_enum, default_value_t = JsonModeArg::Canonical)]
    pub json_mode: JsonModeArg,

    /// Versioned schema manifest required for CSV and Parquet.
    #[arg(long)]
    pub schema: Option<PathBuf>,

    /// Write a final machine-readable report to this path.
    #[arg(long)]
    pub report: Option<PathBuf>,

    /// Human or NDJSON event stream format on stderr.
    #[arg(long, value_enum, default_value_t = LogFormatArg::Human)]
    pub log_format: LogFormatArg,

    /// Exact count before export. Disabled by default for large collections.
    #[arg(long)]
    pub count: bool,

    /// Enable best-effort checkpointing for supported formats.
    #[arg(long)]
    pub checkpoint_dir: Option<PathBuf>,

    /// MongoDB cursor batch size.
    #[arg(long, default_value_t = 1_000)]
    pub batch_size: u32,

    /// Suppress progress events while retaining errors and the final report.
    #[arg(long)]
    pub no_progress: bool,
}

#[derive(Debug, Args)]
pub struct WizardArgs {
    /// Optional URI used by the guided workflow.
    #[arg(long)]
    pub uri: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum InspectCommand {
    /// Generate a versioned candidate schema manifest.
    Schema(SchemaInspectArgs),
}

#[derive(Debug, Args)]
pub struct SchemaInspectArgs {
    #[arg(long, conflicts_with = "uri_env")]
    pub uri: Option<String>,
    #[arg(long, default_value = "MONGODB_URI", conflicts_with = "uri")]
    pub uri_env: String,
    #[arg(long)]
    pub database: String,
    #[arg(long)]
    pub collection: String,
    #[arg(long, default_value = "{}")]
    pub query: String,
    #[arg(long, value_parser = clap::value_parser!(usize), default_value_t = 1_000)]
    pub sample_size: usize,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub overwrite: bool,
    #[arg(long)]
    pub create_dirs: bool,
}

#[derive(Debug, Subcommand)]
pub enum CheckpointCommand {
    /// List persisted interrupted exports.
    List {
        #[arg(long)]
        directory: Option<PathBuf>,
    },
    /// Resume a supported best-effort export.
    Resume(CheckpointResumeArgs),
    /// Delete a checkpoint by id.
    Delete {
        session_id: String,
        #[arg(long)]
        directory: Option<PathBuf>,
    },
}

#[derive(Debug, Args)]
pub struct CheckpointResumeArgs {
    pub session_id: String,
    #[arg(long, conflicts_with = "uri_env")]
    pub uri: Option<String>,
    #[arg(long, default_value = "MONGODB_URI", conflicts_with = "uri")]
    pub uri_env: String,
    #[arg(long)]
    pub directory: Option<PathBuf>,
    #[arg(long)]
    pub log_format: Option<LogFormatArg>,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Create a starter config file explicitly.
    Init {
        #[arg(long)]
        path: Option<PathBuf>,
        #[arg(long)]
        force: bool,
    },
    /// List configured non-secret profiles.
    List {
        #[arg(long)]
        path: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExportFormatArg {
    Jsonl,
    Json,
    Csv,
    Parquet,
    Bson,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum CompressionArg {
    None,
    Gzip,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ConsistencyArg {
    BestEffort,
    Snapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum JsonModeArg {
    Canonical,
    Relaxed,
    Plain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogFormatArg {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ColorMode {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[allow(clippy::enum_variant_names)]
pub enum Shell {
    Bash,
    Elvish,
    Fish,
    #[value(name = "powershell", alias = "power-shell")]
    PowerShell,
    Zsh,
}

impl Commands {
    pub fn requested_log_format(&self) -> Option<LogFormatArg> {
        match self {
            Self::Export(args) => Some(args.log_format),
            Self::Checkpoint(CheckpointCommand::Resume(args)) => {
                Some(args.log_format.unwrap_or(LogFormatArg::Human))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn export_rejects_zero_limit() {
        let result = Cli::try_parse_from([
            "mongo-exporter",
            "export",
            "--database",
            "app",
            "--collection",
            "users",
            "--format",
            "jsonl",
            "--output",
            "users.jsonl",
            "--limit",
            "0",
        ]);

        assert!(result.is_err());
    }
}
