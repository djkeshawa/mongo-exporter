use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;
use mongodb::{
    bson::{Bson, Document},
    options::{ClientOptions, CountOptions, FindOptions, ReadConcern},
    Client, Collection,
};
use parquet::{
    arrow::ArrowWriter, basic::Compression as ParquetCompression,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    cli::{
        CheckpointCommand, ColorMode, Commands, CompressionArg, ConfigCommand, ConsistencyArg,
        ExportArgs, ExportFormatArg, InspectCommand, JsonModeArg, LogFormatArg, SchemaInspectArgs,
        Shell, WizardArgs,
    },
    config::{parse_field_list, parse_sort_spec, ConfigManager},
    database::{select_collection, select_database},
    export::resumable::ResumableExportManager,
    types::{CompressionType, ExportFormat},
    ui,
    utils::{
        bson_value_to_string, collect_field_names, document_to_json_value, get_bson_field,
        get_field_value, validate_uri_format,
    },
};

const REPORT_VERSION: u32 = 1;
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    JsonLines,
    JsonArray,
    Csv,
    Parquet,
    Bson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonMode {
    Canonical,
    Relaxed,
    Plain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Human,
    Json,
}

#[derive(Debug, Serialize)]
struct Event<'a> {
    schema_version: u32,
    event: &'a str,
    run_id: &'a str,
    timestamp: DateTime<Utc>,
    documents: Option<u64>,
    message: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct ExportReport {
    schema_version: u32,
    run_id: String,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    database: String,
    collection: String,
    format: String,
    json_mode: Option<String>,
    consistency: String,
    query_sha256: String,
    schema_sha256: Option<String>,
    output: Option<String>,
    output_sha256: Option<String>,
    bytes_written: u64,
    documents_expected: Option<u64>,
    documents_exported: u64,
    duration_ms: u128,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaManifest {
    pub version: u32,
    pub generated_at: DateTime<Utc>,
    pub sample_size: usize,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub path: String,
    pub kind: String,
    pub nullable: bool,
}

struct AtomicFile {
    destination: PathBuf,
    temporary: PathBuf,
    overwrite: bool,
    committed: bool,
    keep_on_drop: bool,
}

impl AtomicFile {
    fn prepare(path: &Path, overwrite: bool, create_dirs: bool) -> Result<Self> {
        if path.as_os_str().is_empty() {
            bail!("Output path cannot be empty");
        }

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        if !parent.exists() {
            if create_dirs {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create output directory: {}", parent.display())
                })?;
            } else {
                bail!(
                    "Output directory does not exist: {} (use --create-dirs explicitly)",
                    parent.display()
                );
            }
        }

        if path.symlink_metadata().is_ok() && !overwrite {
            bail!(
                "Output already exists: {} (use --overwrite to replace it)",
                path.display()
            );
        }

        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("export");
        for _ in 0..32 {
            let temporary = parent.join(format!(".{name}.{}.part", rand::random::<u64>()));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(_) => {
                    return Ok(Self {
                        destination: path.to_path_buf(),
                        temporary,
                        overwrite,
                        committed: false,
                        keep_on_drop: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("Failed to create temporary output near {}", path.display())
                    })
                }
            }
        }

        bail!("Could not allocate a unique temporary output path")
    }

    fn open(&self) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .open(&self.temporary)
            .with_context(|| {
                format!(
                    "Failed to open temporary output: {}",
                    self.temporary.display()
                )
            })
    }

    fn commit(mut self) -> Result<FileSummary> {
        let summary = file_summary(&self.temporary)?;
        atomic_replace(&self.temporary, &self.destination, self.overwrite)?;
        self.committed = true;
        Ok(summary)
    }
}

impl Drop for AtomicFile {
    fn drop(&mut self) {
        if !self.committed && !self.keep_on_drop {
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[derive(Debug)]
struct FileSummary {
    bytes: u64,
    sha256: String,
}

fn paths_share_destination(left: &Path, right: &Path) -> Result<bool> {
    let left = resolve_destination(left)?;
    let right = resolve_destination(right)?;

    #[cfg(windows)]
    {
        Ok(left
            .to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy()))
    }
    #[cfg(not(windows))]
    {
        Ok(left == right)
    }
}

fn resolve_destination(path: &Path) -> Result<PathBuf> {
    use std::path::Component;

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("Failed to resolve the current directory")?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }

    let mut ancestor = normalized.clone();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        let name = ancestor
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Could not resolve path: {}", path.display()))?;
        suffix.push(name.to_os_string());
        if !ancestor.pop() {
            bail!("Could not resolve path: {}", path.display());
        }
    }
    let mut resolved = fs::canonicalize(&ancestor)
        .with_context(|| format!("Failed to resolve path: {}", ancestor.display()))?;
    for component in suffix.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn file_summary(path: &Path) -> Result<FileSummary> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read temporary output: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes += read as u64;
    }
    Ok(FileSummary {
        bytes,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

#[allow(clippy::needless_return)]
fn atomic_replace(source: &Path, destination: &Path, overwrite: bool) -> Result<()> {
    if !overwrite {
        fs::hard_link(source, destination).with_context(|| {
            format!(
                "Destination appeared while publishing: {}",
                destination.display()
            )
        })?;
        fs::remove_file(source)?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let source_wide: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
        let destination_wide: Vec<u16> = destination
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        let result = unsafe {
            MoveFileExW(
                source_wide.as_ptr(),
                destination_wide.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            bail!(
                "Windows could not atomically replace {}",
                destination.display()
            );
        }
        return Ok(());
    }

    #[cfg(not(windows))]
    {
        fs::rename(source, destination)
            .with_context(|| format!("Failed to atomically replace {}", destination.display()))?;
        Ok(())
    }
}

enum OutputWriter {
    Plain(BufWriter<File>),
    Gzip(BufWriter<flate2::write::GzEncoder<File>>),
    Stdout(BufWriter<io::Stdout>),
}

impl Write for OutputWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(writer) => writer.write(bytes),
            Self::Gzip(writer) => writer.write(bytes),
            Self::Stdout(writer) => writer.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Gzip(writer) => writer.flush(),
            Self::Stdout(writer) => writer.flush(),
        }
    }
}

impl OutputWriter {
    fn finish(self) -> Result<()> {
        match self {
            Self::Plain(writer) => {
                let file = writer
                    .into_inner()
                    .map_err(|error| error.into_error())
                    .context("Failed to flush output file")?;
                file.sync_all().context("Failed to sync output file")?;
            }
            Self::Gzip(writer) => {
                let encoder = writer
                    .into_inner()
                    .map_err(|error| error.into_error())
                    .context("Failed to flush gzip output")?;
                let file = encoder.finish().context("Failed to finish gzip output")?;
                file.sync_all().context("Failed to sync output file")?;
            }
            Self::Stdout(mut writer) => writer.flush().context("Failed to flush stdout")?,
        }
        Ok(())
    }
}

fn output_writer(file: File, compression: CompressionArg) -> OutputWriter {
    match compression {
        CompressionArg::None => OutputWriter::Plain(BufWriter::with_capacity(256 * 1024, file)),
        CompressionArg::Gzip => OutputWriter::Gzip(BufWriter::with_capacity(
            256 * 1024,
            flate2::write::GzEncoder::new(file, flate2::Compression::default()),
        )),
    }
}

fn emit(
    log_format: LogFormat,
    run_id: &str,
    event: &str,
    documents: Option<u64>,
    message: Option<&str>,
) {
    match log_format {
        LogFormat::Human => {
            if let Some(message) = message {
                eprintln!("[{event}] {message}");
            } else if let Some(documents) = documents {
                eprintln!("[{event}] {documents} documents");
            } else {
                eprintln!("[{event}]");
            }
        }
        LogFormat::Json => {
            let event = Event {
                schema_version: REPORT_VERSION,
                event,
                run_id,
                timestamp: Utc::now(),
                documents,
                message,
            };
            if let Ok(line) = serde_json::to_string(&event) {
                eprintln!("{line}");
            }
        }
    }
}

fn fail_export<T>(log_format: LogFormat, run_id: &str, error: anyhow::Error) -> Result<T> {
    let message = error.to_string();
    emit(log_format, run_id, "export_failed", None, Some(&message));
    Err(error)
}

fn output_format(value: ExportFormatArg) -> OutputFormat {
    match value {
        ExportFormatArg::Jsonl => OutputFormat::JsonLines,
        ExportFormatArg::Json => OutputFormat::JsonArray,
        ExportFormatArg::Csv => OutputFormat::Csv,
        ExportFormatArg::Parquet => OutputFormat::Parquet,
        ExportFormatArg::Bson => OutputFormat::Bson,
    }
}

fn json_mode(value: JsonModeArg) -> JsonMode {
    match value {
        JsonModeArg::Canonical => JsonMode::Canonical,
        JsonModeArg::Relaxed => JsonMode::Relaxed,
        JsonModeArg::Plain => JsonMode::Plain,
    }
}

fn log_format(value: LogFormatArg) -> LogFormat {
    match value {
        LogFormatArg::Human => LogFormat::Human,
        LogFormatArg::Json => LogFormat::Json,
    }
}

fn format_name(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::JsonLines => "jsonl",
        OutputFormat::JsonArray => "json",
        OutputFormat::Csv => "csv",
        OutputFormat::Parquet => "parquet",
        OutputFormat::Bson => "bson",
    }
}

fn parse_query(value: &str) -> Result<Document> {
    let json: serde_json::Value = serde_json::from_str(value).context("Invalid query JSON")?;
    match json {
        serde_json::Value::Object(map) => map
            .try_into()
            .map_err(|error| anyhow::anyhow!("Invalid MongoDB Extended JSON query: {error}")),
        _ => bail!("MongoDB query must be a JSON object"),
    }
}

fn query_hash(query: &Document) -> Result<String> {
    let bytes = mongodb::bson::to_vec(query).context("Failed to hash query")?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn load_schema(path: &Path) -> Result<SchemaManifest> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read schema manifest: {}", path.display()))?;
    let manifest: SchemaManifest = serde_json::from_str(&content)
        .with_context(|| format!("Invalid schema manifest: {}", path.display()))?;
    if manifest.version != SCHEMA_VERSION {
        bail!(
            "Unsupported schema manifest version {}; expected {}",
            manifest.version,
            SCHEMA_VERSION
        );
    }
    if manifest.fields.is_empty() {
        bail!("Schema manifest must contain at least one field");
    }
    let mut paths = std::collections::HashSet::new();
    for field in &manifest.fields {
        if field.path.trim().is_empty() {
            bail!("Schema field paths cannot be empty");
        }
        if !paths.insert(field.path.as_str()) {
            bail!("Schema contains duplicate field path '{}'", field.path);
        }
        if field.kind != "string" || !field.nullable {
            bail!(
                "Schema field '{}' requests unsupported kind/nullability; v1 supports nullable string fields",
                field.path
            );
        }
    }
    Ok(manifest)
}

fn schema_hash(manifest: &SchemaManifest) -> Result<String> {
    // Generation time is provenance, not schema content. Excluding it keeps the digest stable
    // when the same field contract is regenerated on a later day.
    let canonical = serde_json::json!({
        "version": manifest.version,
        "fields": &manifest.fields,
    });
    let bytes = serde_json::to_vec(&canonical).context("Failed to hash schema manifest")?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

async fn connect(uri: &str) -> Result<Client> {
    validate_uri_format(uri).context("Invalid MongoDB URI")?;
    let mut client_options = ClientOptions::parse(uri)
        .await
        .context("Failed to parse MongoDB URI")?;
    client_options.server_selection_timeout = Some(Duration::from_secs(10));
    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    let mut last_error = None;
    for attempt in 0..3 {
        match client
            .database("admin")
            .run_command(mongodb::bson::doc! { "ping": 1 }, None)
            .await
        {
            Ok(_) => return Ok(client),
            Err(error) => {
                last_error = Some(error);
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(250 * (attempt + 1) as u64)).await;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "MongoDB connection failed after 3 attempts: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

fn resolve_uri(
    direct: Option<&str>,
    env_name: Option<&str>,
    profile: Option<&str>,
    config_path: Option<&Path>,
) -> Result<String> {
    if let Some(uri) = direct {
        if uri.is_empty() {
            bail!("--uri cannot be empty");
        }
        return Ok(uri.to_string());
    }

    let env_name = if let Some(profile_name) = profile {
        let manager = match config_path {
            Some(path) => ConfigManager::from_path(path.to_path_buf())?,
            None => ConfigManager::new()?,
        };
        let profile = manager
            .get_profile(profile_name)
            .ok_or_else(|| anyhow::anyhow!("Profile '{profile_name}' not found"))?;
        if !profile.uri.is_empty() {
            bail!("Profile '{profile_name}' contains a plaintext URI; replace it with uri_env");
        }
        profile
            .uri_env
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Profile '{profile_name}' has no uri_env"))?
            .to_string()
    } else {
        env_name.unwrap_or("MONGODB_URI").to_string()
    };

    if env_name.trim().is_empty() {
        bail!("MongoDB URI environment variable name cannot be empty");
    }

    std::env::var(&env_name).with_context(|| {
        format!("MongoDB URI not found in environment variable {env_name}; use --uri or --uri-env")
    })
}

pub async fn dispatch(
    command: Commands,
    config_path: Option<&Path>,
    color: ColorMode,
) -> Result<()> {
    match color {
        ColorMode::Auto => {}
        ColorMode::Always => console::set_colors_enabled(true),
        ColorMode::Never => console::set_colors_enabled(false),
    }

    match command {
        Commands::Export(args) => run_export(*args, config_path).await,
        Commands::Wizard(args) => run_wizard(args).await,
        Commands::Inspect(InspectCommand::Schema(args)) => run_schema_inspect(args).await,
        Commands::Checkpoint(command) => run_checkpoint(command, config_path).await,
        Commands::Config(command) => run_config(command, config_path),
        Commands::Completions { shell } => print_completions(shell),
    }
}

async fn run_export(args: ExportArgs, config_path: Option<&Path>) -> Result<()> {
    let format = output_format(args.format);
    let json_mode = json_mode(args.json_mode);
    let log_format = log_format(args.log_format);
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than zero");
    }
    if args.limit == Some(0) {
        bail!("--limit must be greater than zero");
    }
    if args.limit.is_some_and(|value| value > i64::MAX as u64) {
        bail!("--limit exceeds the MongoDB driver's supported range");
    }
    let uri = resolve_uri(
        args.uri.as_deref(),
        args.uri_env.as_deref(),
        args.profile.as_deref(),
        config_path,
    )?;
    let filter = if let Some(path) = args.query_file.as_ref() {
        parse_query(
            &fs::read_to_string(path)
                .with_context(|| format!("Failed to read query file: {}", path.display()))?,
        )?
    } else {
        parse_query(args.query.as_deref().unwrap_or("{}"))?
    };

    if args.output.as_os_str() == "-" {
        if args.overwrite || args.create_dirs {
            bail!("stdout output cannot be combined with --overwrite or --create-dirs");
        }
        if args.compression != CompressionArg::None {
            bail!("stdout output cannot be compressed");
        }
        if !matches!(
            format,
            OutputFormat::JsonLines | OutputFormat::Csv | OutputFormat::Bson
        ) {
            bail!("stdout output supports only JSONL, CSV, and BSON");
        }
        if args.checkpoint_dir.is_some() {
            bail!("stdout output cannot be checkpointed");
        }
    }

    if matches!(format, OutputFormat::Csv | OutputFormat::Parquet) && args.schema.is_none() {
        bail!("--schema is required for CSV and Parquet exports");
    }
    if format == OutputFormat::Parquet && args.output.as_os_str() == "-" {
        bail!("Parquet requires a file output");
    }
    if let Some(report) = args.report.as_ref() {
        if args.output.as_os_str() != "-" && paths_share_destination(report, &args.output)? {
            bail!("--report must use a path different from --output");
        }
    }
    if args.consistency == ConsistencyArg::Snapshot && args.count {
        bail!("--count is not available with snapshot consistency");
    }
    if args.checkpoint_dir.is_some() {
        if args.compression != CompressionArg::None
            || args.consistency == ConsistencyArg::Snapshot
            || args.skip.is_some()
            || !matches!(
                format,
                OutputFormat::JsonLines | OutputFormat::Csv | OutputFormat::Bson
            )
        {
            bail!(
                "checkpointing supports only uncompressed JSONL, CSV, and BSON in best-effort mode without --skip"
            );
        }
        let sort = args.sort.as_deref().unwrap_or("_id:1");
        if sort.replace(' ', "") != "_id:1" && sort.replace(' ', "") != "_id:asc" {
            bail!("checkpointed exports require deterministic sort _id:1");
        }
        if matches!(format, OutputFormat::JsonLines | OutputFormat::JsonArray)
            && json_mode != JsonMode::Plain
        {
            bail!(
                "resumable JSON currently requires --json-mode plain; canonical JSON resume is not yet supported"
            );
        }
        return run_resumable_export(args, config_path).await;
    }

    let schema = match args.schema.as_ref() {
        Some(path) => Some(load_schema(path)?),
        None => None,
    };
    let fields = args.fields.as_deref().map(parse_field_list);
    if fields.as_ref().is_some_and(Vec::is_empty) {
        bail!("--fields must contain at least one non-empty field name");
    }
    if matches!(format, OutputFormat::Csv | OutputFormat::Parquet) && fields.is_some() {
        bail!("CSV and Parquet fields come from --schema; do not combine --fields with --schema");
    }
    let schema_fields = schema.as_ref().map(|manifest| {
        manifest
            .fields
            .iter()
            .map(|field| field.path.clone())
            .collect::<Vec<_>>()
    });
    let query_digest = query_hash(&filter)?;
    let schema_digest = schema.as_ref().map(schema_hash).transpose()?;
    let run_id = format!("run-{:x}", rand::random::<u128>());
    let started_at = Utc::now();
    let destination = if args.output.as_os_str() == "-" {
        None
    } else {
        Some(AtomicFile::prepare(
            &args.output,
            args.overwrite,
            args.create_dirs,
        )?)
    };
    let report_destination = args
        .report
        .as_ref()
        .map(|path| AtomicFile::prepare(path, args.overwrite, args.create_dirs))
        .transpose()?;

    emit(
        log_format,
        &run_id,
        "preflight",
        None,
        Some("connecting to MongoDB"),
    );
    let client = connect(&uri).await?;
    let database = client.database(&args.database);
    let collection = database.collection::<Document>(&args.collection);
    let mut find_options = FindOptions::default();
    find_options.batch_size = Some(args.batch_size);
    find_options.no_cursor_timeout = Some(true);
    if let Some(limit) = args.limit {
        find_options.limit = Some(i64::try_from(limit).context("Invalid --limit")?);
    }
    if let Some(skip) = args.skip {
        find_options.skip = Some(skip);
    }
    if let Some(sort) = args.sort.as_deref() {
        find_options.sort = Some(parse_sort_spec(sort).context("Invalid --sort specification")?);
    } else if args.checkpoint_dir.is_some() {
        find_options.sort = Some(parse_sort_spec("_id:1")?);
    }
    let projection_fields = fields.clone().or_else(|| schema_fields.clone());
    if let Some(fields) = projection_fields.as_ref() {
        let mut projection = Document::new();
        for field in fields {
            projection.insert(field, 1);
        }
        find_options.projection = Some(projection);
    }
    if args.consistency == ConsistencyArg::Snapshot {
        find_options.read_concern = Some(ReadConcern::snapshot());
        emit(
            log_format,
            &run_id,
            "consistency",
            None,
            Some("snapshot read concern requested; unsupported servers fail closed"),
        );
    }

    let expected_count = if args.count {
        let mut count_options = CountOptions::default();
        count_options.limit = args.limit;
        count_options.skip = args.skip;
        let count = collection
            .count_documents(filter.clone(), Some(count_options))
            .await
            .context("Failed to count matching documents")?;
        emit(
            log_format,
            &run_id,
            "count",
            Some(count),
            Some("exact count complete"),
        );
        Some(count)
    } else {
        None
    };

    emit(
        log_format,
        &run_id,
        "export_started",
        expected_count,
        Some("streaming documents"),
    );

    let (summary, documents_exported) = if format == OutputFormat::Parquet {
        let atomic = match destination {
            Some(atomic) => atomic,
            None => {
                return fail_export(
                    log_format,
                    &run_id,
                    anyhow::anyhow!("Parquet requires a file output"),
                )
            }
        };
        let file = match atomic.open() {
            Ok(file) => file,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        let documents = match write_parquet(
            &collection,
            &filter,
            &find_options,
            file,
            schema.as_ref().expect("schema validated before export"),
            args.compression,
        )
        .await
        {
            Ok(documents) => documents,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        let summary = match atomic.commit() {
            Ok(summary) => summary,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        (summary, documents)
    } else if let Some(atomic) = destination {
        let file = match atomic.open() {
            Ok(file) => file,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        let mut writer = output_writer(file, args.compression);
        let documents = match write_stream(
            &collection,
            &filter,
            &find_options,
            &mut writer,
            format,
            json_mode,
            schema_fields.as_deref().or(fields.as_deref()),
            log_format,
            &run_id,
            args.no_progress,
        )
        .await
        {
            Ok(documents) => documents,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        if let Err(error) = writer.finish() {
            return fail_export(log_format, &run_id, error);
        }
        let summary = match atomic.commit() {
            Ok(summary) => summary,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        (summary, documents)
    } else {
        let mut writer = OutputWriter::Stdout(BufWriter::new(io::stdout()));
        let documents = match write_stream(
            &collection,
            &filter,
            &find_options,
            &mut writer,
            format,
            json_mode,
            schema_fields.as_deref().or(fields.as_deref()),
            log_format,
            &run_id,
            args.no_progress,
        )
        .await
        {
            Ok(documents) => documents,
            Err(error) => return fail_export(log_format, &run_id, error),
        };
        if let Err(error) = writer.finish() {
            return fail_export(log_format, &run_id, error);
        }
        (
            FileSummary {
                bytes: 0,
                sha256: String::new(),
            },
            documents,
        )
    };

    let finished_at = Utc::now();
    let report = ExportReport {
        schema_version: REPORT_VERSION,
        run_id: run_id.clone(),
        started_at,
        finished_at,
        database: args.database,
        collection: args.collection,
        format: format_name(format).to_string(),
        json_mode: if matches!(format, OutputFormat::JsonLines | OutputFormat::JsonArray) {
            Some(
                match json_mode {
                    JsonMode::Canonical => "canonical",
                    JsonMode::Relaxed => "relaxed",
                    JsonMode::Plain => "plain",
                }
                .to_string(),
            )
        } else {
            None
        },
        consistency: match args.consistency {
            ConsistencyArg::BestEffort => "best-effort".to_string(),
            ConsistencyArg::Snapshot => "snapshot".to_string(),
        },
        query_sha256: query_digest,
        schema_sha256: schema_digest,
        output: if args.output.as_os_str() == "-" {
            None
        } else {
            Some(args.output.display().to_string())
        },
        output_sha256: if summary.sha256.is_empty() {
            None
        } else {
            Some(summary.sha256.clone())
        },
        bytes_written: summary.bytes,
        documents_expected: expected_count,
        documents_exported,
        duration_ms: (finished_at - started_at)
            .to_std()
            .unwrap_or_default()
            .as_millis(),
        warnings: {
            let mut warnings = Vec::new();
            if expected_count.is_none() {
                warnings.push(
                    "exact count was not requested; progress may be indeterminate".to_string(),
                );
            }
            if args.consistency == ConsistencyArg::BestEffort {
                warnings.push(
                    "best-effort consistency allows source mutations during export".to_string(),
                );
            }
            warnings
        },
    };

    if let Some(atomic) = report_destination {
        write_report(atomic, &report)?;
    }
    emit(
        log_format,
        &run_id,
        "export_completed",
        Some(documents_exported),
        Some("export published successfully"),
    );
    Ok(())
}

async fn run_resumable_export(args: ExportArgs, config_path: Option<&Path>) -> Result<()> {
    let format = output_format(args.format);
    let log_format = log_format(args.log_format);
    let uri = resolve_uri(
        args.uri.as_deref(),
        args.uri_env.as_deref(),
        args.profile.as_deref(),
        config_path,
    )?;
    let filter = if let Some(path) = args.query_file.as_ref() {
        parse_query(
            &fs::read_to_string(path)
                .with_context(|| format!("Failed to read query file: {}", path.display()))?,
        )?
    } else {
        parse_query(args.query.as_deref().unwrap_or("{}"))?
    };
    let schema = args
        .schema
        .as_ref()
        .map(|path| load_schema(path))
        .transpose()?;
    let fields = args.fields.as_deref().map(parse_field_list);
    if fields.as_ref().is_some_and(Vec::is_empty) {
        bail!("--fields must contain at least one non-empty field name");
    }
    if matches!(format, OutputFormat::Csv | OutputFormat::Parquet) && fields.is_some() {
        bail!("CSV and Parquet fields come from --schema; do not combine --fields with --schema");
    }
    let schema_fields = schema.as_ref().map(|manifest| {
        manifest
            .fields
            .iter()
            .map(|field| field.path.clone())
            .collect::<Vec<_>>()
    });
    let export_format = match format {
        OutputFormat::JsonLines => ExportFormat::JsonLines,
        OutputFormat::JsonArray => ExportFormat::JsonArray,
        OutputFormat::Csv => ExportFormat::Csv,
        OutputFormat::Parquet => ExportFormat::Parquet,
        OutputFormat::Bson => ExportFormat::Bson,
    };
    if args.output.as_os_str() == "-" {
        bail!("checkpointed exports require a file output");
    }
    let parent = args.output.parent().unwrap_or_else(|| Path::new("."));
    if !parent.exists() {
        if args.create_dirs {
            fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create output directory: {}", parent.display())
            })?;
        } else {
            bail!(
                "Output directory does not exist: {} (use --create-dirs explicitly)",
                parent.display()
            );
        }
    }
    if args.output.exists() && !args.overwrite {
        bail!(
            "Output already exists: {} (use --overwrite for a new checkpointed export)",
            args.output.display()
        );
    }
    let report_destination = args
        .report
        .as_ref()
        .map(|path| AtomicFile::prepare(path, args.overwrite, args.create_dirs))
        .transpose()?;

    let run_id = format!("run-{:x}", rand::random::<u128>());
    let started_at = Utc::now();
    let query_digest = query_hash(&filter)?;
    let schema_digest = schema.as_ref().map(schema_hash).transpose()?;
    emit(
        log_format,
        &run_id,
        "preflight",
        None,
        Some("connecting to MongoDB"),
    );
    let client = connect(&uri).await?;
    let collection = client
        .database(&args.database)
        .collection::<Document>(&args.collection);
    let expected_count = if args.count {
        let mut count_options = CountOptions::default();
        count_options.limit = args.limit;
        count_options.skip = args.skip;
        let count = collection
            .count_documents(filter.clone(), Some(count_options))
            .await
            .context("Failed to count matching documents")?;
        emit(
            log_format,
            &run_id,
            "count",
            Some(count),
            Some("exact count complete"),
        );
        Some(count)
    } else {
        None
    };

    let performance = crate::config::PerformanceConfig {
        enable_parallel_processing: false,
        document_batch_size: args.batch_size as usize,
        ..Default::default()
    };
    let exporter = crate::export::enterprise::enhanced::EnhancedEnterpriseExporter::new_silent(
        performance,
        args.checkpoint_dir.clone(),
    )?;
    let options = crate::export::enterprise::export::ExportOptions {
        fields: schema_fields.or(fields),
        limit: args.limit,
        skip: args.skip,
        sort: Some(parse_sort_spec("_id:1")?),
        validate_fields: true,
        collect_stats: true,
    };
    emit(
        log_format,
        &run_id,
        "export_started",
        expected_count,
        Some("checkpointed export started"),
    );
    let stats = match exporter
        .export_with_enterprise_features(
            &collection,
            &filter,
            &args.output.to_string_lossy(),
            &export_format,
            &CompressionType::None,
            &options,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            None,
            &uri,
        )
        .await
    {
        Ok(stats) => stats,
        Err(error) => return fail_export(log_format, &run_id, error),
    };
    let finished_at = Utc::now();
    let summary = file_summary(&args.output)?;
    let report = ExportReport {
        schema_version: REPORT_VERSION,
        run_id: run_id.clone(),
        started_at,
        finished_at,
        database: args.database,
        collection: args.collection,
        format: format_name(format).to_string(),
        json_mode: if matches!(format, OutputFormat::JsonLines | OutputFormat::JsonArray) {
            Some("plain".to_string())
        } else {
            None
        },
        consistency: "best-effort".to_string(),
        query_sha256: query_digest,
        schema_sha256: schema_digest,
        output: Some(args.output.display().to_string()),
        output_sha256: Some(summary.sha256),
        bytes_written: summary.bytes,
        documents_expected: expected_count,
        documents_exported: stats.documents_exported,
        duration_ms: (finished_at - started_at)
            .to_std()
            .unwrap_or_default()
            .as_millis(),
        warnings: vec![
            "checkpointed output is appendable and is not published atomically until a future v1 revision".to_string(),
            "resumable JSON uses plain JSON representation".to_string(),
        ],
    };
    if let Some(atomic) = report_destination {
        write_report(atomic, &report)?;
    }
    emit(
        log_format,
        &run_id,
        "export_completed",
        Some(stats.documents_exported),
        Some("checkpointed export completed"),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_stream(
    collection: &Collection<Document>,
    filter: &Document,
    find_options: &FindOptions,
    writer: &mut OutputWriter,
    format: OutputFormat,
    json_mode: JsonMode,
    schema_fields: Option<&[String]>,
    log_format: LogFormat,
    run_id: &str,
    no_progress: bool,
) -> Result<u64> {
    if format == OutputFormat::Parquet {
        bail!("Parquet output must use the columnar writer");
    }

    let mut cursor = collection
        .find(filter.clone(), find_options.clone())
        .await
        .context("Failed to execute MongoDB query")?;
    let mut count = 0u64;

    match format {
        OutputFormat::JsonLines => {
            while let Some(result) = cursor.next().await {
                let document = result.context("Failed to read MongoDB document")?;
                let value = match json_mode {
                    JsonMode::Canonical => {
                        Bson::Document(document.clone()).into_canonical_extjson()
                    }
                    JsonMode::Relaxed => Bson::Document(document.clone()).into_relaxed_extjson(),
                    JsonMode::Plain => document_to_json_value(&document),
                };
                serde_json::to_writer(&mut *writer, &value)
                    .context("Failed to serialize document as JSON")?;
                writer
                    .write_all(b"\n")
                    .context("Failed to write JSONL record")?;
                count += 1;
                emit_progress(log_format, run_id, count, no_progress);
            }
        }
        OutputFormat::JsonArray => {
            writer
                .write_all(b"[\n")
                .context("Failed to write JSON array header")?;
            let mut first = true;
            while let Some(result) = cursor.next().await {
                let document = result.context("Failed to read MongoDB document")?;
                if !first {
                    writer
                        .write_all(b",\n")
                        .context("Failed to write JSON array separator")?;
                }
                first = false;
                let value = match json_mode {
                    JsonMode::Canonical => {
                        Bson::Document(document.clone()).into_canonical_extjson()
                    }
                    JsonMode::Relaxed => Bson::Document(document.clone()).into_relaxed_extjson(),
                    JsonMode::Plain => document_to_json_value(&document),
                };
                serde_json::to_writer(&mut *writer, &value)
                    .context("Failed to serialize document as JSON")?;
                count += 1;
                emit_progress(log_format, run_id, count, no_progress);
            }
            writer
                .write_all(b"\n]\n")
                .context("Failed to write JSON array footer")?;
        }
        OutputFormat::Csv => {
            let fields = schema_fields.ok_or_else(|| anyhow::anyhow!("CSV requires a schema"))?;
            if fields.is_empty() {
                bail!("CSV schema must contain at least one field");
            }
            let mut csv_writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(&mut *writer);
            csv_writer
                .write_record(fields)
                .context("Failed to write CSV header")?;
            while let Some(result) = cursor.next().await {
                let document = result.context("Failed to read MongoDB document")?;
                let values = fields
                    .iter()
                    .map(|field| get_field_value(&document, field))
                    .collect::<Vec<_>>();
                csv_writer
                    .write_record(values)
                    .context("Failed to write CSV record")?;
                count += 1;
                emit_progress(log_format, run_id, count, no_progress);
            }
            csv_writer.flush().context("Failed to flush CSV output")?;
        }
        OutputFormat::Bson => {
            while let Some(result) = cursor.next().await {
                let document = result.context("Failed to read MongoDB document")?;
                let bytes = mongodb::bson::to_vec(&document)
                    .context("Failed to serialize BSON document")?;
                writer
                    .write_all(&bytes)
                    .context("Failed to write BSON record")?;
                count += 1;
                emit_progress(log_format, run_id, count, no_progress);
            }
        }
        OutputFormat::Parquet => unreachable!(),
    }

    Ok(count)
}

fn emit_progress(log_format: LogFormat, run_id: &str, count: u64, no_progress: bool) {
    if !no_progress && count % 1_000 == 0 {
        emit(log_format, run_id, "progress", Some(count), None);
    }
}

fn parquet_string_value(document: &Document, field_path: &str) -> Option<String> {
    match get_bson_field(document, field_path) {
        None | Some(Bson::Null | Bson::Undefined) => None,
        Some(value) => Some(bson_value_to_string(value)),
    }
}

async fn write_parquet(
    collection: &Collection<Document>,
    filter: &Document,
    find_options: &FindOptions,
    file: File,
    schema: &SchemaManifest,
    compression: CompressionArg,
) -> Result<u64> {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let field_names = schema
        .fields
        .iter()
        .map(|field| field.path.clone())
        .collect::<Vec<_>>();
    if field_names.is_empty() {
        bail!("Parquet schema must contain at least one field");
    }

    let arrow_schema = Arc::new(Schema::new(
        field_names
            .iter()
            .map(|field| Field::new(field, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let properties = WriterProperties::builder()
        .set_compression(match compression {
            CompressionArg::None => ParquetCompression::UNCOMPRESSED,
            CompressionArg::Gzip => ParquetCompression::GZIP(Default::default()),
        })
        .build();
    let mut parquet_writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(properties))
        .context("Failed to create Parquet writer")?;
    let mut cursor = collection
        .find(filter.clone(), find_options.clone())
        .await
        .context("Failed to execute MongoDB query")?;
    let batch_size = find_options.batch_size.unwrap_or(1_000).max(1) as usize;
    let mut values = vec![Vec::<Option<String>>::with_capacity(batch_size); field_names.len()];
    let mut count = 0u64;

    while let Some(result) = cursor.next().await {
        let document = result.context("Failed to read MongoDB document")?;
        for (index, field) in field_names.iter().enumerate() {
            values[index].push(parquet_string_value(&document, field));
        }
        count += 1;
        if values[0].len() >= batch_size {
            write_parquet_batch(&mut parquet_writer, &arrow_schema, &values)?;
            for column in &mut values {
                column.clear();
            }
        }
    }

    if !values[0].is_empty() {
        write_parquet_batch(&mut parquet_writer, &arrow_schema, &values)?;
    }
    parquet_writer
        .close()
        .context("Failed to finalize Parquet output")?;
    Ok(count)
}

fn write_parquet_batch(
    writer: &mut ArrowWriter<File>,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
    values: &[Vec<Option<String>>],
) -> Result<()> {
    use arrow::array::{ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;

    let columns = values
        .iter()
        .map(|column| std::sync::Arc::new(StringArray::from(column.clone())) as ArrayRef)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .context("Failed to construct Parquet record batch")?;
    writer
        .write(&batch)
        .context("Failed to write Parquet record batch")?;
    Ok(())
}

fn write_report(atomic: AtomicFile, report: &ExportReport) -> Result<()> {
    let mut writer = BufWriter::new(atomic.open()?);
    serde_json::to_writer_pretty(&mut writer, report)
        .context("Failed to serialize export report")?;
    writer.flush().context("Failed to flush export report")?;
    writer
        .get_ref()
        .sync_all()
        .context("Failed to sync export report")?;
    atomic.commit()?;
    Ok(())
}

async fn infer_schema(
    collection: &Collection<Document>,
    filter: &Document,
    sample_size: usize,
) -> Result<SchemaManifest> {
    if sample_size == 0 {
        bail!("--sample-size must be greater than zero");
    }
    let mut options = FindOptions::default();
    options.limit = Some(sample_size.min(i64::MAX as usize) as i64);
    options.batch_size = Some(sample_size.min(u32::MAX as usize) as u32);
    let mut cursor = collection
        .find(filter.clone(), options)
        .await
        .context("Failed to sample MongoDB documents")?;
    let mut fields = std::collections::HashSet::new();
    let mut sampled = 0usize;
    while let Some(result) = cursor.next().await {
        let document = result.context("Failed to read sampled MongoDB document")?;
        collect_field_names(&document, "", &mut fields);
        sampled += 1;
    }
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    fields.sort();
    if fields.is_empty() {
        bail!("No scalar fields were found in the sampled documents");
    }
    Ok(SchemaManifest {
        version: SCHEMA_VERSION,
        generated_at: Utc::now(),
        sample_size: sampled,
        fields: fields
            .into_iter()
            .map(|path| SchemaField {
                path,
                kind: "string".to_string(),
                nullable: true,
            })
            .collect(),
    })
}

fn write_schema_manifest(
    path: &Path,
    manifest: &SchemaManifest,
    overwrite: bool,
    create_dirs: bool,
) -> Result<()> {
    let atomic = AtomicFile::prepare(path, overwrite, create_dirs)?;
    let mut writer = BufWriter::new(atomic.open()?);
    serde_json::to_writer_pretty(&mut writer, manifest)
        .context("Failed to serialize schema manifest")?;
    writer.flush().context("Failed to flush schema manifest")?;
    writer
        .get_ref()
        .sync_all()
        .context("Failed to sync schema manifest")?;
    atomic.commit()?;
    Ok(())
}

async fn run_schema_inspect(args: SchemaInspectArgs) -> Result<()> {
    let uri = resolve_uri(args.uri.as_deref(), Some(&args.uri_env), None, None)?;
    let filter = parse_query(&args.query)?;
    let client = connect(&uri).await?;
    let collection = client
        .database(&args.database)
        .collection::<Document>(&args.collection);
    let manifest = infer_schema(&collection, &filter, args.sample_size).await?;
    write_schema_manifest(&args.output, &manifest, args.overwrite, args.create_dirs)?;
    eprintln!(
        "schema manifest written: {} ({} fields, {} sampled documents)",
        args.output.display(),
        manifest.fields.len(),
        manifest.sample_size
    );
    Ok(())
}

async fn run_wizard(args: WizardArgs) -> Result<()> {
    ui::show_banner();
    let uri = args.uri.unwrap_or(ui::get_connection_uri()?);
    let client = connect(&uri).await?;
    let database = select_database(&client).await?;
    let collection = select_collection(&database).await?;
    let format = ui::get_export_format()?;
    let compression_type = ui::get_compression_type()?;
    let output = PathBuf::from(ui::get_output_path(&format, &compression_type)?);
    let format_arg = match format {
        ExportFormat::JsonLines => ExportFormatArg::Jsonl,
        ExportFormat::JsonArray => ExportFormatArg::Json,
        ExportFormat::Csv => ExportFormatArg::Csv,
        ExportFormat::Parquet => ExportFormatArg::Parquet,
        ExportFormat::Bson => ExportFormatArg::Bson,
    };
    let compression_arg = match compression_type {
        CompressionType::None => CompressionArg::None,
        CompressionType::Gzip => CompressionArg::Gzip,
    };
    let schema_path = if matches!(format, ExportFormat::Csv | ExportFormat::Parquet) {
        let manifest = infer_schema(&collection, &Document::new(), 1_000).await?;
        let path = output.with_extension("schema.json");
        write_schema_manifest(&path, &manifest, false, false)?;
        eprintln!("schema manifest written: {}", path.display());
        Some(path)
    } else {
        None
    };
    let result = run_export(
        ExportArgs {
            uri: Some(uri),
            uri_env: None,
            profile: None,
            database: database.name().to_string(),
            collection: collection.name().to_string(),
            query: Some("{}".to_string()),
            query_file: None,
            fields: None,
            limit: None,
            skip: None,
            sort: None,
            format: format_arg,
            output,
            overwrite: false,
            create_dirs: false,
            compression: compression_arg,
            consistency: ConsistencyArg::BestEffort,
            json_mode: JsonModeArg::Canonical,
            schema: schema_path.clone(),
            report: None,
            log_format: LogFormatArg::Human,
            count: false,
            checkpoint_dir: None,
            batch_size: 1_000,
            no_progress: false,
        },
        None,
    )
    .await;
    if let Some(path) = schema_path {
        let _ = fs::remove_file(path);
    }
    result
}

fn run_config(command: ConfigCommand, config_path: Option<&Path>) -> Result<()> {
    match command {
        ConfigCommand::Init { path, force } => {
            let path = path.or_else(|| config_path.map(Path::to_path_buf));
            let created = ConfigManager::init(path, force)?;
            eprintln!("configuration initialized: {}", created.display());
            Ok(())
        }
        ConfigCommand::List { path } => {
            let path = path.or_else(|| config_path.map(Path::to_path_buf));
            let manager = match path {
                Some(path) => ConfigManager::from_path(path.clone())?,
                None => ConfigManager::new()?,
            };
            for (name, profile) in manager.list_profiles() {
                println!(
                    "{name}\t{}\t{}",
                    profile.description.as_deref().unwrap_or(""),
                    profile.uri_env.as_deref().unwrap_or("")
                );
            }
            Ok(())
        }
    }
}

async fn run_checkpoint(command: CheckpointCommand, config_path: Option<&Path>) -> Result<()> {
    match command {
        CheckpointCommand::List { directory } => {
            let manager = ResumableExportManager::new(directory)?;
            for session in manager.list_sessions()? {
                println!(
                    "{}\t{}\t{}\t{}",
                    session.session_id,
                    session.config.database,
                    session.config.collection,
                    session.progress.documents_exported
                );
            }
            Ok(())
        }
        CheckpointCommand::Delete {
            session_id,
            directory,
        } => {
            let manager = ResumableExportManager::new(directory)?;
            manager.delete_session(&session_id)
        }
        CheckpointCommand::Resume(args) => {
            let event_format = log_format(args.log_format.unwrap_or(LogFormatArg::Human));
            let run_id = format!("checkpoint-{}", args.session_id);
            let manager = ResumableExportManager::new(args.directory.clone())?;
            let checkpoint = manager
                .load_session(&args.session_id)?
                .ok_or_else(|| anyhow::anyhow!("Checkpoint '{}' not found", args.session_id))?;
            if !matches!(
                checkpoint.config.format,
                ExportFormat::JsonLines | ExportFormat::Csv | ExportFormat::Bson
            ) || !matches!(checkpoint.config.compression, CompressionType::None)
            {
                bail!("Only uncompressed JSONL, CSV, and BSON checkpoints can be resumed");
            }
            let uri = resolve_uri(args.uri.as_deref(), Some(&args.uri_env), None, config_path)?;
            emit(
                event_format,
                &run_id,
                "checkpoint_resume_started",
                Some(checkpoint.progress.documents_exported),
                Some("connecting to MongoDB"),
            );
            let client = connect(&uri).await?;
            let collection = client
                .database(&checkpoint.config.database)
                .collection::<Document>(&checkpoint.config.collection);
            let exporter =
                crate::export::enterprise::enhanced::EnhancedEnterpriseExporter::new_silent(
                    crate::config::PerformanceConfig::default(),
                    args.directory,
                )?;
            let options = crate::export::enterprise::export::ExportOptions {
                fields: checkpoint.config.fields.clone(),
                limit: checkpoint.config.limit,
                skip: checkpoint.config.skip,
                sort: checkpoint.config.sort.clone(),
                collect_stats: true,
                validate_fields: true,
            };
            let exported = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let stats = match exporter
                .export_with_enterprise_features(
                    &collection,
                    &checkpoint.config.filter,
                    &checkpoint.config.output_path,
                    &checkpoint.config.format,
                    &checkpoint.config.compression,
                    &options,
                    exported,
                    Some(args.session_id.clone()),
                    &uri,
                )
                .await
            {
                Ok(stats) => stats,
                Err(error) => return fail_export(event_format, &run_id, error),
            };
            emit(
                event_format,
                &run_id,
                "checkpoint_resume_completed",
                Some(stats.documents_exported),
                Some("checkpoint resumed successfully"),
            );
            Ok(())
        }
    }
}

fn print_completions(shell: Shell) -> Result<()> {
    use clap::CommandFactory;
    use clap_complete::{generate, shells};
    let mut command = crate::cli::Cli::command();
    let name = command.get_name().to_string();
    match shell {
        Shell::Bash => generate(shells::Bash, &mut command, name, &mut io::stdout()),
        Shell::Elvish => generate(shells::Elvish, &mut command, name, &mut io::stdout()),
        Shell::Fish => generate(shells::Fish, &mut command, name, &mut io::stdout()),
        Shell::PowerShell => generate(shells::PowerShell, &mut command, name, &mut io::stdout()),
        Shell::Zsh => generate(shells::Zsh, &mut command, name, &mut io::stdout()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, oid::ObjectId};
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn parses_extended_json_query_types() {
        let query = parse_query(r#"{"_id":{"$oid":"507f1f77bcf86cd799439011"}}"#)
            .expect("query should parse");
        assert_eq!(
            query.get_object_id("_id").unwrap(),
            ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap()
        );
    }

    #[test]
    fn rejects_non_object_queries() {
        assert!(parse_query("[]").is_err());
    }

    #[test]
    fn schema_digest_ignores_generation_timestamp() {
        let fields = vec![SchemaField {
            path: "name".to_string(),
            kind: "string".to_string(),
            nullable: true,
        }];
        let first = SchemaManifest {
            version: SCHEMA_VERSION,
            generated_at: Utc::now(),
            sample_size: 1,
            fields: fields.clone(),
        };
        let second = SchemaManifest {
            version: SCHEMA_VERSION,
            generated_at: Utc::now() + chrono::Duration::days(1),
            sample_size: 999,
            fields,
        };
        assert_eq!(schema_hash(&first).unwrap(), schema_hash(&second).unwrap());
    }

    #[test]
    fn atomic_output_refuses_overwrite_without_flag() {
        let directory = tempdir().unwrap();
        let destination = directory.path().join("export.jsonl");
        let atomic = AtomicFile::prepare(&destination, false, false).unwrap();
        let mut file = atomic.open().unwrap();
        file.write_all(b"first\n").unwrap();
        file.sync_all().unwrap();
        atomic.commit().unwrap();
        assert_eq!(fs::read_to_string(&destination).unwrap(), "first\n");
        assert!(AtomicFile::prepare(&destination, false, false).is_err());
    }

    #[test]
    fn atomic_output_replaces_with_explicit_flag() {
        let directory = tempdir().unwrap();
        let destination = directory.path().join("export.jsonl");
        fs::write(&destination, b"old\n").unwrap();
        let atomic = AtomicFile::prepare(&destination, true, false).unwrap();
        let mut file = atomic.open().unwrap();
        file.write_all(b"new\n").unwrap();
        file.sync_all().unwrap();
        atomic.commit().unwrap();
        let mut content = String::new();
        File::open(destination)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert_eq!(content, "new\n");
    }

    #[test]
    fn report_and_output_aliases_are_detected_before_writing() {
        let directory = tempdir().unwrap();
        let destination = directory.path().join("export.jsonl");
        let alias = directory
            .path()
            .join("missing-directory")
            .join("..")
            .join("export.jsonl");

        assert!(paths_share_destination(&destination, &alias).unwrap());
    }

    #[cfg(windows)]
    #[test]
    fn report_and_output_aliases_are_case_insensitive_on_windows() {
        let directory = tempdir().unwrap();
        let upper = directory.path().join("EXPORT.JSONL");
        let lower = directory.path().join("export.jsonl");

        assert!(paths_share_destination(&upper, &lower).unwrap());
    }

    #[test]
    fn parquet_preserves_empty_strings_and_nulls_missing_values() {
        let document = doc! {
            "empty": "",
            "null": Bson::Null,
            "nested": { "empty": "" },
        };

        assert_eq!(
            parquet_string_value(&document, "empty"),
            Some(String::new())
        );
        assert_eq!(parquet_string_value(&document, "null"), None);
        assert_eq!(parquet_string_value(&document, "missing"), None);
        assert_eq!(
            parquet_string_value(&document, "nested.empty"),
            Some(String::new())
        );
    }
}
