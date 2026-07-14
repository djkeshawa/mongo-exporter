#![allow(dead_code)]

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use crate::types::{CompressionType, ExportFormat};

const CHECKPOINT_VERSION: u32 = 1;
const CHECKPOINT_FILE_PREFIX: &str = "mongo-exporter-";

/// Export checkpoint containing state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCheckpoint {
    /// Storage schema marker used to distinguish exporter-owned state from unrelated JSON.
    #[serde(default)]
    pub checkpoint_version: u32,
    /// Unique export session ID
    pub session_id: String,
    /// Export start time
    pub started_at: DateTime<Utc>,
    /// Last checkpoint time
    pub last_checkpoint: DateTime<Utc>,
    /// Export configuration
    pub config: ExportConfig,
    /// Current progress state
    pub progress: ExportProgress,
    /// Resume cursor information
    pub cursor_state: CursorState,
    /// Export statistics
    pub stats: CheckpointStats,
    /// Error information if any
    pub last_error: Option<String>,
    /// Number of retry attempts
    pub retry_count: u32,
}

/// Export configuration for resumable exports
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    /// MongoDB connection URI. Held in memory only — `#[serde(skip)]` ensures it never
    /// hits disk, since checkpoints would otherwise persist plaintext credentials in a
    /// world-readable cache directory. On resume, the caller must repopulate this from
    /// `--uri` / env before reconnecting.
    #[serde(skip, default)]
    pub uri: String,
    pub database: String,
    pub collection: String,
    pub filter: Document,
    pub format: ExportFormat,
    pub compression: CompressionType,
    pub output_path: String,
    pub fields: Option<Vec<String>>,
    pub sort: Option<Document>,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
}

/// Current export progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportProgress {
    /// Number of documents processed
    pub documents_processed: u64,
    /// Number of documents successfully exported
    pub documents_exported: u64,
    /// Number of documents skipped due to errors
    pub documents_failed: u64,
    /// Bytes written to output file
    pub bytes_written: u64,
    /// Percentage complete (0-100)
    pub percentage_complete: f64,
    /// Estimated time remaining in seconds
    pub eta_seconds: Option<u64>,
}

/// MongoDB cursor state for resuming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorState {
    /// Last processed document ID. MongoDB permits any BSON value as `_id`.
    pub last_id: Option<Bson>,
    /// Last processed document sort key
    pub last_sort_key: Option<Document>,
    /// Batch size being used
    pub batch_size: u32,
    /// Whether we're using a tailable cursor
    pub is_tailable: bool,
}

/// Statistics at checkpoint time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStats {
    /// Processing rate (docs/second)
    pub processing_rate: f64,
    /// Average document size in bytes
    pub avg_document_size: f64,
    /// Peak memory usage in bytes
    pub peak_memory_bytes: u64,
    /// Number of errors encountered
    pub error_count: u32,
    /// Last successful batch timestamp
    pub last_successful_batch: DateTime<Utc>,
}

impl Default for ExportProgress {
    fn default() -> Self {
        Self {
            documents_processed: 0,
            documents_exported: 0,
            documents_failed: 0,
            bytes_written: 0,
            percentage_complete: 0.0,
            eta_seconds: None,
        }
    }
}

impl Default for CursorState {
    fn default() -> Self {
        Self {
            last_id: None,
            last_sort_key: None,
            batch_size: 1000,
            is_tailable: false,
        }
    }
}

impl Default for CheckpointStats {
    fn default() -> Self {
        Self {
            processing_rate: 0.0,
            avg_document_size: 0.0,
            peak_memory_bytes: 0,
            error_count: 0,
            last_successful_batch: Utc::now(),
        }
    }
}

/// Reject session IDs that could be used to escape the checkpoint directory or otherwise
/// confuse the filesystem layer. Mirrors the alphabet used by `generate_session_id`.
fn validate_session_id(session_id: &str) -> Result<()> {
    if session_id.is_empty() {
        anyhow::bail!("Session ID cannot be empty");
    }
    if !session_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!(
            "Invalid session ID '{}': only alphanumerics, '-', and '_' are allowed",
            session_id
        );
    }
    Ok(())
}

fn write_checkpoint_atomically(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("checkpoint.json");

    for _ in 0..32 {
        let temporary = parent.join(format!(".{file_name}.{:x}.tmp", rand::random::<u64>()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut file = match options.open(&temporary) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "Failed to create temporary checkpoint near {}",
                        path.display()
                    )
                })
            }
        };

        let result = (|| -> Result<()> {
            file.write_all(content)
                .context("Failed to write checkpoint contents")?;
            file.sync_all()
                .context("Failed to sync checkpoint contents")?;
            drop(file);
            replace_checkpoint_file(&temporary, path)?;
            #[cfg(unix)]
            if let Ok(directory) = fs::File::open(parent) {
                let _ = directory.sync_all();
            }
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        return result.with_context(|| format!("Failed to publish checkpoint: {}", path.display()));
    }

    anyhow::bail!("Could not allocate a temporary checkpoint file")
}

#[allow(clippy::needless_return)]
fn replace_checkpoint_file(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let source_wide = source
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let destination_wide = destination
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source_wide.as_ptr(),
                destination_wide.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("Windows could not replace {}", destination.display()));
        }
        return Ok(());
    }

    #[cfg(not(windows))]
    {
        fs::rename(source, destination)
            .with_context(|| format!("Failed to replace {}", destination.display()))?;
        Ok(())
    }
}

fn is_managed_checkpoint_file(path: &Path) -> bool {
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return false;
    };
    if path.extension().and_then(|value| value.to_str()) != Some("json")
        || !stem.starts_with(CHECKPOINT_FILE_PREFIX)
        || validate_session_id(stem).is_err()
    {
        return false;
    }

    fs::read_to_string(path)
        .ok()
        .and_then(|content| serde_json::from_str::<ExportCheckpoint>(&content).ok())
        .is_some_and(|checkpoint| {
            checkpoint.checkpoint_version == CHECKPOINT_VERSION && checkpoint.session_id == stem
        })
}

fn combine_filters(base: Document, resume_condition: Document) -> Document {
    if base.is_empty() {
        resume_condition
    } else {
        doc! { "$and": [base, resume_condition] }
    }
}

/// Manager for resumable exports
pub struct ResumableExportManager {
    checkpoint_dir: PathBuf,
    checkpoint_interval: Duration,
    max_checkpoints: usize,
}

impl ResumableExportManager {
    /// Create a new resumable export manager
    pub fn new(checkpoint_dir: Option<PathBuf>) -> Result<Self> {
        let checkpoint_dir = checkpoint_dir.unwrap_or_else(|| {
            dirs::cache_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("mongo-exporter")
                .join("checkpoints")
        });

        // Create checkpoint directory if it doesn't exist
        fs::create_dir_all(&checkpoint_dir).with_context(|| {
            format!(
                "Failed to create checkpoint directory: {}",
                checkpoint_dir.display()
            )
        })?;

        Ok(Self {
            checkpoint_dir,
            checkpoint_interval: Duration::from_secs(30), // Checkpoint every 30 seconds
            max_checkpoints: 10,                          // Keep last 10 checkpoints
        })
    }

    /// Create a new export session
    pub fn create_session(&self, config: ExportConfig) -> Result<ExportCheckpoint> {
        let session_id = self.generate_session_id(&config);
        let now = Utc::now();

        let checkpoint = ExportCheckpoint {
            checkpoint_version: CHECKPOINT_VERSION,
            session_id: session_id.clone(),
            started_at: now,
            last_checkpoint: now,
            config,
            progress: ExportProgress::default(),
            cursor_state: CursorState::default(),
            stats: CheckpointStats::default(),
            last_error: None,
            retry_count: 0,
        };

        self.save_checkpoint(&checkpoint)?;
        Ok(checkpoint)
    }

    /// Load an existing export session
    pub fn load_session(&self, session_id: &str) -> Result<Option<ExportCheckpoint>> {
        validate_session_id(session_id)?;
        let checkpoint_path = self.get_checkpoint_path(session_id);

        if !checkpoint_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&checkpoint_path).with_context(|| {
            format!(
                "Failed to read checkpoint file: {}",
                checkpoint_path.display()
            )
        })?;

        let checkpoint: ExportCheckpoint = serde_json::from_str(&content).with_context(|| {
            format!(
                "Failed to parse checkpoint file: {}",
                checkpoint_path.display()
            )
        })?;

        Ok(Some(checkpoint))
    }

    /// List all available export sessions
    pub fn list_sessions(&self) -> Result<Vec<ExportCheckpoint>> {
        let mut sessions = Vec::new();

        if !self.checkpoint_dir.exists() {
            return Ok(sessions);
        }

        for entry in fs::read_dir(&self.checkpoint_dir).with_context(|| {
            format!(
                "Failed to read checkpoint directory: {}",
                self.checkpoint_dir.display()
            )
        })? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(checkpoint) = serde_json::from_str::<ExportCheckpoint>(&content) {
                        sessions.push(checkpoint);
                    }
                }
            }
        }

        // Sort by last checkpoint time (most recent first)
        sessions.sort_by(|a, b| b.last_checkpoint.cmp(&a.last_checkpoint));

        Ok(sessions)
    }

    /// Delete a persisted checkpoint after the operator has inspected it.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        validate_session_id(session_id)?;
        let checkpoint_path = self.get_checkpoint_path(session_id);
        if !checkpoint_path.exists() {
            anyhow::bail!("Checkpoint '{}' not found", session_id);
        }
        fs::remove_file(&checkpoint_path).with_context(|| {
            format!(
                "Failed to delete checkpoint file: {}",
                checkpoint_path.display()
            )
        })?;
        Ok(())
    }

    /// Save checkpoint to disk
    pub fn save_checkpoint(&self, checkpoint: &ExportCheckpoint) -> Result<()> {
        validate_session_id(&checkpoint.session_id)?;
        let checkpoint_path = self.get_checkpoint_path(&checkpoint.session_id);

        let content =
            serde_json::to_string_pretty(checkpoint).context("Failed to serialize checkpoint")?;

        write_checkpoint_atomically(&checkpoint_path, content.as_bytes())?;

        // Clean up old checkpoints
        self.cleanup_old_checkpoints()?;

        Ok(())
    }

    /// Update checkpoint with current progress
    pub fn update_checkpoint(
        &self,
        checkpoint: &mut ExportCheckpoint,
        progress: ExportProgress,
        cursor_state: CursorState,
        stats: CheckpointStats,
    ) -> Result<()> {
        checkpoint.progress = progress;
        checkpoint.cursor_state = cursor_state;
        checkpoint.stats = stats;
        checkpoint.last_checkpoint = Utc::now();

        self.save_checkpoint(checkpoint)?;
        Ok(())
    }

    /// Mark export as completed and clean up
    pub fn complete_export(&self, session_id: &str) -> Result<()> {
        let checkpoint_path = self.get_checkpoint_path(session_id);

        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path).with_context(|| {
                format!(
                    "Failed to remove completed checkpoint: {}",
                    checkpoint_path.display()
                )
            })?;
        }

        Ok(())
    }

    /// Mark export as failed and update error info
    pub fn mark_failed(&self, checkpoint: &mut ExportCheckpoint, error: String) -> Result<()> {
        checkpoint.last_error = Some(error);
        checkpoint.retry_count += 1;
        checkpoint.last_checkpoint = Utc::now();

        self.save_checkpoint(checkpoint)?;
        Ok(())
    }

    /// Check if enough time has passed for next checkpoint.
    ///
    /// Uses an absolute-value comparison so a backward clock adjustment (NTP, DST) doesn't
    /// suppress checkpointing indefinitely. We treat any "anomalous" gap (negative or larger
    /// than 24h) as a signal to checkpoint immediately.
    pub fn should_checkpoint(&self, last_checkpoint: DateTime<Utc>) -> bool {
        let elapsed = Utc::now()
            .signed_duration_since(last_checkpoint)
            .num_seconds();
        let interval = self.checkpoint_interval.as_secs() as i64;
        elapsed < 0 || elapsed >= interval
    }

    /// Generate a unique session ID.
    ///
    /// Uses high-resolution time + a random suffix so the result is stable across Rust
    /// compiler versions (`std::collections::hash_map::DefaultHasher` is not). Includes a
    /// short namespace based on database/collection so manual inspection of checkpoint
    /// filenames is at least somewhat human-readable.
    fn generate_session_id(&self, config: &ExportConfig) -> String {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let suffix: u64 = rand::random();
        let safe_db: String = config
            .database
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(16)
            .collect();
        let safe_coll: String = config
            .collection
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(16)
            .collect();
        format!(
            "{CHECKPOINT_FILE_PREFIX}{}-{}-{:x}-{:x}",
            safe_db, safe_coll, nanos, suffix
        )
    }

    /// Get checkpoint file path for session
    fn get_checkpoint_path(&self, session_id: &str) -> PathBuf {
        self.checkpoint_dir.join(format!("{}.json", session_id))
    }

    /// Clean up old checkpoint files
    fn cleanup_old_checkpoints(&self) -> Result<()> {
        let mut checkpoints = Vec::new();

        for entry in fs::read_dir(&self.checkpoint_dir)? {
            let entry = entry?;
            let path = entry.path();

            if is_managed_checkpoint_file(&path) {
                if let Ok(modified) = entry.metadata().and_then(|metadata| metadata.modified()) {
                    checkpoints.push((path, modified));
                }
            }
        }

        // Sort by modification time (newest first)
        checkpoints.sort_by(|a, b| b.1.cmp(&a.1));

        // Remove old checkpoints beyond max_checkpoints
        for (path, _) in checkpoints.into_iter().skip(self.max_checkpoints) {
            let _ = fs::remove_file(path);
        }

        Ok(())
    }

    /// Find interrupted exports that can be resumed
    pub fn find_resumable_exports(&self) -> Result<Vec<ExportCheckpoint>> {
        let sessions = self.list_sessions()?;

        // Filter for sessions that were interrupted (not completed)
        let resumable: Vec<ExportCheckpoint> = sessions
            .into_iter()
            .filter(|checkpoint| {
                // Consider resumable if last checkpoint was recent and has some progress
                let elapsed = Utc::now().signed_duration_since(checkpoint.last_checkpoint);
                elapsed.num_hours() < 24 && checkpoint.progress.documents_processed > 0
            })
            .collect();

        Ok(resumable)
    }

    /// Resume export from checkpoint
    pub fn resume_from_checkpoint(&self, checkpoint: &ExportCheckpoint) -> Result<ResumeInfo> {
        let output_mode = self.determine_output_mode(checkpoint)?;
        let resume_filter = if matches!(&output_mode, OutputMode::Append) {
            self.build_resume_filter(checkpoint)?
        } else {
            checkpoint.config.filter.clone()
        };
        let progress_offset = if matches!(&output_mode, OutputMode::Append) {
            checkpoint.progress.documents_exported
        } else {
            0
        };
        let resume_info = ResumeInfo {
            resume_filter,
            output_mode,
            progress_offset,
        };

        Ok(resume_info)
    }

    /// Build MongoDB filter for resuming from last position
    fn build_resume_filter(&self, checkpoint: &ExportCheckpoint) -> Result<Document> {
        let mut resume_filter = checkpoint.config.filter.clone();

        // Add resume condition based on cursor state. A checkpoint without an explicit sort
        // cannot prove that all unseen IDs compare after the last observed ID, so fail closed.
        if let Some(last_id) = &checkpoint.cursor_state.last_id {
            let sort_doc =
                checkpoint.config.sort.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("Cannot resume safely: checkpoint has no sort")
                })?;
            let last_sort_key =
                checkpoint
                    .cursor_state
                    .last_sort_key
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Cannot resume sorted export: no sort key captured")
                    })?;
            if last_sort_key.get("_id") != Some(last_id) {
                anyhow::bail!("Cannot resume safely: captured _id cursor values disagree");
            }
            resume_filter =
                self.build_sorted_resume_filter(resume_filter, sort_doc, last_sort_key)?;
        } else if checkpoint.progress.documents_exported > 0 {
            anyhow::bail!(
                "Cannot resume append safely: checkpoint has progress but no cursor position"
            );
        }

        Ok(resume_filter)
    }

    /// Build resume filter for sorted queries
    fn build_sorted_resume_filter(
        &self,
        base_filter: Document,
        sort_doc: &Document,
        last_sort_key: &Document,
    ) -> Result<Document> {
        let sort_fields: Vec<String> = sort_doc.keys().cloned().collect();
        if sort_fields.as_slice() != ["_id"] {
            anyhow::bail!(
                "Cannot safely resume this sort; checkpointed exports must sort only by _id"
            );
        }

        let direction = sort_doc
            .get_i32("_id")
            .context("Cannot resume safely: _id sort direction must be 1 or -1")?;
        let operator = match direction {
            1 => "$gt",
            -1 => "$lt",
            _ => anyhow::bail!("Cannot resume safely: _id sort direction must be 1 or -1"),
        };
        let last_value = last_sort_key
            .get("_id")
            .ok_or_else(|| anyhow::anyhow!("Cannot resume sorted export: _id was not captured"))?;
        let mut comparison = Document::new();
        comparison.insert(operator, last_value.clone());
        Ok(combine_filters(base_filter, doc! { "_id": comparison }))
    }

    /// Determine how to handle output file for resume
    fn determine_output_mode(&self, checkpoint: &ExportCheckpoint) -> Result<OutputMode> {
        let output_path = Path::new(&checkpoint.config.output_path);

        if output_path.exists() {
            let file_size = fs::metadata(output_path)?.len();

            // Check if file size matches our recorded bytes written
            if file_size == checkpoint.progress.bytes_written {
                Ok(OutputMode::Append)
            } else {
                Ok(OutputMode::Recreate) // File was modified, start over
            }
        } else {
            Ok(OutputMode::Create) // File doesn't exist
        }
    }
}

/// Information needed to resume an export
#[derive(Debug)]
pub struct ResumeInfo {
    pub resume_filter: Document,
    pub output_mode: OutputMode,
    pub progress_offset: u64,
}

/// How to handle the output file during resume
#[derive(Debug)]
pub enum OutputMode {
    Create,   // Create new file
    Append,   // Append to existing file
    Recreate, // Recreate file (corruption detected)
}

/// Display resumable exports in a user-friendly format
pub fn display_resumable_exports(exports: &[ExportCheckpoint]) {
    if exports.is_empty() {
        println!("📋 No resumable exports found");
        return;
    }

    println!("📋 Found {} resumable export(s):", exports.len());
    println!();

    for (i, checkpoint) in exports.iter().enumerate() {
        println!("{}. Session: {}", i + 1, checkpoint.session_id);
        println!(
            "   Database: {}.{}",
            checkpoint.config.database, checkpoint.config.collection
        );
        println!(
            "   Started: {}",
            checkpoint.started_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!(
            "   Progress: {:.1}% ({} docs)",
            checkpoint.progress.percentage_complete, checkpoint.progress.documents_exported
        );
        println!("   Output: {}", checkpoint.config.output_path);

        if let Some(ref error) = checkpoint.last_error {
            println!("   Last Error: {}", error);
        }

        println!("   Retries: {}", checkpoint.retry_count);
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mongo-exporter-{}-{:x}",
            name,
            rand::random::<u64>()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn checkpoint_for_output(output_path: String) -> ExportCheckpoint {
        ExportCheckpoint {
            checkpoint_version: CHECKPOINT_VERSION,
            session_id: "session-123".to_string(),
            started_at: Utc::now(),
            last_checkpoint: Utc::now(),
            config: ExportConfig {
                uri: String::new(),
                database: "db".to_string(),
                collection: "coll".to_string(),
                filter: Document::new(),
                format: ExportFormat::JsonLines,
                compression: CompressionType::None,
                output_path,
                fields: None,
                sort: None,
                limit: Some(10),
                skip: None,
            },
            progress: ExportProgress {
                documents_processed: 5,
                documents_exported: 5,
                documents_failed: 0,
                bytes_written: 5,
                percentage_complete: 50.0,
                eta_seconds: None,
            },
            cursor_state: CursorState::default(),
            stats: CheckpointStats::default(),
            last_error: None,
            retry_count: 0,
        }
    }

    #[test]
    fn test_validate_session_id_accepts_safe_alphabet() {
        assert!(validate_session_id("abc123-_xyz").is_ok());
        assert!(validate_session_id("session-1234abcd").is_ok());
    }

    #[test]
    fn test_validate_session_id_rejects_path_traversal() {
        assert!(validate_session_id("../etc/passwd").is_err());
        assert!(validate_session_id("..").is_err());
        assert!(validate_session_id("a/b").is_err());
        assert!(validate_session_id("a\\b").is_err());
        assert!(validate_session_id("").is_err());
    }

    #[test]
    fn test_export_config_uri_is_not_serialized() {
        // Regression: checkpoints used to persist plaintext credentials in the cache dir.
        let config = ExportConfig {
            uri: "mongodb://user:secret@host:27017".to_string(),
            database: "d".to_string(),
            collection: "c".to_string(),
            filter: Document::new(),
            format: ExportFormat::JsonLines,
            compression: CompressionType::None,
            output_path: "out.jsonl".to_string(),
            fields: None,
            sort: None,
            limit: None,
            skip: None,
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(!json.contains("secret"), "URI leaked into JSON: {}", json);
        assert!(!json.contains("user:"), "URI leaked into JSON: {}", json);
        assert!(!json.contains("\"uri\""));

        // Round-trip yields an empty URI; caller must repopulate from --uri.
        let restored: ExportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.uri, "");
    }

    #[test]
    fn test_resume_recreate_starts_from_zero_progress() {
        let dir = test_dir("resume-recreate");
        let output_path = dir.join("out.jsonl");
        fs::write(&output_path, "modified").unwrap();
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let checkpoint = checkpoint_for_output(output_path.to_string_lossy().to_string());

        let resume_info = manager.resume_from_checkpoint(&checkpoint).unwrap();

        assert!(matches!(resume_info.output_mode, OutputMode::Recreate));
        assert_eq!(resume_info.progress_offset, 0);
        assert!(resume_info.resume_filter.is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_resume_append_requires_cursor_position() {
        let dir = test_dir("resume-no-cursor");
        let output_path = dir.join("out.jsonl");
        fs::write(&output_path, "12345").unwrap();
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let checkpoint = checkpoint_for_output(output_path.to_string_lossy().to_string());

        let err = manager.resume_from_checkpoint(&checkpoint).unwrap_err();

        assert!(err.to_string().contains("no cursor position"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn resume_preserves_original_id_filter_and_supports_string_ids() {
        let dir = test_dir("resume-string-id");
        let output_path = dir.join("out.jsonl");
        fs::write(&output_path, "12345").unwrap();
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let mut checkpoint = checkpoint_for_output(output_path.to_string_lossy().to_string());
        checkpoint.config.filter = doc! { "_id": { "$lt": "z" } };
        checkpoint.config.sort = Some(doc! { "_id": 1 });
        checkpoint.cursor_state.last_id = Some(Bson::String("m".to_string()));
        checkpoint.cursor_state.last_sort_key = Some(doc! { "_id": "m" });

        let resume = manager.resume_from_checkpoint(&checkpoint).unwrap();
        let clauses = resume.resume_filter.get_array("$and").unwrap();
        assert_eq!(clauses.len(), 2);
        assert_eq!(
            clauses[0].as_document().unwrap(),
            &doc! { "_id": { "$lt": "z" } }
        );
        assert_eq!(
            clauses[1].as_document().unwrap(),
            &doc! { "_id": { "$gt": "m" } }
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn resume_fails_closed_without_a_deterministic_id_sort() {
        let dir = test_dir("resume-unsafe-sort");
        let output_path = dir.join("out.jsonl");
        fs::write(&output_path, "12345").unwrap();
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let mut checkpoint = checkpoint_for_output(output_path.to_string_lossy().to_string());
        checkpoint.cursor_state.last_id = Some(Bson::String("m".to_string()));
        checkpoint.cursor_state.last_sort_key = Some(doc! { "_id": "m" });

        let error = manager.resume_from_checkpoint(&checkpoint).unwrap_err();
        assert!(error.to_string().contains("no sort"));

        checkpoint.config.sort = Some(doc! { "created_at": 1 });
        checkpoint.cursor_state.last_sort_key = Some(doc! { "created_at": 42, "_id": "m" });
        let error = manager.resume_from_checkpoint(&checkpoint).unwrap_err();
        assert!(error.to_string().contains("sort only by _id"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cleanup_never_deletes_unrelated_json_files() {
        let dir = test_dir("safe-cleanup");
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let mut unrelated = Vec::new();
        for index in 0..12 {
            let path = dir.join(format!("user-data-{index}.json"));
            fs::write(&path, format!(r#"{{"index":{index}}}"#)).unwrap();
            unrelated.push(path);
        }

        for index in 0..12 {
            let mut checkpoint = checkpoint_for_output("out.jsonl".to_string());
            checkpoint.session_id = format!("{CHECKPOINT_FILE_PREFIX}test-{index}");
            manager.save_checkpoint(&checkpoint).unwrap();
        }

        assert!(unrelated.iter().all(|path| path.exists()));
        let managed_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| is_managed_checkpoint_file(&entry.path()))
            .count();
        assert_eq!(managed_count, 10);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn checkpoint_publication_is_atomic_and_leaves_no_temp_file() {
        let dir = test_dir("atomic-checkpoint");
        let manager = ResumableExportManager::new(Some(dir.clone())).unwrap();
        let mut checkpoint = checkpoint_for_output("out.jsonl".to_string());
        checkpoint.session_id = format!("{CHECKPOINT_FILE_PREFIX}atomic");
        manager.save_checkpoint(&checkpoint).unwrap();

        let stored = manager
            .load_session(&checkpoint.session_id)
            .unwrap()
            .unwrap();
        assert_eq!(stored.checkpoint_version, CHECKPOINT_VERSION);
        assert!(fs::read_dir(&dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        let _ = fs::remove_dir_all(dir);
    }
}
