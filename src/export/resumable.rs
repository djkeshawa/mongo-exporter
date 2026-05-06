use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use crate::types::{CompressionType, ExportFormat};

/// Export checkpoint containing state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCheckpoint {
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
    /// Last processed document ID
    pub last_id: Option<mongodb::bson::oid::ObjectId>,
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

        println!("📋 Created resumable export session: {}", session_id);
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

    /// Save checkpoint to disk
    pub fn save_checkpoint(&self, checkpoint: &ExportCheckpoint) -> Result<()> {
        let checkpoint_path = self.get_checkpoint_path(&checkpoint.session_id);

        let content =
            serde_json::to_string_pretty(checkpoint).context("Failed to serialize checkpoint")?;

        fs::write(&checkpoint_path, content).with_context(|| {
            format!(
                "Failed to write checkpoint file: {}",
                checkpoint_path.display()
            )
        })?;

        // Restrict to owner-only on Unix. Checkpoints carry filter and field information that
        // can be sensitive; until we drop more credentials/PII this is cheap defense in depth.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&checkpoint_path, std::fs::Permissions::from_mode(0o600));
        }

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

        println!(
            "✅ Export session {} completed and checkpoint removed",
            session_id
        );
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
        format!("{}-{}-{:x}-{:x}", safe_db, safe_coll, nanos, suffix)
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

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Ok(metadata) = entry.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        checkpoints.push((path, modified));
                    }
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

        println!(
            "🔄 Resuming export from {} documents ({:.1}% complete)",
            checkpoint.progress.documents_exported, checkpoint.progress.percentage_complete
        );

        Ok(resume_info)
    }

    /// Build MongoDB filter for resuming from last position
    fn build_resume_filter(&self, checkpoint: &ExportCheckpoint) -> Result<Document> {
        let mut resume_filter = checkpoint.config.filter.clone();

        // Add resume condition based on cursor state
        if let Some(last_id) = &checkpoint.cursor_state.last_id {
            // Use _id for resume if no sort specified
            if checkpoint.config.sort.is_none() {
                resume_filter.insert("_id", doc! {"$gt": last_id});
            } else if let Some(ref sort_doc) = checkpoint.config.sort {
                // Build complex resume condition for sorted queries
                if let Some(ref last_sort_key) = checkpoint.cursor_state.last_sort_key {
                    resume_filter =
                        self.build_sorted_resume_filter(resume_filter, sort_doc, last_sort_key)?;
                }
            }
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
        mut base_filter: Document,
        sort_doc: &Document,
        last_sort_key: &Document,
    ) -> Result<Document> {
        // For complex sorted resume, we need to handle multiple sort fields
        // This is a simplified version - production would need more sophisticated logic

        let sort_fields: Vec<String> = sort_doc.keys().cloned().collect();

        if sort_fields.len() == 1 {
            let field = &sort_fields[0];
            if let Some(last_value) = last_sort_key.get(field) {
                let direction = sort_doc.get_i32(field).unwrap_or(1);
                let operator = if direction >= 0 { "$gt" } else { "$lt" };
                base_filter.insert(field, doc! {operator: last_value});
            }
        } else if let Some(last_id) = last_sort_key.get("_id") {
            // For multi-field sorts, fall back to _id-based resume only if we actually
            // captured the _id of the last processed document. Inserting an Option<&Bson>
            // directly would serialize the enum wrapper and produce a filter that matches
            // nothing, silently breaking resume.
            base_filter.insert("_id", doc! {"$gt": last_id});
        } else {
            anyhow::bail!(
                "Cannot resume sorted export: no _id captured in cursor state for multi-field sort"
            );
        }

        Ok(base_filter)
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
}
