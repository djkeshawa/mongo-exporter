use anyhow::{Context, Result};
use console::style;
use futures::stream::StreamExt;
use mongodb::{bson::Document, options::FindOptions, Collection};
use std::{
    io::{Seek, SeekFrom, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::config::PerformanceConfig;
use crate::export::enterprise::export::{ExportOptions, ExportStats};
use crate::export::formats::csv_optimizer::CsvOptimizer;
use crate::export::formats::json_optimizer::JsonOptimizer;
use crate::export::resumable::{
    CheckpointStats, ExportCheckpoint, ExportConfig, ExportProgress, OutputMode,
    ResumableExportManager, ResumeInfo,
};
use crate::types::{CompressionType, ExportFormat};
use crate::utils::error_handling::ErrorHandlingConfig;
use crate::utils::{document_to_json_value, wrap_writer_with_compression};

/// Parameters for optimized export
struct OptimizedExportParams<'a> {
    collection: &'a Collection<Document>,
    filter: &'a Document,
    output_path: &'a str,
    format: &'a ExportFormat,
    compression: &'a CompressionType,
    exported_count: Arc<AtomicU64>,
    find_options: Option<FindOptions>,
}

/// Enhanced enterprise exporter with resumable exports and advanced error handling
pub struct EnhancedEnterpriseExporter {
    performance_config: PerformanceConfig,
    resume_manager: ResumableExportManager,
    start_time: Instant,
}

impl EnhancedEnterpriseExporter {
    pub fn new(
        performance_config: PerformanceConfig,
        _error_config: Option<ErrorHandlingConfig>,
        checkpoint_dir: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let resume_manager = ResumableExportManager::new(checkpoint_dir)?;

        Ok(Self {
            performance_config,
            resume_manager,
            start_time: Instant::now(),
        })
    }

    /// Export with full enterprise features: resumable, error handling, monitoring
    #[allow(clippy::too_many_arguments)]
    pub async fn export_with_enterprise_features(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        format: &ExportFormat,
        compression: &CompressionType,
        options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        resume_session_id: Option<String>,
        uri: &str,
    ) -> Result<ExportStats> {
        // Check for resumable session
        let (checkpoint, resume_info) = if let Some(session_id) = resume_session_id {
            if let Some(checkpoint) = self.resume_manager.load_session(&session_id)? {
                let resume_info = self.resume_manager.resume_from_checkpoint(&checkpoint)?;
                (Some(checkpoint), Some(resume_info))
            } else {
                return Err(anyhow::anyhow!("Resume session '{}' not found", session_id));
            }
        } else {
            // Check for auto-resumable exports
            let resumable = self.resume_manager.find_resumable_exports()?;
            if !resumable.is_empty() {
                println!(
                    "🔍 Found {} resumable export(s). Use --resume <session-id> to continue.",
                    resumable.len()
                );
                crate::export::resumable::display_resumable_exports(&resumable);

                // For now, start new export. In interactive mode, we could ask user.
                (None, None)
            } else {
                (None, None)
            }
        };

        // Create or update export configuration
        let export_config = ExportConfig {
            uri: uri.to_string(),
            database: collection.namespace().db.clone(),
            collection: collection.namespace().coll.clone(),
            filter: filter.clone(),
            format: format.clone(),
            compression: compression.clone(),
            output_path: output_path.to_string(),
            fields: options.fields.clone(),
            sort: options.sort.clone(),
            limit: options.limit,
            skip: options.skip,
        };

        // Create or resume checkpoint
        let mut checkpoint = if let Some(existing_checkpoint) = checkpoint {
            existing_checkpoint
        } else {
            self.resume_manager.create_session(export_config)?
        };

        // Execute export. We deliberately do NOT wrap this in execute_with_retry: a resumable
        // export already has restart semantics via the checkpoint, and retrying the entire
        // pipeline from scratch would re-truncate the output file.
        let result = self
            .execute_resumable_export(
                collection,
                &mut checkpoint,
                resume_info.as_ref(),
                options,
                exported_count.clone(),
            )
            .await;

        match result {
            Ok(stats) => {
                self.resume_manager
                    .complete_export(&checkpoint.session_id)?;
                Ok(stats)
            }
            Err(error) => {
                // Export failed, save checkpoint for potential resume
                self.resume_manager
                    .mark_failed(&mut checkpoint, error.to_string())?;

                println!();
                println!(
                    "{} Export failed and checkpoint saved",
                    style("💾").yellow()
                );
                println!("Resume with: --resume {}", checkpoint.session_id);

                Err(error)
            }
        }
    }

    /// Execute resumable export with checkpointing
    async fn execute_resumable_export(
        &self,
        collection: &Collection<Document>,
        checkpoint: &mut ExportCheckpoint,
        resume_info: Option<&ResumeInfo>,
        options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
    ) -> Result<ExportStats> {
        let start_time = Instant::now();
        let mut stats = ExportStats::default();

        // Determine query filter and options
        let (query_filter, find_options, output_writer) = if let Some(resume_info) = resume_info {
            // Resuming export
            println!("🔄 Resuming export from checkpoint...");

            // Gzip resume is intrinsically unsafe today: bytes_written counts uncompressed
            // bytes while the on-disk file is compressed, so the size comparison in
            // determine_output_mode falls through to Recreate — silently dropping previously
            // exported rows. And even if we appended, concatenated gzip streams aren't read
            // correctly by Python's gzip / Java's GZIPInputStream / many analytics tools.
            // Refuse loudly until we can track compressed bytes and emit a single stream.
            if matches!(checkpoint.config.compression, CompressionType::Gzip) {
                anyhow::bail!(
                    "Gzip-compressed exports cannot be safely resumed (would silently truncate \
                     or produce a multi-stream file rejected by some readers). Re-run from \
                     scratch (omit --resume) or export without --compression gzip."
                );
            }

            let find_options = self.build_resume_find_options(&checkpoint.config, resume_info)?;
            let writer = self.setup_resume_output(&checkpoint.config, resume_info)?;

            (resume_info.resume_filter.clone(), find_options, writer)
        } else {
            // New export - check if we can use optimized versions
            if self.performance_config.enable_parallel_processing && resume_info.is_none() {
                // Build find options for the optimized export
                let find_options = self.build_find_options(&checkpoint.config)?;

                // Use optimized versions for new exports when parallel processing is enabled
                return self
                    .execute_optimized_export(OptimizedExportParams {
                        collection,
                        filter: &checkpoint.config.filter,
                        output_path: &checkpoint.config.output_path,
                        format: &checkpoint.config.format,
                        compression: &checkpoint.config.compression,
                        exported_count,
                        find_options: Some(find_options),
                    })
                    .await;
            }

            let find_options = self.build_find_options(&checkpoint.config)?;
            let writer = self.setup_new_output(&checkpoint.config)?;

            (checkpoint.config.filter.clone(), find_options, writer)
        };

        // CSV needs field discovery up front; cache the schema on the checkpoint so a resume
        // sees the same columns as the original run (otherwise we'd silently corrupt the file).
        if matches!(checkpoint.config.format, ExportFormat::Csv)
            && checkpoint.config.fields.is_none()
            && options.fields.is_none()
        {
            let discovered = crate::utils::discover_csv_fields(
                collection,
                &checkpoint.config.filter,
                self.performance_config.csv_field_sample_size,
                None,
            )
            .await?;
            checkpoint.config.fields = Some(discovered);
            self.resume_manager.save_checkpoint(checkpoint)?;
        }

        // Execute format-specific export
        let format = checkpoint.config.format.clone();
        match format {
            ExportFormat::JsonLines => {
                self.export_jsonl_resumable(
                    collection,
                    &query_filter,
                    &find_options,
                    output_writer,
                    checkpoint,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::JsonArray => {
                self.export_json_array_resumable(
                    collection,
                    &query_filter,
                    &find_options,
                    output_writer,
                    checkpoint,
                    resume_info.is_some(),
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Csv => {
                self.export_csv_resumable(
                    collection,
                    &query_filter,
                    &find_options,
                    output_writer,
                    checkpoint,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Bson => {
                self.export_bson_resumable(
                    collection,
                    &query_filter,
                    &find_options,
                    output_writer,
                    checkpoint,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Parquet => {
                if resume_info.is_some() {
                    anyhow::bail!(
                        "Parquet exports cannot be resumed: the format writes its row-group \
                         index in a footer at the end of the file, so partial files are not \
                         appendable. Re-run the export from scratch (omit --resume) or choose \
                         JSONL/CSV/BSON for a resumable export."
                    );
                }
                drop(output_writer); // unused: parquet manages its own File handle
                self.export_parquet_resumable(
                    collection,
                    &query_filter,
                    &find_options,
                    checkpoint,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
        }

        // Finalize stats
        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;
        stats.documents_exported = exported_count.load(Ordering::Relaxed);

        Ok(stats)
    }

    /// Execute optimized export for new exports when parallel processing is enabled
    async fn execute_optimized_export(
        &self,
        params: OptimizedExportParams<'_>,
    ) -> Result<ExportStats> {
        let start_time = Instant::now();
        let mut stats = ExportStats::default();

        println!("⚡ Using optimized parallel export for {}", params.format);

        match params.format {
            ExportFormat::JsonLines => {
                let optimizer = JsonOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_json_lines_parallel(
                        params.collection,
                        params.filter,
                        params.output_path,
                        params.compression,
                        params.exported_count.clone(),
                        params.find_options,
                    )
                    .await?;
            }
            ExportFormat::JsonArray => {
                let optimizer = JsonOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_json_array_parallel(
                        params.collection,
                        params.filter,
                        params.output_path,
                        params.compression,
                        params.exported_count.clone(),
                        params.find_options,
                    )
                    .await?;
            }
            ExportFormat::Csv => {
                let optimizer = CsvOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_csv_streaming(
                        params.collection,
                        params.filter,
                        params.output_path,
                        params.compression,
                        params.exported_count.clone(),
                        params.find_options,
                    )
                    .await?;
            }
            ExportFormat::Bson => {
                return Err(anyhow::anyhow!(
                    "Optimized export for BSON not available, use resumable export"
                ));
            }
            ExportFormat::Parquet => {
                return Err(anyhow::anyhow!(
                    "Optimized export for Parquet not available, use resumable export"
                ));
            }
        }

        // Finalize stats
        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;
        stats.documents_exported = params.exported_count.load(Ordering::Relaxed);

        Ok(stats)
    }

    /// Build find options from export config
    fn build_find_options(&self, config: &ExportConfig) -> Result<FindOptions> {
        let mut find_options = FindOptions::default();

        if let Some(limit) = config.limit {
            find_options.limit = Some(limit as i64);
        }

        if let Some(skip) = config.skip {
            find_options.skip = Some(skip);
        }

        if let Some(ref sort) = config.sort {
            find_options.sort = Some(sort.clone());
        }

        // Set batch size based on performance config
        find_options.batch_size = Some(self.performance_config.document_batch_size as u32);
        // Disable the server-side 10-minute idle cursor timeout: large exports trivially exceed
        // it, and getting a CursorNotFound mid-stream leaves the output file inconsistent.
        find_options.no_cursor_timeout = Some(true);

        Ok(find_options)
    }

    /// Build find options for resuming export
    fn build_resume_find_options(
        &self,
        config: &ExportConfig,
        resume_info: &ResumeInfo,
    ) -> Result<FindOptions> {
        let mut find_options = self.build_find_options(config)?;

        // Adjust limit if resuming
        if let Some(original_limit) = config.limit {
            let remaining = original_limit.saturating_sub(resume_info.progress_offset);
            find_options.limit = Some(remaining as i64);
        }

        Ok(find_options)
    }

    /// Setup output writer for new export
    fn setup_new_output(&self, config: &ExportConfig) -> Result<Box<dyn Write + Send>> {
        crate::utils::create_buffered_writer(
            &config.output_path,
            &config.compression,
            self.performance_config.write_buffer_size,
        )
    }

    /// Setup output writer for resuming export
    fn setup_resume_output(
        &self,
        config: &ExportConfig,
        resume_info: &ResumeInfo,
    ) -> Result<Box<dyn Write + Send>> {
        match &resume_info.output_mode {
            OutputMode::Create => self.setup_new_output(config),
            OutputMode::Recreate => {
                println!("⚠️  Output file was modified, recreating...");
                self.setup_new_output(config)
            }
            OutputMode::Append => {
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&config.output_path)
                    .with_context(|| {
                        format!(
                            "Failed to open output file for append: {}",
                            config.output_path
                        )
                    })?;
                file.seek(SeekFrom::End(0))?;
                Ok(wrap_writer_with_compression(
                    file,
                    &config.compression,
                    self.performance_config.write_buffer_size,
                ))
            }
        }
    }

    /// Export JSON Lines with resumable checkpointing
    #[allow(clippy::too_many_arguments)]
    async fn export_jsonl_resumable(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
        mut writer: Box<dyn Write + Send>,
        checkpoint: &mut ExportCheckpoint,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut buffer = String::with_capacity(self.performance_config.string_buffer_size);
        let mut local_count = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    let json_str = serde_json::to_string(&document_to_json_value(&document))
                        .context("Failed to serialize document to JSON")?;

                    buffer.push_str(&json_str);
                    buffer.push('\n');
                    local_count += 1;

                    // Flush buffer when threshold reached
                    if buffer.len() > self.performance_config.batch_flush_threshold {
                        writer
                            .write_all(buffer.as_bytes())
                            .context("Failed to write batch to file")?;
                        stats.bytes_written += buffer.len() as u64;
                        buffer.clear();

                        exported_count.store(local_count, Ordering::Relaxed);

                        // Checkpoint periodically. save_progress_checkpoint advances
                        // checkpoint.last_checkpoint, so the next call won't fire immediately.
                        if self
                            .resume_manager
                            .should_checkpoint(checkpoint.last_checkpoint)
                        {
                            self.save_progress_checkpoint(checkpoint, local_count, stats)?;
                        }
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                }
            }
        }

        // Write remaining buffer
        if !buffer.is_empty() {
            writer
                .write_all(buffer.as_bytes())
                .context("Failed to write final batch to file")?;
            stats.bytes_written += buffer.len() as u64;
        }

        writer.flush().context("Failed to flush output file")?;
        exported_count.store(local_count, Ordering::Relaxed);

        Ok(())
    }

    /// Export JSON Array with resumable checkpointing.
    ///
    /// Resuming a JSON Array file is intrinsically tricky: a previous run wrote `[\n` and
    /// some elements but never the closing `]`. On resume we open in append mode and continue
    /// emitting `,\n<element>` pairs, finally writing the closing `]` exactly once when the
    /// cursor drains.
    #[allow(clippy::too_many_arguments)]
    async fn export_json_array_resumable(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
        mut writer: Box<dyn Write + Send>,
        checkpoint: &mut ExportCheckpoint,
        is_resuming: bool,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        // Only emit the opening bracket on a fresh export. On resume the file already starts with `[`.
        if !is_resuming {
            writer
                .write_all(b"[\n")
                .context("Failed to write array start")?;
        }

        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut local_count = checkpoint.progress.documents_exported;

        // Need a comma prefix if there are already documents in the file (i.e., we're resuming
        // after some elements were written, or this is not the first document of a new file).
        let mut batch_needs_comma = is_resuming && checkpoint.progress.documents_exported > 0;

        let mut document_batch = Vec::with_capacity(self.performance_config.document_batch_size);

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;
                    document_batch.push(document);

                    let should_checkpoint = self
                        .resume_manager
                        .should_checkpoint(checkpoint.last_checkpoint);

                    if document_batch.len() >= self.performance_config.document_batch_size
                        || should_checkpoint
                    {
                        self.write_json_array_batch_resumable(
                            &mut writer,
                            &document_batch,
                            batch_needs_comma,
                        )?;

                        local_count += document_batch.len() as u64;
                        exported_count.store(local_count, Ordering::Relaxed);

                        batch_needs_comma = true;
                        document_batch.clear();

                        if should_checkpoint {
                            writer
                                .flush()
                                .context("Failed to flush before checkpoint")?;
                            self.save_progress_checkpoint(checkpoint, local_count, stats)?;
                        }
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                }
            }
        }

        if !document_batch.is_empty() {
            self.write_json_array_batch_resumable(&mut writer, &document_batch, batch_needs_comma)?;
            local_count += document_batch.len() as u64;
            exported_count.store(local_count, Ordering::Relaxed);
        }

        // Closing bracket goes only once, at successful completion.
        writer
            .write_all(b"\n]\n")
            .context("Failed to write array end")?;
        writer.flush().context("Failed to flush output file")?;

        Ok(())
    }

    /// Export CSV with resumable checkpointing.
    ///
    /// Field schema is sourced (in priority order) from the explicit options, the cached
    /// schema on the checkpoint config (set by the caller via `discover_csv_fields` before
    /// dispatch), or as a last resort from a fresh discovery pass.
    #[allow(clippy::too_many_arguments)]
    async fn export_csv_resumable(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
        writer: Box<dyn Write + Send>,
        checkpoint: &mut ExportCheckpoint,
        options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        use csv::Writer;

        let mut csv_writer = Writer::from_writer(writer);

        let fields: Vec<String> = if let Some(ref specified) = options.fields {
            specified.clone()
        } else if let Some(ref cached) = checkpoint.config.fields {
            cached.clone()
        } else {
            anyhow::bail!(
                "CSV resumable export requires a known field schema; none was provided or discovered"
            );
        };

        // Write header only if starting a new export
        if checkpoint.progress.documents_exported == 0 {
            csv_writer
                .write_record(&fields)
                .context("Failed to write CSV header")?;
        }

        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut local_count = checkpoint.progress.documents_exported;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    let row: Vec<String> = fields
                        .iter()
                        .map(|field| crate::utils::get_field_value(&document, field))
                        .collect();

                    csv_writer
                        .write_record(&row)
                        .context("Failed to write CSV row")?;

                    local_count += 1;
                    exported_count.store(local_count, Ordering::Relaxed);

                    if self
                        .resume_manager
                        .should_checkpoint(checkpoint.last_checkpoint)
                    {
                        csv_writer.flush()?;
                        self.save_progress_checkpoint(checkpoint, local_count, stats)?;
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                }
            }
        }

        csv_writer.flush().context("Failed to flush CSV file")?;
        Ok(())
    }

    /// Export BSON with resumable checkpointing
    #[allow(clippy::too_many_arguments)]
    async fn export_bson_resumable(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
        mut writer: Box<dyn Write + Send>,
        checkpoint: &mut ExportCheckpoint,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut local_count = checkpoint.progress.documents_exported;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    let bson_bytes = mongodb::bson::to_vec(&document)
                        .context("Failed to serialize document to BSON")?;

                    writer
                        .write_all(&bson_bytes)
                        .context("Failed to write BSON document")?;

                    stats.bytes_written += bson_bytes.len() as u64;
                    local_count += 1;
                    exported_count.store(local_count, Ordering::Relaxed);

                    // Checkpoint periodically
                    if self
                        .resume_manager
                        .should_checkpoint(checkpoint.last_checkpoint)
                    {
                        writer.flush()?;
                        self.save_progress_checkpoint(checkpoint, local_count, stats)?;
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                }
            }
        }

        writer.flush().context("Failed to flush BSON file")?;
        Ok(())
    }

    /// Export documents in Parquet format. Not actually resumable: Parquet's row-group index
    /// lives in the footer, so we don't support appending to an existing file. Resume callers
    /// are rejected upstream.
    #[allow(clippy::too_many_arguments)]
    async fn export_parquet_resumable(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
        checkpoint: &mut ExportCheckpoint,
        _options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        println!(
            "{} Starting Parquet export with columnar optimization...",
            style("📊").cyan()
        );

        // Use checkpoint configuration for output path and compression
        let output_path = &checkpoint.config.output_path;
        let compression = &checkpoint.config.compression;

        // First pass: discover schema by sampling documents. Pass the caller's find_options so
        // the user's --limit and --skip bound the discovery scan; previously this passed None
        // and could over-scan beyond what would actually be exported.
        let fields = crate::utils::discover_csv_fields(
            collection,
            filter,
            self.performance_config.csv_field_sample_size,
            Some(find_options),
        )
        .await?;

        println!(
            "{} Discovered {} fields for Parquet schema",
            style("🔍").green(),
            fields.len()
        );

        // Create Arrow schema from discovered fields
        let arrow_fields: Vec<arrow::datatypes::Field> = fields
            .iter()
            .map(|field_name| {
                arrow::datatypes::Field::new(field_name, arrow::datatypes::DataType::Utf8, true)
            })
            .collect();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(arrow_fields));

        // Create Parquet writer with compression
        let file = std::fs::File::create(output_path)
            .with_context(|| format!("Failed to create Parquet file: {}", output_path))?;

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(match compression {
                CompressionType::Gzip => {
                    parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
                }
                CompressionType::None => parquet::basic::Compression::UNCOMPRESSED,
            })
            .build();

        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create Parquet writer")?;

        // Process documents in batches using provided find_options
        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute MongoDB query")?;

        let mut local_count = 0u64;
        let batch_size = self.performance_config.document_batch_size.max(1024);
        let mut document_batch = Vec::with_capacity(batch_size);

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    document_batch.push(document);

                    if document_batch.len() >= batch_size {
                        self.write_parquet_batch(&mut writer, &document_batch, &fields, stats)?;
                        local_count += document_batch.len() as u64;
                        exported_count.store(local_count, Ordering::Relaxed);
                        document_batch.clear();

                        // Checkpoint periodically
                        if self
                            .resume_manager
                            .should_checkpoint(checkpoint.last_checkpoint)
                        {
                            self.save_progress_checkpoint(checkpoint, local_count, stats)?;
                        }
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                }
            }
        }

        // Write remaining documents
        if !document_batch.is_empty() {
            self.write_parquet_batch(&mut writer, &document_batch, &fields, stats)?;
            local_count += document_batch.len() as u64;
            exported_count.store(local_count, Ordering::Relaxed);
        }

        // Close the writer
        writer.close().context("Failed to close Parquet writer")?;

        println!(
            "{} Parquet export completed: {} documents",
            style("✅").green(),
            local_count
        );

        Ok(())
    }

    /// Write a batch of documents to Parquet format
    fn write_parquet_batch(
        &self,
        writer: &mut parquet::arrow::ArrowWriter<std::fs::File>,
        documents: &[Document],
        fields: &[String],
        stats: &mut ExportStats,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }

        // Create string arrays for each field
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();

        for field in fields {
            let mut field_values = Vec::with_capacity(documents.len());

            for document in documents {
                let value = crate::utils::get_field_value(document, field);
                field_values.push(if value.is_empty() { None } else { Some(value) });
            }

            let array = arrow::array::StringArray::from(field_values);
            columns.push(std::sync::Arc::new(array) as arrow::array::ArrayRef);
        }

        // Create record batch
        let batch = arrow::record_batch::RecordBatch::try_from_iter(
            fields
                .iter()
                .zip(columns.iter())
                .map(|(name, array)| (name.as_str(), array.clone())),
        )
        .context("Failed to create Arrow record batch")?;

        // Write batch to Parquet
        writer
            .write(&batch)
            .context("Failed to write Parquet batch")?;

        stats.documents_processed += documents.len() as u64;
        stats.bytes_written += batch.get_array_memory_size() as u64;

        Ok(())
    }

    /// Write a batch of documents as JSON array elements with memory-efficient streaming (resumable version)
    fn write_json_array_batch_resumable(
        &self,
        writer: &mut Box<dyn Write + Send>,
        documents: &[Document],
        needs_comma_prefix: bool,
    ) -> Result<()> {
        // Use a string buffer for the batch to limit memory usage
        let mut batch_buffer = String::with_capacity(self.performance_config.string_buffer_size);

        for (i, document) in documents.iter().enumerate() {
            // Add comma separator
            if needs_comma_prefix || i > 0 {
                batch_buffer.push_str(",\n");
            }

            // Serialize document to pretty JSON
            let json_str = serde_json::to_string_pretty(&document_to_json_value(document))
                .context("Failed to serialize document to JSON")?;

            // Add indentation to each line
            for line in json_str.lines() {
                batch_buffer.push_str("  ");
                batch_buffer.push_str(line);
                batch_buffer.push('\n');
            }

            // Flush buffer if it gets too large
            if batch_buffer.len() > self.performance_config.batch_flush_threshold {
                writer
                    .write_all(batch_buffer.as_bytes())
                    .context("Failed to write JSON batch")?;
                batch_buffer.clear();
            }
        }

        // Write remaining buffer
        if !batch_buffer.is_empty() {
            writer
                .write_all(batch_buffer.as_bytes())
                .context("Failed to write final JSON batch")?;
        }

        Ok(())
    }

    /// Save progress checkpoint. Updates the caller's checkpoint in place — in particular,
    /// `last_checkpoint` is advanced to "now", which is what gates `should_checkpoint` from
    /// firing on every batch after the first 30s window.
    fn save_progress_checkpoint(
        &self,
        checkpoint: &mut ExportCheckpoint,
        exported_count: u64,
        stats: &ExportStats,
    ) -> Result<()> {
        let progress = ExportProgress {
            documents_processed: stats.documents_processed,
            documents_exported: exported_count,
            documents_failed: stats.errors.len() as u64,
            bytes_written: stats.bytes_written,
            percentage_complete: 0.0,
            eta_seconds: None,
        };

        let cursor_state = checkpoint.cursor_state.clone();

        let checkpoint_stats = CheckpointStats {
            processing_rate: exported_count as f64
                / self.start_time.elapsed().as_secs_f64().max(0.001),
            avg_document_size: if exported_count > 0 {
                stats.bytes_written as f64 / exported_count as f64
            } else {
                0.0
            },
            peak_memory_bytes: 0,
            error_count: stats.errors.len() as u32,
            last_successful_batch: chrono::Utc::now(),
        };

        self.resume_manager.update_checkpoint(
            checkpoint,
            progress,
            cursor_state,
            checkpoint_stats,
        )?;

        Ok(())
    }
}
