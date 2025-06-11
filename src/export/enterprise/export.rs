use anyhow::{Context, Result};
use console::style;
use futures::stream::StreamExt;
use mongodb::{bson::Document, options::FindOptions, Collection};
use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

// Arrow and Parquet imports
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc as ArrowArc;

use crate::config::PerformanceConfig;
use crate::types::{CompressionType, ExportFormat};
use crate::utils::{collect_field_names, get_field_value};

/// Enterprise export options
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Fields to include in export (None = all fields)
    pub fields: Option<Vec<String>>,
    /// Maximum number of documents to export
    pub limit: Option<u64>,
    /// Number of documents to skip
    pub skip: Option<u64>,
    /// Sort specification
    pub sort: Option<Document>,
    /// Enable field validation
    pub validate_fields: bool,
    /// Enable statistics collection
    pub collect_stats: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            fields: None,
            limit: None,
            skip: None,
            sort: None,
            validate_fields: true,
            collect_stats: true,
        }
    }
}

/// Export statistics
#[derive(Debug, Default)]
pub struct ExportStats {
    pub documents_processed: u64,
    pub documents_exported: u64,
    pub documents_skipped: u64,
    pub fields_discovered: usize,
    pub bytes_written: u64,
    pub processing_time_ms: u64,
    pub memory_peak_bytes: usize,
    pub errors: Vec<String>,
}

/// Enterprise export engine with advanced features
pub struct EnterpriseExporter {
    config: PerformanceConfig,
}

impl EnterpriseExporter {
    pub fn new(config: PerformanceConfig) -> Self {
        Self { config }
    }

    /// Export with enterprise options
    pub async fn export_with_options(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        format: &ExportFormat,
        compression: &CompressionType,
        options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
    ) -> Result<ExportStats> {
        let start_time = std::time::Instant::now();
        let mut stats = ExportStats::default();

        // Validate fields if requested
        if options.validate_fields {
            if let Some(ref fields) = options.fields {
                self.validate_fields(collection, filter, fields).await?;
            }
        }

        // Build find options
        let mut find_options = FindOptions::default();
        if let Some(limit) = options.limit {
            find_options.limit = Some(limit as i64);
        }
        if let Some(skip) = options.skip {
            find_options.skip = Some(skip);
        }
        if let Some(ref sort) = options.sort {
            find_options.sort = Some(sort.clone());
        }

        // Project fields if specified
        if let Some(ref fields) = options.fields {
            let mut projection = Document::new();
            for field in fields {
                projection.insert(field, 1);
            }
            find_options.projection = Some(projection);
        }

        // Export based on format
        match format {
            ExportFormat::JsonLines => {
                self.export_json_lines_enterprise(
                    collection,
                    filter,
                    output_path,
                    compression,
                    &find_options,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::JsonArray => {
                self.export_json_array_enterprise(
                    collection,
                    filter,
                    output_path,
                    compression,
                    &find_options,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Csv => {
                self.export_csv_enterprise(
                    collection,
                    filter,
                    output_path,
                    compression,
                    &find_options,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Bson => {
                self.export_bson_enterprise(
                    collection,
                    filter,
                    output_path,
                    compression,
                    &find_options,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
            ExportFormat::Parquet => {
                self.export_parquet_enterprise(
                    collection,
                    filter,
                    output_path,
                    compression,
                    &find_options,
                    options,
                    exported_count.clone(),
                    &mut stats,
                )
                .await?;
            }
        }

        // Finalize stats
        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;
        stats.memory_peak_bytes = 0; // Memory tracking removed for simplification
        stats.documents_exported = exported_count.load(Ordering::Relaxed);

        Ok(stats)
    }

    /// Validate that specified fields exist in the collection
    async fn validate_fields(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        fields: &[String],
    ) -> Result<()> {
        println!("{} Validating field existence...", style("🔍").cyan());

        // Sample a few documents to check field existence
        let mut cursor = collection
            .find(filter.clone(), FindOptions::builder().limit(100).build())
            .await
            .context("Failed to execute validation query")?;

        let mut found_fields = std::collections::HashSet::new();
        let mut doc_count = 0;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    collect_field_names(&document, "", &mut found_fields);
                    doc_count += 1;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Check which fields are missing
        let missing_fields: Vec<&String> = fields
            .iter()
            .filter(|field| !found_fields.contains(*field))
            .collect();

        if !missing_fields.is_empty() {
            let missing_str: Vec<String> = missing_fields.iter().map(|s| s.to_string()).collect();
            println!(
                "{} Warning: Fields not found in sample of {} documents: {}",
                style("⚠️").yellow(),
                doc_count,
                missing_str.join(", ")
            );
            println!("{} Available fields: {}", style("ℹ️").blue(), {
                let mut sorted_fields: Vec<String> = found_fields.into_iter().collect();
                sorted_fields.sort();
                sorted_fields.join(", ")
            });
        } else {
            println!("{} All specified fields found", style("✅").green());
        }

        Ok(())
    }

    async fn export_json_lines_enterprise(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        find_options: &FindOptions,
        _options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer = self.create_writer(file, compression)?;
        let mut writer = writer;

        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut buffer = String::with_capacity(self.config.string_buffer_size);
        let mut local_count = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    let json_str = serde_json::to_string(&document)
                        .context("Failed to serialize document to JSON")?;

                    buffer.push_str(&json_str);
                    buffer.push('\n');
                    local_count += 1;

                    if buffer.len() > self.config.batch_flush_threshold {
                        stats.bytes_written += buffer.len() as u64;
                        writer
                            .write_all(buffer.as_bytes())
                            .context("Failed to write batch to file")?;
                        buffer.clear();
                        exported_count.store(local_count, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                    continue;
                }
            }
        }

        // Write remaining buffer
        if !buffer.is_empty() {
            stats.bytes_written += buffer.len() as u64;
            writer
                .write_all(buffer.as_bytes())
                .context("Failed to write final batch to file")?;
        }

        writer.flush().context("Failed to flush output file")?;
        exported_count.store(local_count, Ordering::Relaxed);

        Ok(())
    }

    async fn export_json_array_enterprise(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        find_options: &FindOptions,
        _options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer = self.create_writer(file, compression)?;
        let mut writer = writer;

        writer
            .write_all(b"[\n")
            .context("Failed to write array start")?;

        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        // Use batching to reduce memory pressure
        let mut document_batch = Vec::with_capacity(self.config.document_batch_size);
        let mut is_first_batch = true;
        let mut local_count = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;
                    document_batch.push(document);

                    // Process batch when full
                    if document_batch.len() >= self.config.document_batch_size {
                        self.write_json_array_batch(&mut writer, &document_batch, !is_first_batch)?;

                        local_count += document_batch.len() as u64;
                        exported_count.store(local_count, Ordering::Relaxed);

                        document_batch.clear();
                        is_first_batch = false;
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                    continue;
                }
            }
        }

        // Process remaining documents in batch
        if !document_batch.is_empty() {
            self.write_json_array_batch(&mut writer, &document_batch, !is_first_batch)?;
            local_count += document_batch.len() as u64;
            exported_count.store(local_count, Ordering::Relaxed);
        }

        writer
            .write_all(b"\n]\n")
            .context("Failed to write array end")?;
        writer.flush().context("Failed to flush output file")?;

        Ok(())
    }

    async fn export_csv_enterprise(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        find_options: &FindOptions,
        options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        use csv::Writer;

        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer = self.create_writer(file, compression)?;
        let mut csv_writer = Writer::from_writer(writer);

        // Determine fields to export
        let fields = if let Some(ref specified_fields) = options.fields {
            specified_fields.clone()
        } else {
            // Discover fields from sample documents
            self.discover_csv_fields(collection, filter, find_options)
                .await?
        };

        stats.fields_discovered = fields.len();

        // Write header
        csv_writer
            .write_record(&fields)
            .context("Failed to write CSV header")?;

        // Export data
        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut local_count = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    let row: Vec<String> = fields
                        .iter()
                        .map(|field| get_field_value(&document, field))
                        .collect();

                    csv_writer
                        .write_record(&row)
                        .context("Failed to write CSV row")?;

                    local_count += 1;
                    exported_count.store(local_count, Ordering::Relaxed);
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                    continue;
                }
            }
        }

        csv_writer.flush().context("Failed to flush CSV file")?;
        Ok(())
    }

    /// Write a batch of documents as JSON array elements with memory-efficient streaming
    fn write_json_array_batch(
        &self,
        writer: &mut Box<dyn Write + Send>,
        documents: &[Document],
        needs_comma_prefix: bool,
    ) -> Result<()> {
        // Use a string buffer for the batch to limit memory usage
        let mut batch_buffer = String::with_capacity(self.config.string_buffer_size);

        for (i, document) in documents.iter().enumerate() {
            // Add comma separator
            if needs_comma_prefix || i > 0 {
                batch_buffer.push_str(",\n");
            }

            // Serialize document to pretty JSON
            let json_str = serde_json::to_string_pretty(document)
                .context("Failed to serialize document to JSON")?;

            // Add indentation to each line
            for line in json_str.lines() {
                batch_buffer.push_str("  ");
                batch_buffer.push_str(line);
                batch_buffer.push('\n');
            }

            // Flush buffer if it gets too large
            if batch_buffer.len() > self.config.batch_flush_threshold {
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

    async fn export_bson_enterprise(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        find_options: &FindOptions,
        _options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer = self.create_writer(file, compression)?;
        let mut writer = writer;

        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let mut local_count = 0u64;

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
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                    continue;
                }
            }
        }

        writer.flush().context("Failed to flush BSON file")?;
        Ok(())
    }

    async fn export_parquet_enterprise(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        find_options: &FindOptions,
        _options: &ExportOptions,
        exported_count: Arc<AtomicU64>,
        stats: &mut ExportStats,
    ) -> Result<()> {
        println!(
            "{} Starting Parquet export with columnar optimization...",
            style("📊").cyan()
        );

        // First pass: discover schema by sampling documents
        let fields = self
            .discover_csv_fields(collection, filter, find_options)
            .await?;
        stats.fields_discovered = fields.len();

        println!(
            "{} Discovered {} fields for Parquet schema",
            style("🔍").cyan(),
            fields.len()
        );

        // Create Arrow schema
        let arrow_fields: Vec<Field> = fields
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = ArrowArc::new(Schema::new(arrow_fields));

        // Create Parquet writer
        let file = std::fs::File::create(output_path)
            .with_context(|| format!("Failed to create Parquet file: {}", output_path))?;

        let writer_props = WriterProperties::builder()
            .set_compression(match compression {
                CompressionType::None => parquet::basic::Compression::UNCOMPRESSED,
                CompressionType::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            })
            .build();

        let mut parquet_writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props))?;

        // Second pass: stream documents and write in batches
        let mut cursor = collection
            .find(filter.clone(), find_options.clone())
            .await
            .context("Failed to execute query")?;

        let batch_size = self.config.batch_flush_threshold / 1024; // Reasonable batch size for Parquet
        let mut batch_data: Vec<Vec<Option<String>>> = vec![Vec::new(); fields.len()];
        let mut local_count = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    stats.documents_processed += 1;

                    // Extract values for each field
                    for (field_idx, field_name) in fields.iter().enumerate() {
                        let value = if get_field_value(&document, field_name).is_empty() {
                            None
                        } else {
                            Some(get_field_value(&document, field_name))
                        };
                        batch_data[field_idx].push(value);
                    }

                    local_count += 1;

                    // Write batch when it reaches the desired size
                    if batch_data[0].len() >= batch_size {
                        let batch = self.create_arrow_batch(&schema, &fields, &mut batch_data)?;
                        parquet_writer.write(&batch)?;

                        // Clear batch data for next batch
                        for field_data in &mut batch_data {
                            field_data.clear();
                        }

                        exported_count.store(local_count, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    stats.errors.push(format!("Document read error: {}", e));
                    continue;
                }
            }
        }

        // Write remaining data
        if !batch_data[0].is_empty() {
            let batch = self.create_arrow_batch(&schema, &fields, &mut batch_data)?;
            parquet_writer.write(&batch)?;
        }

        // Close the writer
        parquet_writer.close()?;
        exported_count.store(local_count, Ordering::Relaxed);

        println!(
            "{} Parquet export completed with columnar compression",
            style("✅").green()
        );

        Ok(())
    }

    fn create_arrow_batch(
        &self,
        schema: &ArrowArc<Schema>,
        _fields: &[String],
        batch_data: &mut [Vec<Option<String>>],
    ) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for field_data in batch_data.iter() {
            let array = StringArray::from(field_data.clone());
            columns.push(ArrowArc::new(array) as ArrayRef);
        }

        RecordBatch::try_new(schema.clone(), columns).context("Failed to create Arrow RecordBatch")
    }

    async fn discover_csv_fields(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: &FindOptions,
    ) -> Result<Vec<String>> {
        // Use the centralized CSV field discovery utility
        crate::utils::discover_csv_fields(
            collection,
            filter,
            self.config.csv_field_sample_size,
            Some(find_options),
        )
        .await
    }

    fn create_writer(
        &self,
        file: File,
        compression: &CompressionType,
    ) -> Result<Box<dyn Write + Send>> {
        let writer: Box<dyn Write + Send> = match compression {
            CompressionType::None => Box::new(BufWriter::with_capacity(
                self.config.write_buffer_size,
                file,
            )),
            CompressionType::Gzip => {
                use flate2::{write::GzEncoder, Compression};
                let gz_encoder = GzEncoder::new(file, Compression::default());
                Box::new(BufWriter::with_capacity(
                    self.config.write_buffer_size,
                    gz_encoder,
                ))
            }
        };

        Ok(writer)
    }
}

/// Print detailed export statistics
pub fn print_export_stats(stats: &ExportStats) {
    println!();
    println!("{}", style("📊 Export Statistics").cyan().bold());
    println!("┌{}┐", "─".repeat(50));
    println!(
        "│ {:<30} {:>15} │",
        "Documents processed:", stats.documents_processed
    );
    println!(
        "│ {:<30} {:>15} │",
        "Documents exported:", stats.documents_exported
    );

    if stats.documents_skipped > 0 {
        println!(
            "│ {:<30} {:>15} │",
            "Documents skipped:", stats.documents_skipped
        );
    }

    if stats.fields_discovered > 0 {
        println!(
            "│ {:<30} {:>15} │",
            "Fields discovered:", stats.fields_discovered
        );
    }

    println!(
        "│ {:<30} {:>15} │",
        "Bytes written:",
        format_bytes(stats.bytes_written)
    );
    println!(
        "│ {:<30} {:>15} │",
        "Processing time:",
        format!("{}ms", stats.processing_time_ms)
    );
    println!(
        "│ {:<30} {:>15} │",
        "Peak memory:",
        format_bytes(stats.memory_peak_bytes as u64)
    );

    if stats.processing_time_ms > 0 {
        let docs_per_sec =
            (stats.documents_exported as f64 * 1000.0) / stats.processing_time_ms as f64;
        println!(
            "│ {:<30} {:>15} │",
            "Throughput:",
            format!("{:.0} docs/sec", docs_per_sec)
        );
    }

    if !stats.errors.is_empty() {
        println!("│ {:<30} {:>15} │", "Errors:", stats.errors.len());
    }

    println!("└{}┘", "─".repeat(50));

    // Show errors if any
    if !stats.errors.is_empty() {
        println!();
        println!("{}", style("⚠️ Errors encountered:").yellow().bold());
        for (i, error) in stats.errors.iter().enumerate().take(5) {
            println!("  {}. {}", i + 1, error);
        }
        if stats.errors.len() > 5 {
            println!("  ... and {} more errors", stats.errors.len() - 5);
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", size as u64, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}
