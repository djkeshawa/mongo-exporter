use anyhow::{Context, Result};
use csv::Writer;
use futures::stream::StreamExt;
use mongodb::{bson::Document, Collection};
use std::{
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::config::PerformanceConfig;
use crate::types::CompressionType;
use crate::utils::{create_buffered_writer, get_field_value};

/// Optimized CSV exporter with incremental field discovery
pub struct CsvOptimizer {
    config: PerformanceConfig,
}

impl CsvOptimizer {
    pub fn new(config: PerformanceConfig) -> Self {
        Self { config }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn export_csv_streaming(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        exported_count: Arc<AtomicU64>,
        find_options: Option<mongodb::options::FindOptions>,
        fields: Option<Vec<String>>,
    ) -> Result<()> {
        let writer: Box<dyn Write + Send> =
            create_buffered_writer(output_path, compression, self.config.write_buffer_size)?;

        let fields = if let Some(fields) = fields {
            fields
        } else {
            // Phase 1: Incremental field discovery with streaming
            let field_discoverer = FieldDiscoverer::new(&self.config);
            let fields = field_discoverer
                .discover_fields_streaming(collection, filter, find_options.as_ref())
                .await?;

            println!(
                "📋 Discovered {} unique fields across documents",
                fields.len()
            );
            fields
        };

        // Phase 2: Streaming CSV export with discovered fields
        let csv_streamer = CsvStreamer::new(&self.config);
        csv_streamer
            .stream_to_csv(
                collection,
                filter,
                writer,
                fields,
                exported_count,
                find_options,
            )
            .await?;

        Ok(())
    }
}

/// Handles incremental field discovery without loading all documents into memory
struct FieldDiscoverer {
    config: PerformanceConfig,
}

impl FieldDiscoverer {
    fn new(config: &PerformanceConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    async fn discover_fields_streaming(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        find_options: Option<&mongodb::options::FindOptions>,
    ) -> Result<Vec<String>> {
        // Use the centralized CSV field discovery utility
        crate::utils::discover_csv_fields(
            collection,
            filter,
            self.config.csv_field_sample_size,
            find_options,
        )
        .await
    }
}

/// Handles streaming CSV export with known fields
struct CsvStreamer {
    config: PerformanceConfig,
}

impl CsvStreamer {
    fn new(config: &PerformanceConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    async fn stream_to_csv(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        writer: Box<dyn Write + Send>,
        fields: Vec<String>,
        exported_count: Arc<AtomicU64>,
        find_options: Option<mongodb::options::FindOptions>,
    ) -> Result<()> {
        let mut csv_writer = Writer::from_writer(writer);

        // Write header
        csv_writer
            .write_record(&fields)
            .context("Failed to write CSV header")?;

        // Create fresh cursor for data export
        let mut cursor = collection
            .find(filter.clone(), find_options)
            .await
            .context("Failed to execute query for CSV export")?;

        let mut document_buffer = Vec::with_capacity(self.config.document_batch_size);
        let mut total_exported = 0u64;

        // Stream and batch process documents
        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    document_buffer.push(document);

                    // Process batch when full
                    if document_buffer.len() >= self.config.document_batch_size {
                        let batch_size = document_buffer.len();
                        let rows = Self::process_document_batch(&document_buffer, &fields).await?;

                        // Write batch to CSV
                        for row in rows {
                            csv_writer
                                .write_record(&row)
                                .context("Failed to write CSV row")?;
                        }

                        total_exported += batch_size as u64;
                        exported_count.store(total_exported, Ordering::Relaxed);

                        // Clear buffer and flush periodically
                        document_buffer.clear();
                        if total_exported.is_multiple_of(self.config.document_batch_size as u64 * 5)
                        {
                            csv_writer.flush().context("Failed to flush CSV writer")?;
                        }
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Process remaining documents
        if !document_buffer.is_empty() {
            let rows = Self::process_document_batch(&document_buffer, &fields).await?;
            for row in rows {
                csv_writer
                    .write_record(&row)
                    .context("Failed to write CSV row")?;
            }
            total_exported += document_buffer.len() as u64;
            exported_count.store(total_exported, Ordering::Relaxed);
        }

        csv_writer
            .flush()
            .context("Failed to flush final CSV data")?;
        Ok(())
    }

    async fn process_document_batch(
        documents: &[Document],
        fields: &[String],
    ) -> Result<Vec<Vec<String>>> {
        if documents.len() < 100 {
            Ok(documents
                .iter()
                .map(|doc| {
                    fields
                        .iter()
                        .map(|field| get_field_value(doc, field))
                        .collect()
                })
                .collect())
        } else {
            // Run rayon on a blocking-pool thread so it doesn't starve tokio workers.
            let documents = documents.to_vec();
            let fields = fields.to_vec();
            tokio::task::spawn_blocking(move || {
                use rayon::prelude::*;
                documents
                    .par_iter()
                    .map(|doc| {
                        fields
                            .iter()
                            .map(|field| get_field_value(doc, field))
                            .collect()
                    })
                    .collect()
            })
            .await
            .context("Parallel CSV row construction task panicked")
        }
    }
}
