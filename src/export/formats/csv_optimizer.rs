use anyhow::{Context, Result};
use csv::Writer;
use flate2::{write::GzEncoder, Compression};
use futures::stream::StreamExt;
use mongodb::{bson::Document, Collection};
use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::config::PerformanceConfig;
use crate::types::CompressionType;
use crate::utils::get_field_value;

/// Optimized CSV exporter with incremental field discovery
pub struct CsvOptimizer {
    config: PerformanceConfig,
}

impl CsvOptimizer {
    pub fn new(config: PerformanceConfig) -> Self {
        Self { config }
    }

    pub async fn export_csv_streaming(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        exported_count: Arc<AtomicU64>,
    ) -> Result<()> {
        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer: Box<dyn Write> = match compression {
            CompressionType::None => Box::new(BufWriter::with_capacity(
                self.config.write_buffer_size,
                file,
            )),
            CompressionType::Gzip => {
                let gz_encoder = GzEncoder::new(file, Compression::default());
                Box::new(BufWriter::with_capacity(
                    self.config.write_buffer_size,
                    gz_encoder,
                ))
            }
        };

        // Phase 1: Incremental field discovery with streaming
        let field_discoverer = FieldDiscoverer::new(&self.config);
        let fields = field_discoverer
            .discover_fields_streaming(collection, filter)
            .await?;

        println!(
            "📋 Discovered {} unique fields across documents",
            fields.len()
        );

        // Phase 2: Streaming CSV export with discovered fields
        let csv_streamer = CsvStreamer::new(&self.config);
        csv_streamer
            .stream_to_csv(collection, filter, writer, fields, exported_count)
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
    ) -> Result<Vec<String>> {
        // Use the centralized CSV field discovery utility
        crate::utils::discover_csv_fields(
            collection,
            filter,
            self.config.csv_field_sample_size,
            None,
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
        writer: Box<dyn Write>,
        fields: Vec<String>,
        exported_count: Arc<AtomicU64>,
    ) -> Result<()> {
        let mut csv_writer = Writer::from_writer(writer);

        // Write header
        csv_writer
            .write_record(&fields)
            .context("Failed to write CSV header")?;

        // Create fresh cursor for data export
        let mut cursor = collection
            .find(filter.clone(), None)
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
                        let rows = self.process_document_batch(&document_buffer, &fields)?;

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
                        if total_exported % (self.config.document_batch_size as u64 * 5) == 0 {
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
            let rows = self.process_document_batch(&document_buffer, &fields)?;
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

    fn process_document_batch(
        &self,
        documents: &[Document],
        fields: &[String],
    ) -> Result<Vec<Vec<String>>> {
        if documents.len() < 100 {
            // Process sequentially for small batches
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
            // Process in parallel for large batches
            use rayon::prelude::*;
            Ok(documents
                .par_iter()
                .map(|doc| {
                    fields
                        .iter()
                        .map(|field| get_field_value(doc, field))
                        .collect()
                })
                .collect())
        }
    }
}
