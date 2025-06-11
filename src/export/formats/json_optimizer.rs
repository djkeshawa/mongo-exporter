use anyhow::{Context, Result};
use flate2::{write::GzEncoder, Compression};
use futures::stream::StreamExt;
use mongodb::{bson::Document, Collection};
use std::{
    io::{BufWriter, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::mpsc;

use crate::config::PerformanceConfig;
use crate::types::CompressionType;

/// High-performance JSON exporter with parallel processing
pub struct JsonOptimizer {
    config: PerformanceConfig,
}

impl JsonOptimizer {
    pub fn new(config: PerformanceConfig) -> Self {
        Self { config }
    }

    pub async fn export_json_lines_parallel(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        exported_count: Arc<AtomicU64>,
    ) -> Result<()> {
        let start_time = Instant::now();

        // Create writer with optimal buffer size
        let writer = self.create_writer(output_path, compression)?;
        let writer = Arc::new(Mutex::new(writer));

        // Create channels for parallel processing
        let (document_tx, document_rx) = mpsc::channel(self.config.document_batch_size);
        let (json_tx, json_rx) = mpsc::channel(self.config.document_batch_size);

        // Spawn document fetcher task
        let collection_clone = collection.clone();
        let filter_clone = filter.clone();
        let fetch_handle = tokio::spawn(async move {
            Self::fetch_documents(collection_clone, filter_clone, document_tx).await
        });

        // For now, use single worker to avoid receiver cloning issues
        // Spawn worker for JSON serialization
        let worker_handle =
            tokio::spawn(
                async move { Self::json_serialization_worker(0, document_rx, json_tx).await },
            );

        // Spawn writer task
        let writer_clone = writer.clone();
        let config = self.config.clone();
        let exported_count_clone = exported_count.clone();

        let write_handle = tokio::spawn(async move {
            Self::write_json_lines(writer_clone, json_rx, config, exported_count_clone).await
        });

        // Wait for all tasks to complete
        fetch_handle.await??;
        worker_handle.await??;
        write_handle.await??;

        // Final flush
        let mut writer_guard = writer.lock().unwrap();
        writer_guard
            .flush()
            .context("Failed to flush final output")?;
        drop(writer_guard);

        let duration = start_time.elapsed();
        let total_count = exported_count.load(Ordering::Relaxed);
        let docs_per_sec = total_count as f64 / duration.as_secs_f64();

        println!(
            "⚡ Parallel export completed: {} docs in {:.2}s ({:.0} docs/sec)",
            total_count,
            duration.as_secs_f64(),
            docs_per_sec
        );

        Ok(())
    }

    pub async fn export_json_array_parallel(
        &self,
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        compression: &CompressionType,
        exported_count: Arc<AtomicU64>,
    ) -> Result<()> {
        let start_time = Instant::now();

        // For JSON arrays, we need ordered output, so we use a simpler approach
        // with parallel serialization but sequential writing
        let writer = self.create_writer(output_path, compression)?;
        let writer = Arc::new(Mutex::new(writer));

        // Write array start
        {
            let mut writer_guard = writer.lock().unwrap();
            writer_guard
                .write_all(b"[\n")
                .context("Failed to write array start")?;
        }

        let mut cursor = collection
            .find(filter.clone(), None)
            .await
            .context("Failed to execute query")?;

        let mut document_buffer = Vec::with_capacity(self.config.document_batch_size);
        let mut is_first_batch = true;
        let mut total_exported = 0u64;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    document_buffer.push(document);

                    if document_buffer.len() >= self.config.document_batch_size {
                        self.process_json_array_batch(&document_buffer, &writer, !is_first_batch)
                            .await?;

                        total_exported += document_buffer.len() as u64;
                        exported_count.store(total_exported, Ordering::Relaxed);

                        document_buffer.clear();
                        is_first_batch = false;
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        // Process remaining documents
        if !document_buffer.is_empty() {
            self.process_json_array_batch(&document_buffer, &writer, !is_first_batch)
                .await?;
            total_exported += document_buffer.len() as u64;
            exported_count.store(total_exported, Ordering::Relaxed);
        }

        // Write array end
        {
            let mut writer_guard = writer.lock().unwrap();
            writer_guard
                .write_all(b"\n]\n")
                .context("Failed to write array end")?;
            writer_guard
                .flush()
                .context("Failed to flush final output")?;
        }

        let duration = start_time.elapsed();
        let docs_per_sec = total_exported as f64 / duration.as_secs_f64();

        println!(
            "⚡ Parallel array export completed: {} docs in {:.2}s ({:.0} docs/sec)",
            total_exported,
            duration.as_secs_f64(),
            docs_per_sec
        );

        Ok(())
    }

    fn create_writer(
        &self,
        output_path: &str,
        compression: &CompressionType,
    ) -> Result<Box<dyn Write + Send>> {
        use std::fs::File;

        let file = File::create(output_path)
            .with_context(|| format!("Failed to create output file: {}", output_path))?;

        let writer: Box<dyn Write + Send> = match compression {
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

        Ok(writer)
    }

    async fn fetch_documents(
        collection: Collection<Document>,
        filter: Document,
        tx: mpsc::Sender<Document>,
    ) -> Result<()> {
        let mut cursor = collection
            .find(filter, None)
            .await
            .context("Failed to execute query")?;

        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    if tx.send(document).await.is_err() {
                        // Channel closed, workers are done
                        break;
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    async fn json_serialization_worker(
        worker_id: usize,
        mut rx: mpsc::Receiver<Document>,
        tx: mpsc::Sender<String>,
    ) -> Result<()> {
        let mut processed = 0;

        while let Some(document) = rx.recv().await {
            let json_str =
                serde_json::to_string(&document).context("Failed to serialize document to JSON")?;

            // Processing completed

            if tx.send(json_str).await.is_err() {
                // Channel closed, writer is done
                break;
            }

            processed += 1;
        }

        println!("🔧 Worker {} processed {} documents", worker_id, processed);
        Ok(())
    }

    async fn write_json_lines(
        writer: Arc<Mutex<Box<dyn Write + Send>>>,
        mut rx: mpsc::Receiver<String>,
        config: PerformanceConfig,
        exported_count: Arc<AtomicU64>,
    ) -> Result<()> {
        let mut buffer = String::with_capacity(config.string_buffer_size);
        let mut count = 0u64;

        while let Some(json_str) = rx.recv().await {
            buffer.push_str(&json_str);
            buffer.push('\n');
            count += 1;

            // Flush buffer when it gets large
            if buffer.len() > config.batch_flush_threshold {
                {
                    let mut writer_guard = writer.lock().unwrap();
                    writer_guard
                        .write_all(buffer.as_bytes())
                        .context("Failed to write JSON batch")?;
                }
                buffer.clear();
                exported_count.store(count, Ordering::Relaxed);

                // Buffer flushed
            }

            // Periodic progress updates
            if count % 1000 == 0 {
                exported_count.store(count, Ordering::Relaxed);
            }
        }

        // Write remaining buffer
        if !buffer.is_empty() {
            let mut writer_guard = writer.lock().unwrap();
            writer_guard
                .write_all(buffer.as_bytes())
                .context("Failed to write final JSON batch")?;
        }

        exported_count.store(count, Ordering::Relaxed);
        Ok(())
    }

    async fn process_json_array_batch(
        &self,
        documents: &[Document],
        writer: &Arc<Mutex<Box<dyn Write + Send>>>,
        needs_comma_prefix: bool,
    ) -> Result<()> {
        // Parallel serialization
        let json_strings = if documents.len() > 50 {
            use rayon::prelude::*;
            documents
                .par_iter()
                .map(serde_json::to_string_pretty)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("Failed to serialize documents to JSON")?
        } else {
            documents
                .iter()
                .map(serde_json::to_string_pretty)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("Failed to serialize documents to JSON")?
        };

        // Sequential writing for correct JSON array format
        let mut writer_guard = writer.lock().unwrap();

        for (i, json_str) in json_strings.iter().enumerate() {
            if needs_comma_prefix || i > 0 {
                writer_guard
                    .write_all(b",\n")
                    .context("Failed to write comma")?;
            }

            // Indent each line
            for line in json_str.lines() {
                writer_guard
                    .write_all(b"  ")
                    .context("Failed to write indent")?;
                writer_guard
                    .write_all(line.as_bytes())
                    .context("Failed to write line")?;
                writer_guard
                    .write_all(b"\n")
                    .context("Failed to write newline")?;
            }
        }

        Ok(())
    }
}
