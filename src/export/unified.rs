use anyhow::{Context, Result};
use console::style;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{bson::Document, options::FindOptions, Collection};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::config::PerformanceConfig;
use crate::export::enterprise::enhanced::EnhancedEnterpriseExporter;
use crate::export::enterprise::export::{
    EnterpriseExporter, ExportOptions as EnterpriseOptions, ExportStats,
};
use crate::export::formats::csv_optimizer::CsvOptimizer;
use crate::export::formats::json_optimizer::JsonOptimizer;
use crate::export::resumable::ResumableExportManager;
use crate::types::{CompressionType, ExportFormat};
use crate::utils::error_handling::ErrorHandlingConfig;

/// Unified export options combining all features
#[derive(Debug, Clone)]
pub struct UnifiedExportOptions {
    // Core options (always available)
    pub uri: String,
    pub filter: Document,
    pub output_path: String,
    pub format: ExportFormat,
    pub compression: CompressionType,

    // Advanced options (intelligently activated)
    pub fields: Option<Vec<String>>,
    pub limit: Option<u64>,
    pub skip: Option<u64>,
    pub sort: Option<Document>,

    // Performance tuning (auto-configured)
    pub batch_size: Option<usize>,
    pub buffer_size: Option<usize>,

    // Feature flags (smart defaults)
    pub force_resumable: Option<bool>,
    pub collect_stats: bool,
    pub validate_fields: Option<bool>,

    // Resume session ID for continuing interrupted exports
    pub resume_session_id: Option<String>,

    // Optional pre-computed document count to avoid an extra MongoDB round-trip
    pub total_count_hint: Option<u64>,
}

impl Default for UnifiedExportOptions {
    fn default() -> Self {
        Self {
            uri: String::new(),
            filter: Document::new(),
            output_path: String::new(),
            format: ExportFormat::JsonLines,
            compression: CompressionType::None,
            fields: None,
            limit: None,
            skip: None,
            sort: None,
            batch_size: None,
            buffer_size: None,
            force_resumable: None,
            collect_stats: true,
            validate_fields: None,
            resume_session_id: None,
            total_count_hint: None,
        }
    }
}

/// Export profile for intelligent feature configuration
#[derive(Debug)]
struct ExportProfile {
    document_count: u64,
    avg_document_size: usize,
    estimated_duration: Duration,
    available_memory: usize,
    cpu_cores: usize,
    format_requirements: FormatRequirements,
    optimal_strategy: ExportStrategy,
}

#[derive(Debug)]
struct FormatRequirements {
    needs_field_discovery: bool,
    supports_streaming: bool,
    supports_parallel: bool,
}

#[derive(Debug, Clone, Copy)]
enum ExportStrategy {
    FastStream, // Direct streaming for small exports
    Parallel,   // Parallel processing for medium exports
    Resumable,  // Checkpoint-based for large exports
}

/// Unified exporter combining the best of all modes.
///
/// `error_config` is accepted for forward compatibility but not currently consumed here —
/// the retry/circuit-breaker plumbing lives at the connection layer (see
/// `database::connect_to_mongodb_with_retry`) and at format-level error reporting via
/// per-document error vectors in `ExportStats`.
pub struct UnifiedExporter {
    performance_config: PerformanceConfig,
    resume_manager: ResumableExportManager,
    collection: Collection<Document>,
}

impl UnifiedExporter {
    pub fn new(
        collection: Collection<Document>,
        performance_config: Option<PerformanceConfig>,
        _error_config: Option<ErrorHandlingConfig>,
    ) -> Result<Self> {
        let performance_config = performance_config.unwrap_or_default();
        let resume_manager = ResumableExportManager::new(None)?;

        Ok(Self {
            performance_config,
            resume_manager,
            collection,
        })
    }

    /// Main export entry point with intelligent feature detection
    pub async fn export(&self, options: UnifiedExportOptions) -> Result<ExportStats> {
        let start_time = Instant::now();

        // Check for resume session
        if let Some(session_id) = &options.resume_session_id {
            return self.resume_export(session_id, options.clone()).await;
        }

        // Analyze export requirements
        let profile = self.analyze_export_profile(&options).await?;

        // Show export plan to user
        self.display_export_plan(&profile, &options);

        // Configure features based on profile
        let features = self.configure_features(&profile, &options);

        // Create progress tracking
        let progress = Arc::new(AtomicU64::new(0));
        let progress_bar = self.create_progress_bar(profile.document_count, &profile);

        // Start progress update task. Wrap the join handle in a guard so the task is aborted
        // on every exit path (including `?` propagation), not only on the success branch.
        let pb_clone = progress_bar.clone();
        let progress_clone = progress.clone();
        let progress_task = tokio::spawn(async move {
            loop {
                pb_clone.set_position(progress_clone.load(Ordering::Relaxed));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        struct AbortOnDrop(tokio::task::JoinHandle<()>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _progress_guard = AbortOnDrop(progress_task);

        // Execute export with optimal strategy.
        // FastStream and Parallel share the same underlying pipeline today — Parallel only
        // changes the announcement message, so we route both to `export_fast_stream` and
        // mention parallelism in the message printed inside that function via `features`.
        let stats = match profile.optimal_strategy {
            ExportStrategy::FastStream | ExportStrategy::Parallel => {
                self.export_fast_stream(&options, &features, progress.clone())
                    .await?
            }
            ExportStrategy::Resumable => {
                self.export_with_checkpoints(&options, &features, progress.clone())
                    .await?
            }
        };

        // Finish progress bar with success message
        progress_bar.finish_with_message(format!(
            "{} Successfully exported {} documents to {} in {:.2}s",
            style("✅").green(),
            style(stats.documents_exported).bold(),
            style(&options.output_path).bold(),
            start_time.elapsed().as_secs_f64()
        ));

        // Display statistics if collected
        if options.collect_stats && stats.documents_processed > 0 {
            self.display_export_stats(&stats);
        }

        Ok(stats)
    }

    /// Analyze export requirements to determine optimal configuration
    async fn analyze_export_profile(
        &self,
        options: &UnifiedExportOptions,
    ) -> Result<ExportProfile> {
        // Get document count, preferring the pre-computed hint to avoid a second round-trip
        let raw_count = match options.total_count_hint {
            Some(c) => c,
            None => self
                .collection
                .count_documents(options.filter.clone(), None)
                .await
                .context("Failed to count documents")?,
        };
        let document_count = match options.limit {
            Some(limit) => limit.min(raw_count),
            None => raw_count,
        };

        // Sample collection for size estimation
        let sample_size = 100.min(document_count);
        let avg_document_size = if sample_size > 0 {
            let sample_docs = self
                .sample_collection(&options.filter, sample_size as usize)
                .await?;
            self.calculate_avg_document_size(&sample_docs)
        } else {
            1024 // Default 1KB
        };

        // Get system resources
        let cpu_cores = num_cpus::get();
        let available_memory = self.get_available_memory();

        // Determine format requirements
        let format_requirements = self.get_format_requirements(&options.format);

        // Estimate export duration
        let estimated_duration =
            self.estimate_duration(document_count, avg_document_size, &options.format);

        // Determine optimal strategy
        let optimal_strategy = self.determine_strategy(
            document_count,
            estimated_duration,
            &format_requirements,
            options.force_resumable,
            available_memory,
        );

        Ok(ExportProfile {
            document_count,
            avg_document_size,
            estimated_duration,
            available_memory,
            cpu_cores,
            format_requirements,
            optimal_strategy,
        })
    }

    /// Sample collection for analysis
    async fn sample_collection(
        &self,
        filter: &Document,
        sample_size: usize,
    ) -> Result<Vec<Document>> {
        let options = FindOptions::builder()
            .limit(Some(sample_size as i64))
            .build();

        let mut cursor = self
            .collection
            .find(filter.clone(), options)
            .await
            .context("Failed to create cursor for sampling")?;

        let mut samples = Vec::with_capacity(sample_size);
        while let Some(result) = cursor.next().await {
            samples.push(result?);
        }

        Ok(samples)
    }

    /// Calculate average document size from samples
    fn calculate_avg_document_size(&self, samples: &[Document]) -> usize {
        if samples.is_empty() {
            return 1024; // Default 1KB
        }

        let total_size: usize = samples
            .iter()
            .map(|doc| mongodb::bson::to_vec(doc).unwrap_or_default().len())
            .sum();

        total_size / samples.len()
    }

    /// Get available system memory
    fn get_available_memory(&self) -> usize {
        // Try to detect system memory, fall back to default if detection fails
        self.detect_system_memory()
            .unwrap_or(4 * 1024 * 1024 * 1024)
    }

    /// Detect system memory using platform-specific methods
    fn detect_system_memory(&self) -> Option<usize> {
        #[cfg(target_os = "linux")]
        {
            self.get_linux_memory()
        }
        #[cfg(target_os = "macos")]
        {
            self.get_macos_memory()
        }
        #[cfg(target_os = "windows")]
        {
            self.get_windows_memory()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            None
        }
    }

    #[cfg(target_os = "linux")]
    fn get_linux_memory(&self) -> Option<usize> {
        use std::fs;

        // Read /proc/meminfo for MemAvailable or MemFree + Buffers + Cached
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;

        let mut mem_available = None;
        let mut mem_free = None;
        let mut buffers = None;
        let mut cached = None;

        for line in meminfo.lines() {
            if let Some(value) = line.strip_prefix("MemAvailable:") {
                if let Some(kb) = Self::parse_meminfo_value(value) {
                    mem_available = Some(kb * 1024); // Convert KB to bytes
                }
            } else if let Some(value) = line.strip_prefix("MemFree:") {
                if let Some(kb) = Self::parse_meminfo_value(value) {
                    mem_free = Some(kb * 1024);
                }
            } else if let Some(value) = line.strip_prefix("Buffers:") {
                if let Some(kb) = Self::parse_meminfo_value(value) {
                    buffers = Some(kb * 1024);
                }
            } else if let Some(value) = line.strip_prefix("Cached:") {
                if let Some(kb) = Self::parse_meminfo_value(value) {
                    cached = Some(kb * 1024);
                }
            }
        }

        // Prefer MemAvailable if available, otherwise estimate
        mem_available.or_else(|| match (mem_free, buffers, cached) {
            (Some(free), Some(buf), Some(cache)) => Some(free + buf + cache),
            _ => None,
        })
    }

    #[cfg(target_os = "macos")]
    fn get_macos_memory(&self) -> Option<usize> {
        use std::process::Command;

        // Use vm_stat command to get memory info
        let output = Command::new("vm_stat").output().ok()?;
        let stdout = String::from_utf8_lossy(&output.stdout);

        let mut page_size = 4096; // Default page size
        let mut free_pages = 0;
        let mut inactive_pages = 0;

        for line in stdout.lines() {
            if line.contains("page size of") {
                if let Some(size_str) = line.split_whitespace().nth(7) {
                    page_size = size_str.parse().unwrap_or(4096);
                }
            } else if line.starts_with("Pages free:") {
                if let Some(pages_str) = line.split_whitespace().nth(2) {
                    free_pages = pages_str.trim_end_matches('.').parse().unwrap_or(0);
                }
            } else if line.starts_with("Pages inactive:") {
                if let Some(pages_str) = line.split_whitespace().nth(2) {
                    inactive_pages = pages_str.trim_end_matches('.').parse().unwrap_or(0);
                }
            }
        }

        Some((free_pages + inactive_pages) * page_size)
    }

    #[cfg(target_os = "windows")]
    fn get_windows_memory(&self) -> Option<usize> {
        use std::process::Command;

        // Use wmic command to get available memory
        let output = Command::new("wmic")
            .args(&["OS", "get", "FreePhysicalMemory", "/value"])
            .output()
            .ok()?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        for line in stdout.lines() {
            if line.starts_with("FreePhysicalMemory=") {
                if let Some(kb_str) = line.split('=').nth(1) {
                    if let Ok(kb) = kb_str.trim().parse::<usize>() {
                        return Some(kb * 1024); // Convert KB to bytes
                    }
                }
            }
        }

        None
    }

    fn parse_meminfo_value(value: &str) -> Option<usize> {
        value.split_whitespace().next()?.parse().ok()
    }

    /// Get format-specific requirements
    fn get_format_requirements(&self, format: &ExportFormat) -> FormatRequirements {
        match format {
            ExportFormat::JsonLines => FormatRequirements {
                needs_field_discovery: false,
                supports_streaming: true,
                supports_parallel: true,
            },
            ExportFormat::JsonArray => FormatRequirements {
                needs_field_discovery: false,
                supports_streaming: true,
                supports_parallel: true,
            },
            ExportFormat::Csv => FormatRequirements {
                needs_field_discovery: true,
                supports_streaming: true,
                supports_parallel: true,
            },
            ExportFormat::Parquet => FormatRequirements {
                needs_field_discovery: true,
                supports_streaming: false,
                supports_parallel: false,
            },
            ExportFormat::Bson => FormatRequirements {
                needs_field_discovery: false,
                supports_streaming: true,
                supports_parallel: false,
            },
        }
    }

    /// Estimate export duration based on parameters
    fn estimate_duration(
        &self,
        document_count: u64,
        avg_doc_size: usize,
        format: &ExportFormat,
    ) -> Duration {
        // Rough estimation based on format and size
        let base_rate = match format {
            ExportFormat::JsonLines => 50_000.0, // docs/sec
            ExportFormat::JsonArray => 40_000.0,
            ExportFormat::Csv => 30_000.0,
            ExportFormat::Parquet => 20_000.0,
            ExportFormat::Bson => 60_000.0,
        };

        // Adjust for document size
        let size_factor = 1024.0 / avg_doc_size as f64;
        let adjusted_rate = base_rate * size_factor.sqrt();

        let seconds = document_count as f64 / adjusted_rate;
        Duration::from_secs_f64(seconds)
    }

    /// Determine optimal export strategy
    fn determine_strategy(
        &self,
        document_count: u64,
        estimated_duration: Duration,
        format_reqs: &FormatRequirements,
        force_resumable: Option<bool>,
        available_memory: usize,
    ) -> ExportStrategy {
        // Force resumable if requested
        if force_resumable == Some(true) {
            return ExportStrategy::Resumable;
        }

        // Use resumable for large exports or limited memory
        if document_count > 1_000_000
            || estimated_duration > Duration::from_secs(300)
            || available_memory < 2 * 1024 * 1024 * 1024
        {
            // Less than 2GB
            return ExportStrategy::Resumable;
        }

        // Use parallel for medium exports if supported and streaming capable
        if document_count > 10_000
            && format_reqs.supports_parallel
            && format_reqs.supports_streaming
        {
            return ExportStrategy::Parallel;
        }

        // Default to fast streaming
        ExportStrategy::FastStream
    }

    /// Configure features based on export profile
    fn configure_features(
        &self,
        profile: &ExportProfile,
        options: &UnifiedExportOptions,
    ) -> ExportFeatures {
        ExportFeatures {
            enable_resumable: matches!(profile.optimal_strategy, ExportStrategy::Resumable),
            enable_parallel: matches!(profile.optimal_strategy, ExportStrategy::Parallel)
                && profile.cpu_cores > 2,
            enable_validation: options.validate_fields.unwrap_or(
                profile.format_requirements.needs_field_discovery || options.fields.is_some(),
            ),
            enable_compression: !matches!(options.compression, CompressionType::None),
            batch_size: options
                .batch_size
                .unwrap_or_else(|| self.calculate_optimal_batch_size(profile)),
            buffer_size: options
                .buffer_size
                .unwrap_or(self.performance_config.write_buffer_size),
        }
    }

    /// Calculate optimal batch size based on profile
    fn calculate_optimal_batch_size(&self, profile: &ExportProfile) -> usize {
        let base_batch = match profile.optimal_strategy {
            ExportStrategy::FastStream => 10_000,
            ExportStrategy::Parallel => 5_000,
            ExportStrategy::Resumable => 1_000,
        };

        // Adjust for document size
        let size_factor = 1024.0 / profile.avg_document_size as f64;
        (base_batch as f64 * size_factor.sqrt()) as usize
    }

    /// Create progress bar with contextual information
    fn create_progress_bar(&self, total: u64, profile: &ExportProfile) -> ProgressBar {
        let pb = ProgressBar::new(total);

        let template = match profile.optimal_strategy {
            ExportStrategy::FastStream => {
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({rate}/s) [{eta}]"
            }
            ExportStrategy::Parallel => {
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({rate}/s) [{eta}] Parallel: {msg}"
            }
            ExportStrategy::Resumable => {
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({rate}/s) [{eta}] {msg}"
            }
        };

        pb.set_style(
            ProgressStyle::default_bar()
                .template(template)
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );

        // Set initial message based on strategy
        match profile.optimal_strategy {
            ExportStrategy::Parallel => {
                pb.set_message(format!("{} threads", profile.cpu_cores.min(8)));
            }
            ExportStrategy::Resumable => {
                pb.set_message("Checkpoint enabled");
            }
            _ => {}
        }

        pb
    }

    /// Display export plan to user
    fn display_export_plan(&self, profile: &ExportProfile, options: &UnifiedExportOptions) {
        println!();
        println!("{}", style("Optimizing export strategy...").dim());
        println!(
            "  Processing {} documents (~{}) - estimated time: {:.1}s",
            style(profile.document_count.to_string()).white(),
            humansize::format_size(
                profile.document_count * profile.avg_document_size as u64,
                humansize::BINARY
            ),
            profile.estimated_duration.as_secs_f64()
        );

        let strategy_desc = match profile.optimal_strategy {
            ExportStrategy::FastStream => "Fast streaming".to_string(),
            ExportStrategy::Parallel => format!("Parallel ({} threads)", profile.cpu_cores.min(8)),
            ExportStrategy::Resumable => "Resumable with checkpoints".to_string(),
        };

        println!(
            "  {} Strategy: {} (memory: {})",
            style("⚙️").cyan(),
            style(strategy_desc).bold(),
            humansize::format_size(profile.available_memory as u64, humansize::BINARY)
        );

        if profile.format_requirements.needs_field_discovery {
            println!(
                "  {} Features: Field discovery enabled for schema inference",
                style("🔍").cyan()
            );
        }

        if let Some(limit) = options.limit {
            println!(
                "  {} Limit: {} documents",
                style("📌").cyan(),
                style(limit.to_string()).bold()
            );
        }

        println!();
    }

    /// Display export statistics
    fn display_export_stats(&self, stats: &ExportStats) {
        println!();
        println!("{} Export Statistics", style("📊").cyan().bold());
        println!("┌{}┐", "─".repeat(56));
        println!(
            "│ {:<30} {:>22} │",
            "Documents processed:", stats.documents_processed
        );
        println!(
            "│ {:<30} {:>22} │",
            "Documents exported:", stats.documents_exported
        );

        if stats.fields_discovered > 0 {
            println!(
                "│ {:<30} {:>22} │",
                "Fields discovered:", stats.fields_discovered
            );
        }

        println!(
            "│ {:<30} {:>22} │",
            "Bytes written:",
            humansize::format_size(stats.bytes_written, humansize::BINARY)
        );

        println!(
            "│ {:<30} {:>22} │",
            "Processing time:",
            format!("{:.2}s", stats.processing_time_ms as f64 / 1000.0)
        );

        let throughput = if stats.processing_time_ms > 0 {
            (stats.documents_exported as f64 / (stats.processing_time_ms as f64 / 1000.0)) as u64
        } else {
            0
        };

        println!(
            "│ {:<30} {:>22} │",
            "Throughput:",
            format!("{} /s", throughput)
        );

        if !stats.errors.is_empty() {
            println!(
                "│ {:<30} {:>22} │",
                "Errors encountered:",
                stats.errors.len()
            );
        }

        println!("└{}┘", "─".repeat(56));
    }

    /// Export using fast streaming (small datasets)
    async fn export_fast_stream(
        &self,
        options: &UnifiedExportOptions,
        features: &ExportFeatures,
        progress: Arc<AtomicU64>,
    ) -> Result<ExportStats> {
        if features.enable_parallel {
            // Don't claim N worker threads: JSONL serialization is single-worker today, only
            // CSV and JSON Array actually fan out via rayon. What's accurate is the pipelined
            // fetch/serialize/write topology. Keep the message honest until JSONL parallelism
            // is finished.
            println!(
                "{} Pipelined mode: batch_size={}, buffer_size={}KB",
                style("◦").dim(),
                features.batch_size,
                features.buffer_size / 1024,
            );
        } else {
            println!(
                "{} Fast streaming mode: batch_size={}, buffer_size={}KB",
                style("◦").dim(),
                features.batch_size,
                features.buffer_size / 1024
            );
        }

        let start_time = std::time::Instant::now();

        // Build FindOptions from UnifiedExportOptions
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
        if let Some(ref fields) = options.fields {
            let mut projection = Document::new();
            for field in fields {
                projection.insert(field, 1);
            }
            find_options.projection = Some(projection);
        }
        find_options.batch_size = Some(features.batch_size as u32);
        // Disable the server-side 10-minute idle cursor timeout: large exports trivially exceed
        // it, and getting a CursorNotFound mid-stream leaves the output file inconsistent.
        find_options.no_cursor_timeout = Some(true);

        // Use optimized exporters from basic mode
        match options.format {
            ExportFormat::JsonLines => {
                let optimizer = JsonOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_json_lines_parallel(
                        &self.collection,
                        &options.filter,
                        &options.output_path,
                        &options.compression,
                        progress.clone(),
                        Some(find_options),
                    )
                    .await?;
            }
            ExportFormat::JsonArray => {
                let optimizer = JsonOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_json_array_parallel(
                        &self.collection,
                        &options.filter,
                        &options.output_path,
                        &options.compression,
                        progress.clone(),
                        Some(find_options),
                    )
                    .await?;
            }
            ExportFormat::Csv => {
                let optimizer = CsvOptimizer::new(self.performance_config.clone());
                optimizer
                    .export_csv_streaming(
                        &self.collection,
                        &options.filter,
                        &options.output_path,
                        &options.compression,
                        progress.clone(),
                        Some(find_options),
                        options.fields.clone(),
                    )
                    .await?;
            }
            _ => {
                // Fall back to enterprise exporter for other formats
                return self
                    .export_with_enterprise(options, features, progress)
                    .await;
            }
        }

        // Calculate processing time
        let processing_time_ms = start_time.elapsed().as_millis() as u64;

        // Get file size for bytes_written
        let bytes_written = std::fs::metadata(&options.output_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        // Create stats with actual values
        let exported = progress.load(Ordering::Relaxed);
        Ok(ExportStats {
            documents_processed: exported,
            documents_exported: exported,
            bytes_written,
            processing_time_ms,
            ..Default::default()
        })
    }

    /// Export with checkpointing (large datasets)
    async fn export_with_checkpoints(
        &self,
        options: &UnifiedExportOptions,
        features: &ExportFeatures,
        progress: Arc<AtomicU64>,
    ) -> Result<ExportStats> {
        // Show resumable configuration
        if features.enable_resumable {
            println!(
                "{} Resumable mode: checkpointing enabled, batch_size={}",
                style("◦").dim(),
                features.batch_size
            );
        }
        if features.enable_validation {
            println!("{} Field validation enabled", style("◦").dim());
        }
        if features.enable_compression {
            println!("{} Compression enabled", style("◦").dim());
        }
        // Use enhanced enterprise exporter for resumable exports
        let enhanced_exporter = EnhancedEnterpriseExporter::new(
            self.performance_config.clone(),
            Some(ErrorHandlingConfig::default()),
            None,
        )?;

        // Convert to enterprise options
        let enterprise_options = self.convert_to_enterprise_options(options);

        enhanced_exporter
            .export_with_enterprise_features(
                &self.collection,
                &options.filter,
                &options.output_path,
                &options.format,
                &options.compression,
                &enterprise_options,
                progress,
                None, // No resume session for new export
                &options.uri,
            )
            .await
    }

    /// Export using enterprise exporter
    async fn export_with_enterprise(
        &self,
        options: &UnifiedExportOptions,
        _features: &ExportFeatures,
        progress: Arc<AtomicU64>,
    ) -> Result<ExportStats> {
        let enterprise_exporter = EnterpriseExporter::new(self.performance_config.clone());
        let enterprise_options = self.convert_to_enterprise_options(options);

        enterprise_exporter
            .export_with_options(
                &self.collection,
                &options.filter,
                &options.output_path,
                &options.format,
                &options.compression,
                &enterprise_options,
                progress,
            )
            .await
    }

    /// Resume an interrupted export
    async fn resume_export(
        &self,
        session_id: &str,
        options: UnifiedExportOptions,
    ) -> Result<ExportStats> {
        println!(
            "{} Resuming export session: {}",
            style("◦").dim(),
            session_id
        );

        // Verify the session exists before handing off — gives a clearer error than a deeper failure.
        match self.resume_manager.load_session(session_id) {
            Ok(Some(_)) => {}
            Ok(None) => anyhow::bail!("Resume session '{}' not found", session_id),
            Err(e) => return Err(e.context("Failed to load resume session")),
        }

        let enhanced_exporter = EnhancedEnterpriseExporter::new(
            self.performance_config.clone(),
            Some(ErrorHandlingConfig::default()),
            None,
        )?;

        let progress = Arc::new(AtomicU64::new(0));
        let enterprise_options = self.convert_to_enterprise_options(&options);

        enhanced_exporter
            .export_with_enterprise_features(
                &self.collection,
                &options.filter,
                &options.output_path,
                &options.format,
                &options.compression,
                &enterprise_options,
                progress,
                Some(session_id.to_string()),
                &options.uri,
            )
            .await
    }

    /// Convert unified options to enterprise options
    fn convert_to_enterprise_options(&self, options: &UnifiedExportOptions) -> EnterpriseOptions {
        EnterpriseOptions {
            fields: options.fields.clone(),
            limit: options.limit,
            skip: options.skip,
            sort: options.sort.clone(),
            validate_fields: options.validate_fields.unwrap_or(false),
            collect_stats: options.collect_stats,
        }
    }
}

/// Export features configuration
#[derive(Debug)]
struct ExportFeatures {
    enable_resumable: bool,
    enable_parallel: bool,
    enable_validation: bool,
    enable_compression: bool,
    batch_size: usize,
    buffer_size: usize,
}

// Note: Use UnifiedExporter::new() and .export() directly instead of this function
