pub mod manager;

pub use manager::*;

/// Performance configuration for the export operations
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Buffer size for writers (default: 256KB)
    pub write_buffer_size: usize,
    /// String buffer size for batching (default: 64KB)
    pub string_buffer_size: usize,
    /// Batch flush threshold (default: 32KB)
    pub batch_flush_threshold: usize,
    /// Document batch size for processing (default: 10000)
    pub document_batch_size: usize,
    /// CSV field discovery sample size (default: 1000)
    pub csv_field_sample_size: usize,
    /// Enable parallel processing
    pub enable_parallel_processing: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 256 * 1024,    // 256KB
            string_buffer_size: 64 * 1024,    // 64KB
            batch_flush_threshold: 32 * 1024, // 32KB
            document_batch_size: 10000,       // Process 10k docs at a time
            csv_field_sample_size: 1000,      // Sample first 1000 docs
            enable_parallel_processing: true, // Enable parallel processing
        }
    }
}

impl PerformanceConfig {
    /// Create a memory-optimized configuration for large datasets
    pub fn memory_optimized() -> Self {
        Self {
            write_buffer_size: 128 * 1024,     // 128KB (smaller)
            string_buffer_size: 32 * 1024,     // 32KB (smaller)
            batch_flush_threshold: 16 * 1024,  // 16KB (more frequent)
            document_batch_size: 5000,         // Smaller batches
            csv_field_sample_size: 500,        // Smaller sample
            enable_parallel_processing: false, // Disable for memory savings
        }
    }

    /// Create a speed-optimized configuration for fast exports
    pub fn speed_optimized() -> Self {
        Self {
            write_buffer_size: 1024 * 1024,   // 1MB
            string_buffer_size: 128 * 1024,   // 128KB
            batch_flush_threshold: 64 * 1024, // 64KB
            document_batch_size: 20000,       // Larger batches
            csv_field_sample_size: 2000,      // Larger sample
            enable_parallel_processing: true, // Enable for speed
        }
    }
}
