use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::time::sleep;

/// Classification of different error types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Network connectivity issues
    Network,
    /// MongoDB authentication/authorization
    Authentication,
    /// MongoDB server errors
    Database,
    /// File system I/O errors
    FileSystem,
    /// Data serialization/format errors
    Serialization,
    /// Memory/resource exhaustion
    Resource,
    /// User input/configuration errors
    Configuration,
    /// Unknown/unexpected errors
    Unknown,
}

/// Severity level of errors
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Hash)]
pub enum ErrorSeverity {
    /// Informational - operation can continue
    Info,
    /// Warning - operation can continue but attention needed
    Warning,
    /// Error - operation failed but can be retried
    Error,
    /// Critical - operation failed and cannot be retried
    Critical,
    /// Fatal - entire export must be aborted
    Fatal,
}

/// Detailed error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetails {
    /// Error category
    pub category: ErrorCategory,
    /// Error severity
    pub severity: ErrorSeverity,
    /// Error message
    pub message: String,
    /// Original error context
    pub context: Option<String>,
    /// Timestamp when error occurred
    pub timestamp: DateTime<Utc>,
    /// Whether this error is retryable
    pub retryable: bool,
    /// Suggested retry delay in seconds
    pub retry_delay: Option<u64>,
    /// Recovery suggestions
    pub recovery_suggestions: Vec<String>,
    /// Related error code if any
    pub error_code: Option<String>,
}

/// Error handling configuration
#[derive(Debug, Clone)]
pub struct ErrorHandlingConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial retry delay in milliseconds
    pub initial_retry_delay: u64,
    /// Maximum retry delay in milliseconds
    pub max_retry_delay: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Jitter factor to add randomness to delays
    pub jitter_factor: f64,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker timeout in seconds
    pub circuit_breaker_timeout: u64,
    /// Whether to collect detailed error statistics
    pub collect_error_stats: bool,
}

impl Default for ErrorHandlingConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_retry_delay: 1000, // 1 second
            max_retry_delay: 60000,    // 60 seconds
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: 300, // 5 minutes
            collect_error_stats: true,
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing, blocking requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker for preventing cascading failures
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    failure_count: Arc<AtomicU32>,
    last_failure_time: Arc<Mutex<Option<Instant>>>,
    config: ErrorHandlingConfig,
}

impl CircuitBreaker {
    pub fn new(config: ErrorHandlingConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            failure_count: Arc::new(AtomicU32::new(0)),
            last_failure_time: Arc::new(Mutex::new(None)),
            config,
        }
    }

    /// Check if operation should be allowed
    pub fn can_execute(&self) -> bool {
        let state = self.state.lock().unwrap().clone();

        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout has passed
                let last_failure = self.last_failure_time.lock().unwrap();
                if let Some(last_time) = *last_failure {
                    if last_time.elapsed().as_secs() >= self.config.circuit_breaker_timeout {
                        // Move to half-open state
                        drop(last_failure);
                        *self.state.lock().unwrap() = CircuitState::HalfOpen;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record successful operation
    pub fn record_success(&self) {
        let mut state = self.state.lock().unwrap();
        self.failure_count.store(0, Ordering::Relaxed);
        *state = CircuitState::Closed;
    }

    /// Record failed operation
    pub fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        *self.last_failure_time.lock().unwrap() = Some(Instant::now());

        if failures >= self.config.circuit_breaker_threshold {
            *self.state.lock().unwrap() = CircuitState::Open;
        }
    }

    /// Get current state
    pub fn get_state(&self) -> CircuitState {
        self.state.lock().unwrap().clone()
    }
}

/// Error statistics collector
#[derive(Debug)]
pub struct ErrorStatistics {
    /// Total number of errors by category
    pub errors_by_category: Arc<Mutex<std::collections::HashMap<ErrorCategory, u64>>>,
    /// Total number of errors by severity
    pub errors_by_severity: Arc<Mutex<std::collections::HashMap<ErrorSeverity, u64>>>,
    /// Total retry attempts
    pub total_retries: Arc<AtomicU64>,
    /// Total recovery successes
    pub recovery_successes: Arc<AtomicU64>,
    /// Error rate (errors per second)
    pub error_rate: Arc<Mutex<f64>>,
    /// Last error timestamp
    pub last_error_time: Arc<Mutex<Option<Instant>>>,
}

impl Default for ErrorStatistics {
    fn default() -> Self {
        Self {
            errors_by_category: Arc::new(Mutex::new(std::collections::HashMap::new())),
            errors_by_severity: Arc::new(Mutex::new(std::collections::HashMap::new())),
            total_retries: Arc::new(AtomicU64::new(0)),
            recovery_successes: Arc::new(AtomicU64::new(0)),
            error_rate: Arc::new(Mutex::new(0.0)),
            last_error_time: Arc::new(Mutex::new(None)),
        }
    }
}

impl ErrorStatistics {
    /// Record an error occurrence
    pub fn record_error(&self, error: &ErrorDetails) {
        // Update category count
        {
            let mut categories = self.errors_by_category.lock().unwrap();
            *categories.entry(error.category.clone()).or_insert(0) += 1;
        }

        // Update severity count
        {
            let mut severities = self.errors_by_severity.lock().unwrap();
            *severities.entry(error.severity.clone()).or_insert(0) += 1;
        }

        // Update error rate
        *self.last_error_time.lock().unwrap() = Some(Instant::now());

        // Calculate simple error rate (errors in last minute)
        // In production, this would use a proper sliding window
        *self.error_rate.lock().unwrap() += 1.0;
    }

    /// Record a retry attempt
    pub fn record_retry(&self) {
        self.total_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful recovery
    pub fn record_recovery(&self) {
        self.recovery_successes.fetch_add(1, Ordering::Relaxed);
    }
}

/// Advanced error handler with retry logic and circuit breaker
pub struct AdvancedErrorHandler {
    config: ErrorHandlingConfig,
    circuit_breaker: CircuitBreaker,
    statistics: ErrorStatistics,
}

impl AdvancedErrorHandler {
    pub fn new(config: ErrorHandlingConfig) -> Self {
        Self {
            circuit_breaker: CircuitBreaker::new(config.clone()),
            statistics: ErrorStatistics::default(),
            config,
        }
    }

    /// Execute operation with advanced error handling
    pub async fn execute_with_retry<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut last_error = None;

        while attempt <= self.config.max_retries {
            // Check circuit breaker
            if !self.circuit_breaker.can_execute() {
                return Err(anyhow::anyhow!(
                    "Circuit breaker is open - too many recent failures"
                ));
            }

            match operation().await {
                Ok(result) => {
                    self.circuit_breaker.record_success();
                    if attempt > 0 {
                        self.statistics.record_recovery();
                    }
                    return Ok(result);
                }
                Err(error) => {
                    let error_details = self.classify_error(&error);

                    if self.config.collect_error_stats {
                        self.statistics.record_error(&error_details);
                    }

                    // Don't retry if error is not retryable
                    if !error_details.retryable || error_details.severity >= ErrorSeverity::Critical
                    {
                        self.circuit_breaker.record_failure();
                        return Err(error);
                    }

                    last_error = Some(error);
                    attempt += 1;

                    if attempt <= self.config.max_retries {
                        self.statistics.record_retry();
                        let delay = self.calculate_retry_delay(attempt);

                        println!(
                            "⚠️  {} (attempt {}/{}), retrying in {}ms...",
                            error_details.message,
                            attempt,
                            self.config.max_retries + 1,
                            delay
                        );

                        sleep(Duration::from_millis(delay)).await;
                    } else {
                        self.circuit_breaker.record_failure();
                    }
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("Operation failed after {} retries", self.config.max_retries)
        }))
    }

    /// Classify error into category and severity
    pub fn classify_error(&self, error: &anyhow::Error) -> ErrorDetails {
        let error_string = error.to_string().to_lowercase();

        let (category, severity, retryable, retry_delay, suggestions) = if error_string
            .contains("connection")
            || error_string.contains("network")
            || error_string.contains("timeout")
        {
            (
                ErrorCategory::Network,
                ErrorSeverity::Error,
                true,
                Some(5),
                vec![
                    "Check network connectivity".to_string(),
                    "Verify MongoDB server is running".to_string(),
                    "Check firewall settings".to_string(),
                ],
            )
        } else if error_string.contains("authentication")
            || error_string.contains("unauthorized")
            || error_string.contains("auth")
        {
            (
                ErrorCategory::Authentication,
                ErrorSeverity::Critical,
                false,
                None,
                vec![
                    "Check MongoDB credentials".to_string(),
                    "Verify user permissions".to_string(),
                    "Check authentication database".to_string(),
                ],
            )
        } else if error_string.contains("permission")
            || error_string.contains("access denied")
            || error_string.contains("forbidden")
        {
            (
                ErrorCategory::Database,
                ErrorSeverity::Critical,
                false,
                None,
                vec![
                    "Check user read permissions".to_string(),
                    "Verify collection access rights".to_string(),
                    "Contact database administrator".to_string(),
                ],
            )
        } else if error_string.contains("file")
            || error_string.contains("disk")
            || error_string.contains("space")
        {
            (
                ErrorCategory::FileSystem,
                ErrorSeverity::Error,
                true,
                Some(10),
                vec![
                    "Check available disk space".to_string(),
                    "Verify file write permissions".to_string(),
                    "Check output directory exists".to_string(),
                ],
            )
        } else if error_string.contains("serialize")
            || error_string.contains("parse")
            || error_string.contains("json")
        {
            (
                ErrorCategory::Serialization,
                ErrorSeverity::Warning,
                true,
                Some(1),
                vec![
                    "Check document structure".to_string(),
                    "Verify field types are supported".to_string(),
                    "Consider field filtering".to_string(),
                ],
            )
        } else if error_string.contains("memory")
            || error_string.contains("oom")
            || error_string.contains("resource")
        {
            (
                ErrorCategory::Resource,
                ErrorSeverity::Error,
                true,
                Some(30),
                vec![
                    "Reduce batch size".to_string(),
                    "Enable memory optimization mode".to_string(),
                    "Close other applications".to_string(),
                ],
            )
        } else {
            (
                ErrorCategory::Unknown,
                ErrorSeverity::Error,
                true,
                Some(5),
                vec![
                    "Check error details".to_string(),
                    "Contact support if issue persists".to_string(),
                ],
            )
        };

        ErrorDetails {
            category,
            severity,
            message: error.to_string(),
            context: error.chain().nth(1).map(|e| e.to_string()),
            timestamp: Utc::now(),
            retryable,
            retry_delay,
            recovery_suggestions: suggestions,
            error_code: None,
        }
    }

    /// Calculate retry delay with exponential backoff and jitter
    fn calculate_retry_delay(&self, attempt: u32) -> u64 {
        let base_delay = self.config.initial_retry_delay as f64;
        let exponential_delay =
            base_delay * self.config.backoff_multiplier.powi(attempt as i32 - 1);

        // Cap at max delay
        let capped_delay = exponential_delay.min(self.config.max_retry_delay as f64);

        // Add jitter to prevent thundering herd
        let jitter = capped_delay * self.config.jitter_factor * (rand::random::<f64>() - 0.5) * 2.0;
        let final_delay = capped_delay + jitter;

        final_delay.max(0.0) as u64
    }

    /// Get error statistics
    pub fn get_statistics(&self) -> ErrorStatisticsReport {
        let categories = self.statistics.errors_by_category.lock().unwrap().clone();
        let severities = self.statistics.errors_by_severity.lock().unwrap().clone();
        let total_retries = self.statistics.total_retries.load(Ordering::Relaxed);
        let recovery_successes = self.statistics.recovery_successes.load(Ordering::Relaxed);
        let error_rate = *self.statistics.error_rate.lock().unwrap();

        ErrorStatisticsReport {
            errors_by_category: categories,
            errors_by_severity: severities,
            total_retries,
            recovery_successes,
            error_rate,
            circuit_breaker_state: self.circuit_breaker.get_state(),
        }
    }

    /// Reset statistics
    #[allow(dead_code)]
    pub fn reset_statistics(&self) {
        self.statistics.errors_by_category.lock().unwrap().clear();
        self.statistics.errors_by_severity.lock().unwrap().clear();
        self.statistics.total_retries.store(0, Ordering::Relaxed);
        self.statistics
            .recovery_successes
            .store(0, Ordering::Relaxed);
        *self.statistics.error_rate.lock().unwrap() = 0.0;
    }
}

/// Error statistics report
#[derive(Debug)]
pub struct ErrorStatisticsReport {
    pub errors_by_category: std::collections::HashMap<ErrorCategory, u64>,
    pub errors_by_severity: std::collections::HashMap<ErrorSeverity, u64>,
    pub total_retries: u64,
    pub recovery_successes: u64,
    #[allow(dead_code)]
    pub error_rate: f64,
    pub circuit_breaker_state: CircuitState,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Network => write!(f, "Network"),
            ErrorCategory::Authentication => write!(f, "Authentication"),
            ErrorCategory::Database => write!(f, "Database"),
            ErrorCategory::FileSystem => write!(f, "File System"),
            ErrorCategory::Serialization => write!(f, "Serialization"),
            ErrorCategory::Resource => write!(f, "Resource"),
            ErrorCategory::Configuration => write!(f, "Configuration"),
            ErrorCategory::Unknown => write!(f, "Unknown"),
        }
    }
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorSeverity::Info => write!(f, "Info"),
            ErrorSeverity::Warning => write!(f, "Warning"),
            ErrorSeverity::Error => write!(f, "Error"),
            ErrorSeverity::Critical => write!(f, "Critical"),
            ErrorSeverity::Fatal => write!(f, "Fatal"),
        }
    }
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "Closed"),
            CircuitState::Open => write!(f, "Open"),
            CircuitState::HalfOpen => write!(f, "Half-Open"),
        }
    }
}

/// Display error statistics in a user-friendly format
pub fn display_error_statistics(stats: &ErrorStatisticsReport) {
    println!("📊 Error Handling Statistics");
    println!("┌{}┐", "─".repeat(50));

    // Circuit breaker status
    let status_color = match stats.circuit_breaker_state {
        CircuitState::Closed => "🟢",
        CircuitState::HalfOpen => "🟡",
        CircuitState::Open => "🔴",
    };
    println!(
        "│ {:<30} {:>15} │",
        "Circuit Breaker:",
        format!("{} {}", status_color, stats.circuit_breaker_state)
    );

    // Retry statistics
    println!("│ {:<30} {:>15} │", "Total Retries:", stats.total_retries);
    println!(
        "│ {:<30} {:>15} │",
        "Recovery Successes:", stats.recovery_successes
    );

    if stats.total_retries > 0 {
        let success_rate = (stats.recovery_successes as f64 / stats.total_retries as f64) * 100.0;
        println!("│ {:<30} {:>14.1}% │", "Recovery Rate:", success_rate);
    }

    println!("├{}┤", "─".repeat(50));

    // Errors by category
    if !stats.errors_by_category.is_empty() {
        println!("│ {:<48} │", "Errors by Category:");
        for (category, count) in &stats.errors_by_category {
            println!("│   {:<25} {:>20} │", format!("{}:", category), count);
        }
    }

    // Errors by severity
    if !stats.errors_by_severity.is_empty() {
        println!("├{}┤", "─".repeat(50));
        println!("│ {:<48} │", "Errors by Severity:");
        for (severity, count) in &stats.errors_by_severity {
            println!("│   {:<25} {:>20} │", format!("{}:", severity), count);
        }
    }

    println!("└{}┘", "─".repeat(50));
}

