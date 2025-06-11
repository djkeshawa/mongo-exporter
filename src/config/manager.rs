use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use crate::config::PerformanceConfig;
use crate::types::{CompressionType, ExportFormat};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Default connection settings
    pub default: ConnectionProfile,
    /// Named profiles for different environments
    pub profiles: HashMap<String, ConnectionProfile>,
    /// Global settings
    pub settings: GlobalSettings,
}

/// Connection profile for different environments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfile {
    /// MongoDB connection URI
    pub uri: String,
    /// Default database name
    pub database: Option<String>,
    /// Default collection name
    pub collection: Option<String>,
    /// Default export format
    pub format: Option<ExportFormat>,
    /// Default compression
    pub compression: Option<CompressionType>,
    /// Default output directory
    pub output_dir: Option<String>,
    /// Performance mode
    pub performance_mode: Option<String>,
    /// Field selections
    pub fields: Option<Vec<String>>,
    /// Default filter query
    pub filter: Option<String>,
    /// Connection timeout in seconds
    pub timeout: Option<u64>,
    /// Description of this profile
    pub description: Option<String>,
}

/// Global application settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSettings {
    /// Default performance mode
    pub default_performance_mode: String,
    /// Enable color output
    pub enable_colors: bool,
    /// Progress bar update frequency
    pub progress_frequency: u64,
    /// Auto-save export statistics
    pub save_statistics: bool,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry delay in milliseconds
    pub retry_delay: u64,
    /// Log level
    pub log_level: String,
}

impl Default for ConnectionProfile {
    fn default() -> Self {
        Self {
            uri: "mongodb://localhost:27017".to_string(),
            database: None,
            collection: None,
            format: Some(ExportFormat::JsonLines),
            compression: Some(CompressionType::None),
            output_dir: Some("./exports".to_string()),
            performance_mode: Some("balanced".to_string()),
            fields: None,
            filter: None,
            timeout: Some(30),
            description: Some("Default local MongoDB connection".to_string()),
        }
    }
}

impl Default for GlobalSettings {
    fn default() -> Self {
        Self {
            default_performance_mode: "balanced".to_string(),
            enable_colors: true,
            progress_frequency: 100,
            save_statistics: true,
            max_retries: 3,
            retry_delay: 1000,
            log_level: "info".to_string(),
        }
    }
}

/// Configuration manager for loading, saving, and managing profiles
pub struct ConfigManager {
    #[allow(dead_code)]
    config_path: PathBuf,
    config: Config,
}

impl ConfigManager {
    /// Create a new configuration manager
    pub fn new() -> Result<Self> {
        let config_path = Self::get_config_path()?;
        let config = Self::load_config(&config_path)?;

        Ok(Self {
            config_path,
            config,
        })
    }

    /// Get the configuration file path
    fn get_config_path() -> Result<PathBuf> {
        if let Ok(config_dir) = std::env::var("MONGO_EXPORTER_CONFIG") {
            return Ok(PathBuf::from(config_dir));
        }

        if let Some(home_dir) = dirs::home_dir() {
            let config_path = home_dir
                .join(".config")
                .join("mongo-exporter")
                .join("config.toml");
            return Ok(config_path);
        }

        // Fallback to current directory
        Ok(PathBuf::from("mongo-exporter.toml"))
    }

    /// Load configuration from file
    fn load_config(path: &Path) -> Result<Config> {
        if path.exists() {
            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {}", path.display()))?;

            toml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", path.display()))
        } else {
            // Create default config
            let config = Config::default();

            // Create parent directories if they don't exist
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create config directory: {}", parent.display())
                })?;
            }

            // Save default config
            let content =
                toml::to_string_pretty(&config).context("Failed to serialize default config")?;

            fs::write(path, content)
                .with_context(|| format!("Failed to write default config: {}", path.display()))?;

            println!("📝 Created default configuration at: {}", path.display());

            Ok(config)
        }
    }

    /// Save current configuration to file
    pub fn save(&self) -> Result<()> {
        let content =
            toml::to_string_pretty(&self.config).context("Failed to serialize configuration")?;

        fs::write(&self.config_path, content).with_context(|| {
            format!(
                "Failed to write config file: {}",
                self.config_path.display()
            )
        })?;

        Ok(())
    }

    /// Get a connection profile by name
    pub fn get_profile(&self, name: &str) -> Option<&ConnectionProfile> {
        if name == "default" {
            Some(&self.config.default)
        } else {
            self.config.profiles.get(name)
        }
    }

    /// Add or update a connection profile
    pub fn set_profile(&mut self, name: String, profile: ConnectionProfile) {
        if name == "default" {
            self.config.default = profile;
        } else {
            self.config.profiles.insert(name, profile);
        }
    }

    /// List all available profiles
    pub fn list_profiles(&self) -> Vec<(&str, &ConnectionProfile)> {
        let mut profiles = vec![("default", &self.config.default)];

        for (name, profile) in &self.config.profiles {
            profiles.push((name, profile));
        }

        profiles.sort_by_key(|(name, _)| *name);
        profiles
    }

    /// Remove a profile
    #[allow(dead_code)]
    pub fn remove_profile(&mut self, name: &str) -> Result<()> {
        if name == "default" {
            anyhow::bail!("Cannot remove the default profile");
        }

        if self.config.profiles.remove(name).is_none() {
            anyhow::bail!("Profile '{}' not found", name);
        }

        Ok(())
    }

    /// Get global settings
    #[allow(dead_code)]
    pub fn get_settings(&self) -> &GlobalSettings {
        &self.config.settings
    }

    /// Update global settings
    #[allow(dead_code)]
    pub fn update_settings(&mut self, settings: GlobalSettings) {
        self.config.settings = settings;
    }

    /// Get performance config from settings
    #[allow(dead_code)]
    pub fn get_performance_config(&self, mode: Option<&str>) -> PerformanceConfig {
        let mode = mode.unwrap_or(&self.config.settings.default_performance_mode);

        match mode {
            "memory" => PerformanceConfig::memory_optimized(),
            "speed" => PerformanceConfig::speed_optimized(),
            _ => PerformanceConfig::default(), // balanced
        }
    }

    /// Create a sample configuration with multiple profiles
    #[allow(dead_code)]
    pub fn create_sample_config(&mut self) -> Result<()> {
        // Development profile
        let dev_profile = ConnectionProfile {
            uri: "mongodb://localhost:27017".to_string(),
            database: Some("myapp_dev".to_string()),
            collection: None,
            format: Some(ExportFormat::JsonLines),
            compression: Some(CompressionType::None),
            output_dir: Some("./dev-exports".to_string()),
            performance_mode: Some("balanced".to_string()),
            fields: None,
            filter: None,
            timeout: Some(10),
            description: Some("Development environment".to_string()),
        };

        // Production profile
        let prod_profile = ConnectionProfile {
            uri: "mongodb+srv://user:password@cluster.mongodb.net".to_string(),
            database: Some("myapp_prod".to_string()),
            collection: None,
            format: Some(ExportFormat::Parquet),
            compression: Some(CompressionType::Gzip),
            output_dir: Some("./prod-exports".to_string()),
            performance_mode: Some("memory".to_string()),
            fields: None,
            filter: Some(
                r#"{"created_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}"#.to_string(),
            ),
            timeout: Some(60),
            description: Some("Production environment with date filtering".to_string()),
        };

        // Analytics profile
        let analytics_profile = ConnectionProfile {
            uri: "mongodb://analytics-replica:27017".to_string(),
            database: Some("analytics".to_string()),
            collection: Some("events".to_string()),
            format: Some(ExportFormat::Parquet),
            compression: Some(CompressionType::Gzip),
            output_dir: Some("./analytics-exports".to_string()),
            performance_mode: Some("speed".to_string()),
            fields: Some(vec![
                "timestamp".to_string(),
                "user_id".to_string(),
                "event_type".to_string(),
                "properties".to_string(),
            ]),
            filter: Some(r#"{"event_type": {"$in": ["purchase", "signup", "login"]}}"#.to_string()),
            timeout: Some(120),
            description: Some("Analytics events export for data science".to_string()),
        };

        self.set_profile("dev".to_string(), dev_profile);
        self.set_profile("prod".to_string(), prod_profile);
        self.set_profile("analytics".to_string(), analytics_profile);

        self.save()?;

        println!("📋 Created sample profiles: dev, prod, analytics");
        Ok(())
    }
}

/// Helper function to parse field list from string
pub fn parse_field_list(fields_str: &str) -> Vec<String> {
    fields_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Helper function to parse sort specification
pub fn parse_sort_spec(sort_str: &str) -> Result<mongodb::bson::Document> {
    use mongodb::bson::Bson;

    let mut sort_doc = mongodb::bson::Document::new();

    for part in sort_str.split(',') {
        let part = part.trim();
        if let Some((field, direction)) = part.split_once(':') {
            let field = field.trim();
            let direction = direction.trim();

            let sort_value = match direction {
                "1" | "asc" | "ascending" => Bson::Int32(1),
                "-1" | "desc" | "descending" => Bson::Int32(-1),
                _ => anyhow::bail!(
                    "Invalid sort direction '{}'. Use 1/-1 or asc/desc",
                    direction
                ),
            };

            sort_doc.insert(field, sort_value);
        } else {
            // Default to ascending if no direction specified
            sort_doc.insert(part, Bson::Int32(1));
        }
    }

    Ok(sort_doc)
}
