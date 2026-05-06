use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

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
    config: Config,
}

impl ConfigManager {
    /// Create a new configuration manager
    pub fn new() -> Result<Self> {
        let config_path = Self::get_config_path()?;
        let config = Self::load_config(&config_path)?;

        Ok(Self { config })
    }

    /// Get the configuration file path.
    ///
    /// `MONGO_EXPORTER_CONFIG` overrides the location and is interpreted as a *file path*
    /// (the previous variable name `config_dir` was misleading).
    fn get_config_path() -> Result<PathBuf> {
        if let Ok(config_file) = std::env::var("MONGO_EXPORTER_CONFIG") {
            return Ok(PathBuf::from(config_file));
        }

        if let Some(home_dir) = dirs::home_dir() {
            let config_path = home_dir
                .join(".config")
                .join("mongo-exporter")
                .join("config.toml");
            return Ok(config_path);
        }

        Ok(PathBuf::from("mongo-exporter.toml"))
    }

    /// Load configuration from file. On first run, writes a default config and signals via
    /// stderr (not stdout) so the bootstrap message doesn't pollute machine-parsed output.
    fn load_config(path: &Path) -> Result<Config> {
        if path.exists() {
            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {}", path.display()))?;

            toml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", path.display()))
        } else {
            let config = Config::default();

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create config directory: {}", parent.display())
                })?;
            }

            let content =
                toml::to_string_pretty(&config).context("Failed to serialize default config")?;

            fs::write(path, content)
                .with_context(|| format!("Failed to write default config: {}", path.display()))?;

            eprintln!("📝 Created default configuration at: {}", path.display());

            Ok(config)
        }
    }

    /// Get a connection profile by name
    pub fn get_profile(&self, name: &str) -> Option<&ConnectionProfile> {
        if name == "default" {
            Some(&self.config.default)
        } else {
            self.config.profiles.get(name)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_field_list_simple() {
        let fields = parse_field_list("name,age,email");
        assert_eq!(fields, vec!["name", "age", "email"]);
    }

    #[test]
    fn test_parse_field_list_with_spaces() {
        let fields = parse_field_list("name, age , email ");
        assert_eq!(fields, vec!["name", "age", "email"]);
    }

    #[test]
    fn test_parse_field_list_empty() {
        let fields = parse_field_list("");
        assert_eq!(fields, Vec::<String>::new());
    }

    #[test]
    fn test_parse_field_list_single() {
        let fields = parse_field_list("username");
        assert_eq!(fields, vec!["username"]);
    }

    #[test]
    fn test_parse_field_list_with_empty_items() {
        let fields = parse_field_list("name,,age,,,email");
        assert_eq!(fields, vec!["name", "age", "email"]);
    }

    #[test]
    fn test_parse_sort_spec_ascending() {
        let sort = parse_sort_spec("name:1").unwrap();
        assert_eq!(sort.get_i32("name").unwrap(), 1);
    }

    #[test]
    fn test_parse_sort_spec_descending() {
        let sort = parse_sort_spec("created_at:-1").unwrap();
        assert_eq!(sort.get_i32("created_at").unwrap(), -1);
    }

    #[test]
    fn test_parse_sort_spec_asc_keyword() {
        let sort = parse_sort_spec("name:asc").unwrap();
        assert_eq!(sort.get_i32("name").unwrap(), 1);
    }

    #[test]
    fn test_parse_sort_spec_desc_keyword() {
        let sort = parse_sort_spec("created_at:desc").unwrap();
        assert_eq!(sort.get_i32("created_at").unwrap(), -1);
    }

    #[test]
    fn test_parse_sort_spec_default_ascending() {
        let sort = parse_sort_spec("name").unwrap();
        assert_eq!(sort.get_i32("name").unwrap(), 1);
    }

    #[test]
    fn test_parse_sort_spec_multiple_fields() {
        let sort = parse_sort_spec("name:1,created_at:-1,age:asc").unwrap();
        assert_eq!(sort.get_i32("name").unwrap(), 1);
        assert_eq!(sort.get_i32("created_at").unwrap(), -1);
        assert_eq!(sort.get_i32("age").unwrap(), 1);
    }

    #[test]
    fn test_parse_sort_spec_with_spaces() {
        let sort = parse_sort_spec("name : 1 , created_at : -1").unwrap();
        assert_eq!(sort.get_i32("name").unwrap(), 1);
        assert_eq!(sort.get_i32("created_at").unwrap(), -1);
    }

    #[test]
    fn test_parse_sort_spec_invalid_direction() {
        let result = parse_sort_spec("name:invalid");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid sort direction"));
    }
}
