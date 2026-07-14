#![allow(dead_code)]

use anyhow::{Context, Result};
use regex::Regex;
use std::env;
use std::sync::OnceLock;

fn credential_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(mongodb(?:\+srv)?://[^:]+:)([^@]+)(@.*)")
            .expect("static regex pattern is valid")
    })
}

/// Masks sensitive information in MongoDB URIs
///
/// # Example
/// ```
/// use mongo_exporter::utils::mask_uri;
///
/// let uri = "mongodb://user:password@localhost:27017/db";
/// let masked = mask_uri(uri);
/// assert_eq!(masked, "mongodb://user:****@localhost:27017/db");
/// ```
pub fn mask_uri(uri: &str) -> String {
    if let Some(caps) = credential_regex().captures(uri) {
        format!("{}****{}", &caps[1], &caps[3])
    } else {
        uri.to_string()
    }
}

/// Gets MongoDB URI from environment variable or provided value
///
/// Priority:
/// 1. Provided URI parameter
/// 2. MONGODB_URI environment variable
/// 3. MONGO_URI environment variable
pub fn get_uri_from_env_or_provided(provided_uri: Option<&str>) -> Option<String> {
    if let Some(uri) = provided_uri {
        return Some(uri.to_string());
    }

    // Try MONGODB_URI first
    if let Ok(uri) = env::var("MONGODB_URI") {
        if !uri.is_empty() {
            return Some(uri);
        }
    }

    // Try MONGO_URI as fallback
    if let Ok(uri) = env::var("MONGO_URI") {
        if !uri.is_empty() {
            return Some(uri);
        }
    }

    None
}

/// Validates MongoDB URI format
///
/// Ensures the URI starts with mongodb:// or mongodb+srv://
pub fn validate_uri_format(uri: &str) -> Result<()> {
    if uri.is_empty() {
        anyhow::bail!("MongoDB URI cannot be empty");
    }

    if !uri.starts_with("mongodb://") && !uri.starts_with("mongodb+srv://") {
        anyhow::bail!(
            "Invalid MongoDB URI format. Must start with 'mongodb://' or 'mongodb+srv://'\n\
            Examples:\n\
            • mongodb://localhost:27017\n\
            • mongodb://user:password@localhost:27017/database\n\
            • mongodb+srv://user:password@cluster.mongodb.net/"
        );
    }

    Ok(())
}

/// Sanitizes a file path to prevent path traversal attacks
///
/// Returns an error if the path contains dangerous patterns
pub fn sanitize_file_path(path: &str) -> Result<std::path::PathBuf> {
    use std::path::{Component, PathBuf};

    if path.is_empty() {
        anyhow::bail!("File path cannot be empty");
    }

    let path_buf = PathBuf::from(path);

    // Check for dangerous components
    for component in path_buf.components() {
        if let Component::ParentDir = component {
            anyhow::bail!(
                "Path contains '..' which could lead to path traversal attack: {}",
                path
            );
        }
    }

    Ok(path_buf)
}

/// Validates that a file path is writable by attempting an actual probe.
///
/// The previous implementation only inspected the owner bit (`0o200`) of the parent
/// directory's permissions, which ignores group/other bits and effective UID — a directory
/// writable by us via group could fail this check, while a sticky/restricted directory
/// could pass. Instead, attempt to create the target file (truncating if it exists) and
/// remove it again only when it didn't already exist; any I/O error becomes a clear,
/// platform-correct write-permission failure.
pub fn validate_output_path(path: &str) -> Result<std::path::PathBuf> {
    let path_buf = sanitize_file_path(path)?;

    if let Some(parent) = path_buf.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            anyhow::bail!(
                "Parent directory does not exist: {}\n\
                Please create the directory first or choose a different path.",
                parent.display()
            );
        }
    }

    let pre_existing = path_buf.exists();

    // Probe writability by opening the file in create+append mode. Append avoids truncating
    // a file the user wants to keep; if creation succeeds and the file is new, we remove it.
    {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path_buf)
            .with_context(|| format!("Output path is not writable: {}", path_buf.display()))?;
    }

    if !pre_existing {
        // Probe-only: don't leave an empty artifact behind.
        let _ = std::fs::remove_file(&path_buf);
    } else {
        eprintln!(
            "⚠️  Warning: File already exists and will be overwritten: {}",
            path
        );
    }

    Ok(path_buf)
}

/// Masks sensitive information in error messages
pub fn mask_error_message(error: &str) -> String {
    credential_regex()
        .replace_all(error, "${1}****${3}")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_uri_with_password() {
        let uri = "mongodb://user:password123@localhost:27017/db";
        let masked = mask_uri(uri);
        assert_eq!(masked, "mongodb://user:****@localhost:27017/db");
        assert!(!masked.contains("password123"));
    }

    #[test]
    fn test_mask_uri_with_srv() {
        let uri = "mongodb+srv://admin:secret@cluster.mongodb.net/";
        let masked = mask_uri(uri);
        assert_eq!(masked, "mongodb+srv://admin:****@cluster.mongodb.net/");
        assert!(!masked.contains("secret"));
    }

    #[test]
    fn test_mask_uri_without_password() {
        let uri = "mongodb://localhost:27017";
        let masked = mask_uri(uri);
        assert_eq!(masked, "mongodb://localhost:27017");
    }

    #[test]
    fn test_validate_uri_format_valid() {
        assert!(validate_uri_format("mongodb://localhost:27017").is_ok());
        assert!(validate_uri_format("mongodb+srv://cluster.mongodb.net").is_ok());
    }

    #[test]
    fn test_validate_uri_format_invalid() {
        assert!(validate_uri_format("http://localhost:27017").is_err());
        assert!(validate_uri_format("localhost:27017").is_err());
        assert!(validate_uri_format("").is_err());
    }

    #[test]
    fn test_sanitize_file_path_valid() {
        assert!(sanitize_file_path("output.json").is_ok());
        assert!(sanitize_file_path("/tmp/output.json").is_ok());
        assert!(sanitize_file_path("./exports/data.csv").is_ok());
    }

    #[test]
    fn test_sanitize_file_path_with_parent_dir() {
        let result = sanitize_file_path("../../../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[test]
    fn test_mask_error_message() {
        let error = "Connection failed to mongodb://user:secret@host:27017";
        let masked = mask_error_message(error);
        assert!(!masked.contains("secret"));
        assert!(masked.contains("****"));
    }
}
