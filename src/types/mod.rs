#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ExportFormat {
    JsonLines,
    JsonArray,
    Csv,
    Parquet,
    Bson,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
}

impl std::fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportFormat::JsonLines => write!(f, "JSON Lines (.jsonl)"),
            ExportFormat::JsonArray => write!(f, "JSON Array (.json)"),
            ExportFormat::Csv => write!(f, "CSV (.csv)"),
            ExportFormat::Parquet => write!(f, "Parquet (.parquet) - Analytics optimized"),
            ExportFormat::Bson => write!(f, "BSON (.bson) - MongoDB native format"),
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "No compression"),
            CompressionType::Gzip => write!(f, "Gzip compression (smaller files)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_format_display() {
        assert_eq!(ExportFormat::JsonLines.to_string(), "JSON Lines (.jsonl)");
        assert_eq!(ExportFormat::JsonArray.to_string(), "JSON Array (.json)");
        assert_eq!(ExportFormat::Csv.to_string(), "CSV (.csv)");
        assert_eq!(
            ExportFormat::Parquet.to_string(),
            "Parquet (.parquet) - Analytics optimized"
        );
        assert_eq!(
            ExportFormat::Bson.to_string(),
            "BSON (.bson) - MongoDB native format"
        );
    }

    #[test]
    fn test_compression_type_display() {
        assert_eq!(CompressionType::None.to_string(), "No compression");
        assert_eq!(
            CompressionType::Gzip.to_string(),
            "Gzip compression (smaller files)"
        );
    }

    #[test]
    fn test_export_format_serialization() {
        let format = ExportFormat::JsonLines;
        let serialized = serde_json::to_string(&format).unwrap();
        let deserialized: ExportFormat = serde_json::from_str(&serialized).unwrap();

        // Verify round-trip serialization
        assert!(matches!(deserialized, ExportFormat::JsonLines));
    }

    #[test]
    fn test_compression_type_serialization() {
        let compression = CompressionType::Gzip;
        let serialized = serde_json::to_string(&compression).unwrap();
        let deserialized: CompressionType = serde_json::from_str(&serialized).unwrap();

        // Verify round-trip serialization
        assert!(matches!(deserialized, CompressionType::Gzip));
    }

    #[test]
    fn test_export_format_clone() {
        let original = ExportFormat::Parquet;
        let cloned = original.clone();

        assert!(matches!(cloned, ExportFormat::Parquet));
    }

    #[test]
    fn test_compression_type_clone() {
        let original = CompressionType::None;
        let cloned = original.clone();

        assert!(matches!(cloned, CompressionType::None));
    }
}
