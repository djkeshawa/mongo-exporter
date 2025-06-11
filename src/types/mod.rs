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

#[derive(Debug, Clone)]
pub enum ExportMethod {
    Native,
    MongoExport,
}

// ExportMode removed - unified export system automatically selects optimal strategy

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

impl std::fmt::Display for ExportMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportMethod::Native => write!(f, "Native (High-performance Rust streaming)"),
            ExportMethod::MongoExport => write!(f, "MongoExport (MongoDB official tool)"),
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
