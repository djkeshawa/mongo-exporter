use anyhow::{Context, Result};
use console::style;
use mongodb::{bson::Document, Collection};
use std::process::Command;

use crate::export::enterprise::export::ExportOptions;
use crate::types::ExportFormat;

/// MongoExport integration for high-performance exports
pub struct MongoExportRunner;

impl MongoExportRunner {
    /// Check if mongoexport is available in the system
    pub fn is_available() -> bool {
        Command::new("mongoexport")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    /// Build mongoexport command with common options
    #[deprecated(note = "Use unified_export::UnifiedExporter instead")]
    #[allow(dead_code)]
    pub fn build_command(
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        format: &ExportFormat,
        uri: &str,
        options: Option<&ExportOptions>,
    ) -> Result<Command> {
        // Check if format is supported by mongoexport
        match format {
            ExportFormat::Parquet | ExportFormat::Bson => {
                anyhow::bail!(
                    "Format {:?} is not supported by mongoexport. Use native exporter instead.",
                    format
                );
            }
            _ => {}
        }

        let mut cmd = Command::new("mongoexport");

        // Add connection URI
        cmd.arg("--uri").arg(uri);

        // Add database and collection
        cmd.arg("--db").arg(&collection.namespace().db);
        cmd.arg("--collection").arg(&collection.namespace().coll);

        // Add filter query if not empty
        let filter_str = serde_json::to_string(filter)?;
        if filter_str != "{}" {
            cmd.arg("--query").arg(filter_str);
        }

        // Add output file
        cmd.arg("--out").arg(output_path);

        // Add format-specific options
        match format {
            ExportFormat::JsonLines => {
                // Default JSON format (one document per line)
            }
            ExportFormat::JsonArray => {
                cmd.arg("--jsonArray");
            }
            ExportFormat::Csv => {
                cmd.arg("--type").arg("csv");
                cmd.arg("--headerline");
            }
            _ => unreachable!(), // Already checked above
        }

        // Add advanced options if provided
        if let Some(options) = options {
            if let Some(limit) = options.limit {
                cmd.arg("--limit").arg(limit.to_string());
            }

            if let Some(skip) = options.skip {
                cmd.arg("--skip").arg(skip.to_string());
            }

            if let Some(ref sort) = options.sort {
                let sort_str = serde_json::to_string(sort)?;
                cmd.arg("--sort").arg(sort_str);
            }

            // Add field projection if specified
            if let Some(ref fields) = options.fields {
                let fields_str = fields.join(",");
                cmd.arg("--fields").arg(fields_str);
            }
        }

        Ok(cmd)
    }

    /// Execute mongoexport for supported formats
    #[deprecated(note = "Use unified_export::UnifiedExporter instead")]
    #[allow(dead_code)]
    pub async fn export_with_mongoexport(
        collection: &Collection<Document>,
        filter: &Document,
        output_path: &str,
        format: &ExportFormat,
        options: &ExportOptions,
        uri: &str,
    ) -> Result<()> {
        // Use the centralized command builder
        #[allow(deprecated)]
        let mut cmd =
            Self::build_command(collection, filter, output_path, format, uri, Some(options))?;

        // Show the command being executed
        println!();
        println!(
            "{} Using mongoexport for optimized performance",
            style("🚀").green()
        );
        println!(
            "{} Command: mongoexport with specified parameters",
            style("📝").dim()
        );

        // Execute the command
        let output = cmd
            .output()
            .context("Failed to execute mongoexport command")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("mongoexport failed: {}", stderr);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.is_empty() {
            println!("{}", stdout);
        }

        println!("{} MongoExport completed successfully", style("✅").green());

        Ok(())
    }

    /// Get estimated performance benefit message
    pub fn get_performance_message(estimated_docs: u64) -> String {
        if estimated_docs > 100_000 {
            "MongoExport will provide significant performance benefits for large datasets"
                .to_string()
        } else if estimated_docs > 10_000 {
            "MongoExport recommended for better performance".to_string()
        } else {
            "Either mongoexport or native export will work well".to_string()
        }
    }
}
