pub mod error_handling;

pub use error_handling::*;

use anyhow::{Context, Result};
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{bson::Document, options::FindOptions, Collection};
use std::collections::HashSet;
use std::time::Duration;

pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⣾⣽⣻⢿⡿⣟⣯⣷")
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

pub fn bson_value_to_string(value: &mongodb::bson::Bson) -> String {
    match value {
        mongodb::bson::Bson::String(s) => s.clone(),
        mongodb::bson::Bson::Int32(i) => i.to_string(),
        mongodb::bson::Bson::Int64(i) => i.to_string(),
        mongodb::bson::Bson::Double(d) => d.to_string(),
        mongodb::bson::Bson::Boolean(b) => b.to_string(),
        mongodb::bson::Bson::Null => String::new(),
        mongodb::bson::Bson::DateTime(dt) => dt.to_string(),
        mongodb::bson::Bson::ObjectId(oid) => oid.to_string(),
        mongodb::bson::Bson::Array(arr) => {
            format!(
                "[{}]",
                arr.iter()
                    .map(bson_value_to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        mongodb::bson::Bson::Document(doc) => {
            serde_json::to_string(doc).unwrap_or_else(|_| String::new())
        }
        _ => format!("{:?}", value),
    }
}

pub fn collect_field_names(
    doc: &mongodb::bson::Document,
    prefix: &str,
    fields: &mut std::collections::HashSet<String>,
) {
    for (key, value) in doc {
        let field_name = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            mongodb::bson::Bson::Document(nested_doc) => {
                collect_field_names(nested_doc, &field_name, fields);
            }
            _ => {
                fields.insert(field_name);
            }
        }
    }
}

pub fn get_field_value(doc: &mongodb::bson::Document, field_path: &str) -> String {
    let parts: Vec<&str> = field_path.split('.').collect();
    let mut current = doc;

    for (i, part) in parts.iter().enumerate() {
        if let Some(value) = current.get(*part) {
            if i == parts.len() - 1 {
                // Last part, return the value
                return bson_value_to_string(value);
            } else if let mongodb::bson::Bson::Document(nested_doc) = value {
                // Continue traversing
                current = nested_doc;
            } else {
                // Can't traverse further, return empty
                return String::new();
            }
        } else {
            return String::new();
        }
    }

    String::new()
}

/// Centralized CSV field discovery utility
///
/// Discovers all field names in a collection by sampling documents
/// Supports streaming for memory efficiency and configurable sample sizes
pub async fn discover_csv_fields(
    collection: &Collection<Document>,
    filter: &Document,
    sample_size: usize,
    find_options: Option<&FindOptions>,
) -> Result<Vec<String>> {
    let mut options = find_options.cloned().unwrap_or_default();
    options.limit = Some(sample_size as i64);

    let mut cursor = collection
        .find(filter.clone(), options)
        .await
        .context("Failed to execute field discovery query")?;

    let mut all_fields = HashSet::new();
    let mut documents_processed = 0;

    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                collect_field_names(&document, "", &mut all_fields);
                documents_processed += 1;

                // Optimization: stop early if we have a good field set
                if documents_processed >= 100 && all_fields.len() > 50 {
                    break;
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    // Sort fields for consistent output
    let mut sorted_fields: Vec<String> = all_fields.into_iter().collect();
    sorted_fields.sort();

    Ok(sorted_fields)
}
