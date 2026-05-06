pub mod error_handling;
pub mod secrets;

pub use error_handling::*;
pub use secrets::*;

use anyhow::{Context, Result};
use flate2::{write::GzEncoder, Compression};
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{bson::Document, options::FindOptions, Collection};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;

use crate::types::CompressionType;

/// Build a buffered writer for an output file with optional gzip compression.
///
/// Single source of truth — previously this logic was duplicated across
/// `enterprise/export.rs`, `enterprise/enhanced.rs`, `formats/json_optimizer.rs`,
/// and `formats/csv_optimizer.rs`.
pub fn create_buffered_writer(
    output_path: &str,
    compression: &CompressionType,
    buffer_size: usize,
) -> Result<Box<dyn Write + Send>> {
    let file = File::create(output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path))?;
    Ok(wrap_writer_with_compression(file, compression, buffer_size))
}

/// Wrap an already-opened file (e.g. for append-mode resume) with compression and buffering.
pub fn wrap_writer_with_compression(
    file: File,
    compression: &CompressionType,
    buffer_size: usize,
) -> Box<dyn Write + Send> {
    match compression {
        CompressionType::None => Box::new(BufWriter::with_capacity(buffer_size, file)),
        CompressionType::Gzip => {
            let gz = GzEncoder::new(file, Compression::default());
            Box::new(BufWriter::with_capacity(buffer_size, gz))
        }
    }
}

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

/// Convert a BSON document to a plain `serde_json::Value`.
///
/// `serde_json::to_string(&bson::Document)` would invoke BSON's serde implementation, which
/// emits MongoDB Extended JSON v2 wrappers (`{"$oid": "..."}`, `{"$date": {"$numberLong":
/// "..."}}`, `{"$numberDecimal": "..."}`). That is faithful for round-tripping back into
/// MongoDB but surprises every other downstream consumer (jq, pandas, ad-hoc scripts) that
/// expects plain JSON. This converter produces plain types, consistent with the CSV/Parquet
/// `bson_value_to_string` formatting (ISO-8601 dates, hex ObjectIds, hex binary).
pub fn document_to_json_value(document: &mongodb::bson::Document) -> serde_json::Value {
    bson_to_json_value(mongodb::bson::Bson::Document(document.clone()))
}

fn bson_to_json_value(value: mongodb::bson::Bson) -> serde_json::Value {
    use mongodb::bson::Bson;
    use serde_json::{Number, Value};
    match value {
        Bson::Double(v) => Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null),
        Bson::String(v) => Value::String(v),
        Bson::Array(arr) => Value::Array(arr.into_iter().map(bson_to_json_value).collect()),
        Bson::Document(d) => Value::Object(
            d.into_iter()
                .map(|(k, v)| (k, bson_to_json_value(v)))
                .collect(),
        ),
        Bson::Boolean(v) => Value::Bool(v),
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Int32(v) => Value::Number(v.into()),
        Bson::Int64(v) => Value::Number(v.into()),
        Bson::ObjectId(v) => Value::String(v.to_hex()),
        Bson::DateTime(v) => Value::String(
            v.try_to_rfc3339_string()
                .unwrap_or_else(|_| v.timestamp_millis().to_string()),
        ),
        Bson::Decimal128(v) => Value::String(v.to_string()),
        Bson::Binary(b) => {
            let mut s = String::with_capacity(b.bytes.len() * 2);
            for byte in &b.bytes {
                use std::fmt::Write;
                let _ = write!(&mut s, "{:02x}", byte);
            }
            Value::String(s)
        }
        Bson::RegularExpression(re) => Value::String(format!("/{}/{}", re.pattern, re.options)),
        Bson::Symbol(s) => Value::String(s),
        Bson::JavaScriptCode(c) => Value::String(c),
        Bson::JavaScriptCodeWithScope(j) => Value::String(j.code),
        Bson::Timestamp(ts) => Value::String(format!("{}.{}", ts.time, ts.increment)),
        Bson::MinKey | Bson::MaxKey | Bson::DbPointer(_) => Value::Null,
    }
}

pub fn bson_value_to_string(value: &mongodb::bson::Bson) -> String {
    use mongodb::bson::Bson;
    match value {
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(d) => d.to_string(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Null | Bson::Undefined => String::new(),
        Bson::DateTime(dt) => dt.to_string(),
        Bson::ObjectId(oid) => oid.to_string(),
        Bson::Decimal128(d) => d.to_string(),
        Bson::Symbol(s) => s.clone(),
        Bson::JavaScriptCode(code) => code.clone(),
        Bson::JavaScriptCodeWithScope(jsws) => jsws.code.clone(),
        Bson::RegularExpression(re) => format!("/{}/{}", re.pattern, re.options),
        Bson::Timestamp(ts) => format!("{}.{}", ts.time, ts.increment),
        Bson::Binary(bin) => {
            // Hex is dependency-free and round-trippable; for binary cells in CSV/JSON
            // contexts that's a more useful default than the Debug printout.
            let mut s = String::with_capacity(bin.bytes.len() * 2);
            for byte in &bin.bytes {
                use std::fmt::Write;
                let _ = write!(&mut s, "{:02x}", byte);
            }
            s
        }
        Bson::Array(arr) => {
            format!(
                "[{}]",
                arr.iter()
                    .map(bson_value_to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        Bson::Document(doc) => serde_json::to_string(doc).unwrap_or_default(),
        Bson::MinKey | Bson::MaxKey | Bson::DbPointer(_) => String::new(),
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
    // Honor the caller's limit: never sample more documents than they asked to export, since
    // any field discovered beyond their limit would never appear in the actual output.
    let sample_limit = sample_size as i64;
    options.limit = Some(match options.limit {
        Some(existing) if existing > 0 => existing.min(sample_limit),
        _ => sample_limit,
    });

    let mut cursor = collection
        .find(filter.clone(), options)
        .await
        .context("Failed to execute field discovery query")?;

    let mut all_fields = HashSet::new();

    // Honor the caller's sample_size — early-exiting at a fixed 100 docs would silently
    // drop columns for sparse schemas where new fields appear later in the sample window.
    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                collect_field_names(&document, "", &mut all_fields);
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

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, Bson};

    #[test]
    fn test_bson_value_to_string_primitives() {
        assert_eq!(
            bson_value_to_string(&Bson::String("test".to_string())),
            "test"
        );
        assert_eq!(bson_value_to_string(&Bson::Int32(42)), "42");
        assert_eq!(bson_value_to_string(&Bson::Int64(9999)), "9999");
        assert_eq!(bson_value_to_string(&Bson::Double(2.5)), "2.5");
        assert_eq!(bson_value_to_string(&Bson::Boolean(true)), "true");
        assert_eq!(bson_value_to_string(&Bson::Null), "");
    }

    #[test]
    fn test_bson_value_to_string_array() {
        let array = Bson::Array(vec![
            Bson::String("a".to_string()),
            Bson::String("b".to_string()),
            Bson::Int32(1),
        ]);
        let result = bson_value_to_string(&array);
        assert_eq!(result, "[a,b,1]");
    }

    #[test]
    fn test_collect_field_names_flat_document() {
        let doc = doc! {
            "name": "John",
            "age": 30,
            "email": "john@example.com"
        };
        let mut fields = HashSet::new();
        collect_field_names(&doc, "", &mut fields);

        assert_eq!(fields.len(), 3);
        assert!(fields.contains("name"));
        assert!(fields.contains("age"));
        assert!(fields.contains("email"));
    }

    #[test]
    fn test_collect_field_names_nested_document() {
        let doc = doc! {
            "name": "John",
            "address": {
                "city": "New York",
                "country": "USA",
                "zip": "10001"
            },
            "age": 30
        };
        let mut fields = HashSet::new();
        collect_field_names(&doc, "", &mut fields);

        assert_eq!(fields.len(), 5);
        assert!(fields.contains("name"));
        assert!(fields.contains("age"));
        assert!(fields.contains("address.city"));
        assert!(fields.contains("address.country"));
        assert!(fields.contains("address.zip"));
    }

    #[test]
    fn test_collect_field_names_deeply_nested() {
        let doc = doc! {
            "user": {
                "profile": {
                    "name": "John",
                    "contact": {
                        "email": "john@example.com"
                    }
                }
            }
        };
        let mut fields = HashSet::new();
        collect_field_names(&doc, "", &mut fields);

        assert!(fields.contains("user.profile.name"));
        assert!(fields.contains("user.profile.contact.email"));
    }

    #[test]
    fn test_get_field_value_simple() {
        let doc = doc! {
            "name": "Alice",
            "age": 25
        };

        assert_eq!(get_field_value(&doc, "name"), "Alice");
        assert_eq!(get_field_value(&doc, "age"), "25");
        assert_eq!(get_field_value(&doc, "nonexistent"), "");
    }

    #[test]
    fn test_get_field_value_nested() {
        let doc = doc! {
            "user": {
                "profile": {
                    "name": "Bob",
                    "age": 30
                }
            }
        };

        assert_eq!(get_field_value(&doc, "user.profile.name"), "Bob");
        assert_eq!(get_field_value(&doc, "user.profile.age"), "30");
        assert_eq!(get_field_value(&doc, "user.profile.missing"), "");
    }

    #[test]
    fn test_get_field_value_array() {
        let doc = doc! {
            "tags": ["rust", "mongodb", "export"]
        };

        let result = get_field_value(&doc, "tags");
        assert!(result.contains("rust"));
        assert!(result.contains("mongodb"));
        assert!(result.contains("export"));
    }

    #[test]
    fn test_document_to_json_value_objectid_is_plain_string() {
        // Regression: previously serde_json::to_string(&doc) produced {"$oid":"..."}.
        let oid = mongodb::bson::oid::ObjectId::new();
        let doc = doc! { "_id": oid };
        let value = document_to_json_value(&doc);
        let s = serde_json::to_string(&value).unwrap();
        assert!(!s.contains("$oid"), "got Extended JSON: {}", s);
        assert!(s.contains(&oid.to_hex()));
    }

    #[test]
    fn test_document_to_json_value_datetime_is_iso_string() {
        let dt = mongodb::bson::DateTime::from_millis(1_699_000_000_000);
        let doc = doc! { "ts": dt };
        let value = document_to_json_value(&doc);
        let s = serde_json::to_string(&value).unwrap();
        assert!(!s.contains("$date"), "got Extended JSON: {}", s);
        assert!(s.contains("2023"), "expected ISO-8601 year, got: {}", s);
    }

    #[test]
    fn test_document_to_json_value_decimal128_is_plain_string() {
        let dec: mongodb::bson::Decimal128 = "1.5".parse().unwrap();
        let doc = doc! { "amount": dec };
        let value = document_to_json_value(&doc);
        let s = serde_json::to_string(&value).unwrap();
        assert!(!s.contains("$numberDecimal"), "got Extended JSON: {}", s);
        assert!(s.contains("1.5"));
    }

    #[test]
    fn test_document_to_json_value_nested_and_array() {
        let oid = mongodb::bson::oid::ObjectId::new();
        let doc = doc! {
            "nested": { "_id": oid, "name": "alice" },
            "tags": ["a", "b"]
        };
        let value = document_to_json_value(&doc);
        let s = serde_json::to_string(&value).unwrap();
        assert!(!s.contains("$oid"));
        assert!(s.contains(&oid.to_hex()));
        assert!(s.contains("\"alice\""));
        assert!(s.contains("[\"a\",\"b\"]"));
    }
}
