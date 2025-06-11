use anyhow::{Context, Result};
use console::style;
use dialoguer::{theme::ColorfulTheme, Input, Select};
use mongodb::bson::Document;
use serde_json::Value;
use std::path::Path;

use crate::config::PerformanceConfig;
use crate::config::{ConfigManager, ConnectionProfile};
use crate::export::enterprise::export::ExportOptions;
use crate::utils::error_handling::ErrorHandlingConfig;
use crate::export::mongoexport::MongoExportRunner;
use crate::export::resumable::ResumableExportManager;
use crate::types::{CompressionType, ExportFormat, ExportMethod};

pub fn create_theme() -> ColorfulTheme {
    ColorfulTheme {
        values_style: console::Style::new().green().bold(),
        active_item_style: console::Style::new().cyan().bold(),
        inactive_item_style: console::Style::new().dim(),
        active_item_prefix: console::style("❯".to_string()).green().bold(),
        inactive_item_prefix: console::style(" ".to_string()).dim(),
        checked_item_prefix: console::style("✓".to_string()).green().bold(),
        unchecked_item_prefix: console::style("○".to_string()).dim(),
        picked_item_prefix: console::style("✓".to_string()).green().bold(),
        unpicked_item_prefix: console::style(" ".to_string()).dim(),
        prompt_style: console::Style::new().bold(),
        prompt_prefix: console::style("?".to_string()).cyan().bold(),
        prompt_suffix: console::style("·".to_string()).dim(),
        success_prefix: console::style("✓".to_string()).green().bold(),
        success_suffix: console::style("·".to_string()).dim(),
        error_prefix: console::style("✗".to_string()).red().bold(),
        error_style: console::Style::new().red(),
        hint_style: console::Style::new().dim(),
        defaults_style: console::Style::new().dim(),
    }
}

pub fn show_input_section(title: &str, description: &str) {
    println!();
    println!("{} {}", style("📝").cyan(), style(title).cyan().bold());
    if !description.is_empty() {
        println!("   {}", style(description).dim());
    }
    println!();
}

pub fn show_selection_section(title: &str, description: &str) {
    println!();
    println!("{} {}", style("🎯").cyan(), style(title).cyan().bold());
    if !description.is_empty() {
        println!("   {}", style(description).dim());
    }
    println!();
}

pub fn show_result_section(message: &str, is_success: bool) {
    let (styled_icon, styled_message) = if is_success {
        (
            style("✓").green().bold(),
            style(message).green().bold(),
        )
    } else {
        (style("✗").red().bold(), style(message).red().bold())
    };

    println!();
    println!("{} {}", styled_icon, styled_message);
    println!();
}

pub fn show_banner() {
    let ascii_art = r#"
███╗   ███╗ ██████╗ ███╗   ██╗ ██████╗  ██████╗ 
████╗ ████║██╔═══██╗████╗  ██║██╔════╝ ██╔═══██╗
██╔████╔██║██║   ██║██╔██╗ ██║██║  ███╗██║   ██║
██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║██║   ██║
██║ ╚═╝ ██║╚██████╔╝██║ ╚████║╚██████╔╝╚██████╔╝
╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝  ╚═════╝ 
                                                 
███████╗██╗  ██╗██████╗  ██████╗ ██████╗ ████████╗███████╗██████╗ 
██╔════╝╚██╗██╔╝██╔══██╗██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝██╔══██╗
█████╗   ╚███╔╝ ██████╔╝██║   ██║██████╔╝   ██║   █████╗  ██████╔╝
██╔══╝   ██╔██╗ ██╔═══╝ ██║   ██║██╔══██╗   ██║   ██╔══╝  ██╔══██╗
███████╗██╔╝ ██╗██║     ╚██████╔╝██║  ██║   ██║   ███████╗██║  ██║
╚══════╝╚═╝  ╚═╝╚═╝      ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝
"#;

    let subtitle = "Export MongoDB collections with ease and style";
    let version = format!("v{}", env!("CARGO_PKG_VERSION"));

    println!();
    println!("{}", style(ascii_art).green().bold());
    println!("{}", style(format!("{:^70}", subtitle)).dim());
    println!("{}", style(format!("{:^70}", version)).dim());
    println!();
}

pub fn get_connection_uri() -> Result<String> {
    show_input_section("MongoDB Connection", "Enter your MongoDB connection string");

    println!();
    println!("{}", style("Examples:").dim());
    println!("  {}  mongodb://localhost:27017", style("Local:").dim());
    println!(
        "  {}   mongodb://user:pass@host:27017/db",
        style("Auth:").dim()
    );
    println!(
        "  {}  mongodb+srv://user:pass@cluster.mongodb.net",
        style("Atlas:").dim()
    );
    println!();

    loop {
        let uri: String = Input::with_theme(&create_theme())
            .with_prompt("MongoDB URI")
            .interact_text()
            .context("Failed to get MongoDB URI")?;

        let trimmed_uri = uri.trim();

        if trimmed_uri.is_empty() {
            println!("{} MongoDB URI cannot be empty", style("✗").red());
            continue;
        }

        if !trimmed_uri.starts_with("mongodb://") && !trimmed_uri.starts_with("mongodb+srv://") {
            println!(
                "{} URI must start with 'mongodb://' or 'mongodb+srv://'",
                style("✗").red()
            );
            continue;
        }

        show_result_section("MongoDB URI validated", true);
        return Ok(trimmed_uri.to_string());
    }
}

pub fn get_filter_query() -> Result<Document> {
    loop {
        show_input_section(
            "Filter Query",
            "Enter a MongoDB query in JSON format (or {} for all documents)",
        );

        let filter_input: String = Input::with_theme(&create_theme())
            .with_prompt("Filter query")
            .default("{}".to_string())
            .interact_text()
            .context("Failed to get filter input")?;

        // Validate JSON
        match serde_json::from_str::<Value>(&filter_input) {
            Ok(json_value) => {
                // Convert to BSON Document
                match mongodb::bson::to_document(&json_value) {
                    Ok(doc) => {
                        show_result_section("Filter query validated", true);
                        return Ok(doc);
                    }
                    Err(e) => {
                        println!("{} {}", style("✗ Invalid BSON:").red(), e);
                        println!(
                            "{}",
                            style("Please try again with a valid MongoDB query.").yellow()
                        );
                        println!();
                        println!("{}", style("Valid examples:").dim());
                        println!("  {} (all documents)", style("{}").green());
                        println!(
                            "  {} (simple filter)",
                            style("{\"status\": \"active\"}").green()
                        );
                        println!(
                            "  {} (range query)",
                            style("{\"age\": {\"$gte\": 18}}").green()
                        );
                    }
                }
            }
            Err(e) => {
                println!("{} {}", style("✗ Invalid JSON:").red(), e);
                println!("{}", style("Please enter a valid JSON object.").yellow());
                println!();
                println!("{}", style("Valid examples:").dim());
                println!("  {} (all documents)", style("{}").green());
                println!(
                    "  {} (simple filter)",
                    style("{\"status\": \"active\"}").green()
                );
                println!(
                    "  {} (multiple conditions)",
                    style("{\"status\": \"active\", \"age\": {\"$gte\": 18}}").green()
                );
                println!(
                    "  {} (text search)",
                    style("{\"name\": {\"$regex\": \"john\", \"$options\": \"i\"}}").green()
                );
            }
        }
    }
}

pub fn get_export_format() -> Result<ExportFormat> {
    let formats = vec![
        ExportFormat::JsonLines,
        ExportFormat::JsonArray,
        ExportFormat::Csv,
        ExportFormat::Parquet,
        ExportFormat::Bson,
    ];

    show_selection_section(
        "Export Format",
        "Choose the output format - Use ↑↓ to navigate, Enter to select",
    );

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select export format")
        .items(&formats)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Format selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    show_result_section(
        &format!("Selected format: {}", &formats[selection].to_string()),
        true,
    );

    Ok(formats[selection].clone())
}

/// Get export method with smart defaults
pub fn get_export_method(estimated_docs: Option<u64>) -> Result<ExportMethod> {
    let mongoexport_available = MongoExportRunner::is_available();

    if !mongoexport_available {
        println!();
        println!("{} MongoExport not found in PATH", style("⚠️").yellow());
        println!("{} Using native Rust implementation", style("ℹ️").blue());
        return Ok(ExportMethod::Native);
    }

    let methods = vec![ExportMethod::MongoExport, ExportMethod::Native];

    show_selection_section(
        "Export Method",
        "Choose export method - MongoExport is faster for large datasets",
    );

    // Show performance recommendation
    if let Some(count) = estimated_docs {
        let message = MongoExportRunner::get_performance_message(count);
        println!();
        println!("{} {}", style("💡").yellow(), style(message).dim());
        println!();
    }

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select export method")
        .items(&methods)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Export method selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    show_result_section(
        &format!("Selected method: {}", &methods[selection].to_string()),
        true,
    );

    Ok(methods[selection].clone())
}

pub fn get_compression_type() -> Result<CompressionType> {
    let compression_types = vec![CompressionType::None, CompressionType::Gzip];

    show_selection_section(
        "Compression",
        "Choose compression - Gzip reduces file size but takes more time",
    );

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select compression")
        .items(&compression_types)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Compression selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    show_result_section(
        &format!(
            "Selected compression: {}",
            &compression_types[selection].to_string()
        ),
        true,
    );

    Ok(compression_types[selection].clone())
}

pub fn get_performance_mode() -> Result<PerformanceConfig> {
    #[derive(Clone)]
    enum PerformanceMode {
        Balanced,
        MemoryOptimized,
        SpeedOptimized,
    }

    impl std::fmt::Display for PerformanceMode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                PerformanceMode::Balanced => write!(f, "Balanced (Recommended for most use cases)"),
                PerformanceMode::MemoryOptimized => {
                    write!(f, "Memory Optimized (Best for large datasets)")
                }
                PerformanceMode::SpeedOptimized => {
                    write!(f, "Speed Optimized (Fastest export, uses more memory)")
                }
            }
        }
    }

    let modes = vec![
        PerformanceMode::Balanced,
        PerformanceMode::MemoryOptimized,
        PerformanceMode::SpeedOptimized,
    ];

    show_selection_section(
        "Performance Mode",
        "Choose optimization strategy - affects memory usage and speed",
    );

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select performance mode")
        .items(&modes)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Performance mode selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    let config = match &modes[selection] {
        PerformanceMode::Balanced => PerformanceConfig::default(),
        PerformanceMode::MemoryOptimized => PerformanceConfig::memory_optimized(),
        PerformanceMode::SpeedOptimized => PerformanceConfig::speed_optimized(),
    };

    show_result_section(
        &format!("Selected mode: {}", &modes[selection].to_string()),
        true,
    );

    Ok(config)
}

pub fn get_output_path(format: &ExportFormat, compression: &CompressionType) -> Result<String> {
    let base_extension = match format {
        ExportFormat::JsonLines => "jsonl",
        ExportFormat::JsonArray => "json",
        ExportFormat::Csv => "csv",
        ExportFormat::Parquet => "parquet",
        ExportFormat::Bson => "bson",
    };

    let default_extension = match compression {
        CompressionType::None => base_extension.to_string(),
        CompressionType::Gzip => format!("{}.gz", base_extension),
    };

    loop {
        show_input_section(
            "Output File Path",
            &format!(
                "Enter the path where the {} file will be saved",
                default_extension.to_uppercase()
            ),
        );

        let path: String = Input::with_theme(&create_theme())
            .with_prompt("File path")
            .default(format!("export.{}", default_extension))
            .interact_text()
            .context("Failed to get output path")?;

        let trimmed_path = path.trim();

        // Validate path is not empty
        if trimmed_path.is_empty() {
            println!("{} File path cannot be empty", style("❌").red());
            println!();
            show_file_path_examples(format);
            continue;
        }

        // Validate path doesn't contain invalid characters
        if trimmed_path.contains('\0') {
            println!(
                "{} File path contains invalid characters",
                style("❌").red()
            );
            println!();
            show_file_path_examples(format);
            continue;
        }

        // Check if path is a directory (ends with /)
        if trimmed_path.ends_with('/') || trimmed_path.ends_with('\\') {
            println!(
                "{} Path appears to be a directory, not a file",
                style("❌").red()
            );
            println!("{} Please specify a filename", style("💡").yellow());
            println!();
            show_file_path_examples(format);
            continue;
        }

        let path_obj = Path::new(trimmed_path);

        // Try to create parent directories if they don't exist
        if let Some(parent) = path_obj.parent() {
            if !parent.as_os_str().is_empty() {
                match std::fs::create_dir_all(parent) {
                    Ok(_) => {}
                    Err(e) => {
                        println!(
                            "{} Failed to create directory '{}': {}",
                            style("❌").red(),
                            parent.display(),
                            e
                        );
                        println!(
                            "{} Please check permissions or try a different path",
                            style("💡").yellow()
                        );
                        println!();
                        show_file_path_examples(format);
                        continue;
                    }
                }
            }
        }

        // Test if we can create/write to the file
        match std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path_obj)
        {
            Ok(_) => {
                // File can be created/written to, remove it for now
                let _ = std::fs::remove_file(path_obj);
                show_result_section(&format!("Output path validated: {}", trimmed_path), true);
                return Ok(trimmed_path.to_string());
            }
            Err(e) => {
                println!(
                    "{} Cannot write to file '{}': {}",
                    style("❌").red(),
                    trimmed_path,
                    e
                );
                println!(
                    "{} Please check permissions or try a different path",
                    style("💡").yellow()
                );
                println!();
                show_file_path_examples(format);
                continue;
            }
        }
    }
}

fn show_file_path_examples(format: &ExportFormat) {
    let extension = match format {
        ExportFormat::JsonLines => "jsonl",
        ExportFormat::JsonArray => "json",
        ExportFormat::Csv => "csv",
        ExportFormat::Parquet => "parquet",
        ExportFormat::Bson => "bson",
    };

    println!("{}", style("Valid file path examples:").dim());
    println!(
        "  {} (current directory)",
        style(&format!("export.{}", extension)).green()
    );
    println!(
        "  {} (subdirectory)",
        style(&format!("data/export.{}", extension)).green()
    );
    println!(
        "  {} (full path)",
        style(&format!("/tmp/exports/data.{}", extension)).green()
    );
    println!(
        "  {} (relative path)",
        style(&format!("./exports/backup.{}", extension)).green()
    );
}

// ================ ENTERPRISE FEATURES UI ================

/// Get or create a configuration profile
pub fn get_connection_profile() -> Result<String> {
    let config_manager = ConfigManager::new()?;
    let profiles: Vec<(&str, &ConnectionProfile)> = config_manager.list_profiles();

    if profiles.is_empty() {
        println!("{} No saved profiles found", style("📋").cyan());
        return get_connection_uri();
    }

    show_selection_section(
        "Connection Profile",
        "Choose a saved profile or enter a new connection",
    );

    let mut options = vec!["Enter new connection".to_string()];
    for (name, profile) in &profiles {
        let desc = profile.description.as_deref().unwrap_or("No description");
        options.push(format!("{} - {}", name, desc));
    }

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select connection")
        .items(&options)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Profile selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    if selection == 0 {
        // New connection
        get_connection_uri()
    } else {
        // Existing profile
        let (_, profile) = profiles[selection - 1];
        show_result_section(
            &format!("Using profile: {}", profiles[selection - 1].0),
            true,
        );
        Ok(profile.uri.clone())
    }
}

/// Interactive field selection for CSV/structured exports
pub fn get_field_selection(sample_doc: Option<&Document>) -> Result<Option<Vec<String>>> {
    show_selection_section(
        "Field Selection",
        "Choose which fields to export (leave empty for all fields)",
    );

    let use_field_selection = if let Some(doc) = sample_doc {
        let available_fields: Vec<String> = doc.keys().cloned().collect();

        if available_fields.is_empty() {
            println!(
                "{} No fields found in sample document",
                style("⚠️").yellow()
            );
            return Ok(None);
        }

        println!("{}", style("Available fields:").dim());
        for (i, field) in available_fields.iter().enumerate() {
            println!("  {}. {}", i + 1, style(field).green());
        }
        println!();

        let options = vec!["Export all fields", "Select specific fields"];

        let selection = match Select::with_theme(&create_theme())
            .with_prompt("Field selection option")
            .items(&options)
            .default(0)
            .interact()
        {
            Ok(selection) => selection,
            Err(_) => {
                show_result_section("Field selection cancelled", false);
                return Ok(None);
            }
        };

        selection == 1
    } else {
        let options = vec!["Export all fields (default)", "Specify custom field list"];

        let selection = match Select::with_theme(&create_theme())
            .with_prompt("Field selection option")
            .items(&options)
            .default(0)
            .interact()
        {
            Ok(selection) => selection,
            Err(_) => {
                show_result_section("Field selection cancelled", false);
                return Ok(None);
            }
        };

        selection == 1
    };

    if !use_field_selection {
        show_result_section("Using all fields", true);
        return Ok(None);
    }

    show_input_section("Custom Field List", "Enter field names separated by commas");

    println!();
    println!("{}", style("Examples:").dim());
    println!("  {}  name,email,created_at", style("Basic:").dim());
    println!("  {}  _id,user.name,stats.total", style("Nested:").dim());
    println!(
        "  {}  name,age,address.city,metadata.*",
        style("Mixed:").dim()
    );
    println!();

    loop {
        let fields_input: String = Input::with_theme(&create_theme())
            .with_prompt("Fields")
            .interact_text()
            .context("Failed to get field input")?;

        let trimmed_input = fields_input.trim();

        if trimmed_input.is_empty() {
            show_result_section("Using all fields (empty input)", true);
            return Ok(None);
        }

        let fields: Vec<String> = trimmed_input
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if fields.is_empty() {
            println!("{} No valid fields found", style("✗").red());
            continue;
        }

        show_result_section(
            &format!("Selected {} fields: {}", fields.len(), fields.join(", ")),
            true,
        );
        return Ok(Some(fields));
    }
}

/// Get advanced export options (limit, skip, sort)
pub fn get_advanced_options() -> Result<ExportOptions> {
    let mut options = ExportOptions::default();

    show_selection_section(
        "Advanced Export Options",
        "Configure advanced settings (all optional)",
    );

    // Ask if user wants to configure advanced options
    let configure_advanced = {
        let choices = vec![
            "Use default settings (recommended for most cases)",
            "Configure advanced options (limit, skip, sort)",
        ];

        let selection = match Select::with_theme(&create_theme())
            .with_prompt("Advanced options")
            .items(&choices)
            .default(0)
            .interact()
        {
            Ok(selection) => selection,
            Err(_) => {
                show_result_section("Advanced options cancelled", false);
                return Ok(options);
            }
        };

        selection == 1
    };

    if !configure_advanced {
        show_result_section("Using default settings", true);
        return Ok(options);
    }

    // Limit
    show_input_section(
        "Document Limit",
        "Maximum number of documents to export (optional)",
    );
    let limit_input: String = Input::with_theme(&create_theme())
        .with_prompt("Limit (leave empty for no limit)")
        .allow_empty(true)
        .interact_text()
        .context("Failed to get limit input")?;

    if !limit_input.trim().is_empty() {
        match limit_input.trim().parse::<u64>() {
            Ok(limit) => {
                options.limit = Some(limit);
                println!("{} Set limit to {} documents", style("✓").green(), limit);
            }
            Err(_) => {
                println!(
                    "{} Invalid limit number, using no limit",
                    style("!").yellow()
                );
            }
        }
    }

    // Skip
    show_input_section(
        "Document Skip",
        "Number of documents to skip from the beginning (optional)",
    );
    let skip_input: String = Input::with_theme(&create_theme())
        .with_prompt("Skip (leave empty for no skip)")
        .allow_empty(true)
        .interact_text()
        .context("Failed to get skip input")?;

    if !skip_input.trim().is_empty() {
        match skip_input.trim().parse::<u64>() {
            Ok(skip) => {
                options.skip = Some(skip);
                println!("{} Set skip to {} documents", style("✓").green(), skip);
            }
            Err(_) => {
                println!(
                    "{} Invalid skip number, using no skip",
                    style("!").yellow()
                );
            }
        }
    }

    // Sort
    show_input_section(
        "Document Sort",
        "Sort specification in MongoDB format (optional)",
    );
    println!();
    println!("{}", style("Examples:").dim());
    println!(
        "  {}  created_at:1 (ascending by created_at)",
        style("Single:").dim()
    );
    println!("  {}  name:-1 (descending by name)", style("Single:").dim());
    println!(
        "  {}  priority:1,created_at:-1 (multiple fields)",
        style("Multi:").dim()
    );
    println!();

    let sort_input: String = Input::with_theme(&create_theme())
        .with_prompt("Sort (leave empty for no sort)")
        .allow_empty(true)
        .interact_text()
        .context("Failed to get sort input")?;

    if !sort_input.trim().is_empty() {
        match crate::config::parse_sort_spec(sort_input.trim()) {
            Ok(sort_doc) => {
                options.sort = Some(sort_doc);
                println!("{} Set sort specification", style("✓").green());
            }
            Err(e) => {
                println!("{} Invalid sort specification: {}", style("!").yellow(), e);
                println!("{} Continuing without sort", style("i").blue());
            }
        }
    }

    // Field validation and stats collection
    options.validate_fields = true;
    options.collect_stats = true;

    show_result_section("Advanced options configured", true);
    Ok(options)
}

/// Handle resumable export session selection
pub fn handle_resume_sessions() -> Result<Option<String>> {
    let resume_manager = ResumableExportManager::new(None)?;
    let resumable_sessions = resume_manager.find_resumable_exports()?;

    if resumable_sessions.is_empty() {
        return Ok(None);
    }

    show_selection_section(
        "Resumable Export Sessions",
        &format!(
            "Found {} interrupted export(s) that can be resumed",
            resumable_sessions.len()
        ),
    );

    // Show session details
    for (i, checkpoint) in resumable_sessions.iter().enumerate() {
        println!(
            "{}. {} ({}% complete)",
            i + 1,
            style(&checkpoint.session_id).cyan(),
            checkpoint.progress.percentage_complete
        );
        println!(
            "   {} {}.{}",
            style("📊").dim(),
            checkpoint.config.database,
            checkpoint.config.collection
        );
        println!(
            "   {} {} docs exported",
            style("📝").dim(),
            checkpoint.progress.documents_exported
        );
        if let Some(ref error) = checkpoint.last_error {
            println!("   {} Last error: {}", style("⚠️").yellow(), error);
        }
        println!();
    }

    let mut options = vec!["🆕 Start new export".to_string()];
    for checkpoint in resumable_sessions.iter() {
        options.push(format!(
            "🔄 Resume session {} ({:.1}% complete)",
            checkpoint.session_id, checkpoint.progress.percentage_complete
        ));
    }

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Choose action")
        .items(&options)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Resume selection cancelled", false);
            return Ok(None);
        }
    };

    if selection == 0 {
        show_result_section("Starting new export", true);
        Ok(None)
    } else {
        let session_id = resumable_sessions[selection - 1].session_id.clone();
        show_result_section(&format!("Resuming session: {}", session_id), true);
        Ok(Some(session_id))
    }
}

/// Show export preview and get confirmation
pub fn show_export_preview(
    database: &str,
    collection: &str,
    filter: &Document,
    format: &ExportFormat,
    compression: &CompressionType,
    output_path: &str,
    options: &ExportOptions,
    estimated_count: Option<u64>,
) -> Result<bool> {
    println!();
    println!("{} {}", style("📋").cyan(), style("Export Preview").cyan().bold());
    println!();

    // Connection info
    println!("   {}: {}.{}", style("Database").dim(), database, collection);

    // Filter
    let filter_display = if filter.is_empty() {
        "All documents".to_string()
    } else {
        let filter_str = format!("{}", filter);
        if filter_str.len() > 60 {
            format!("{}...", &filter_str[..57])
        } else {
            filter_str
        }
    };
    println!("   {}: {}", style("Filter").dim(), filter_display);

    // Format and compression
    println!("   {}: {}", style("Format").dim(), format.to_string());
    println!("   {}: {}", style("Compression").dim(), compression.to_string());
    println!("   {}: {}", style("Output").dim(), output_path);

    // Advanced options
    if let Some(limit) = options.limit {
        println!("   {}: {} documents", style("Limit").dim(), limit);
    }
    if let Some(skip) = options.skip {
        println!("   {}: {} documents", style("Skip").dim(), skip);
    }
    if options.sort.is_some() {
        println!("   {}: Enabled", style("Sort").dim());
    }
    if let Some(ref fields) = options.fields {
        let fields_display = if fields.len() <= 3 {
            fields.join(", ")
        } else {
            format!("{} fields ({}...)", fields.len(), fields[..2].join(", "))
        };
        println!("   {}: {}", style("Fields").dim(), fields_display);
    }

    // Estimated count
    if let Some(count) = estimated_count {
        println!("   {}: {}", style("Est. Documents").dim(), count);
    }

    println!();

    // Confirmation
    let options = vec!["✅ Proceed with export", "❌ Cancel export"];

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Confirm export")
        .items(&options)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Export cancelled", false);
            return Ok(false);
        }
    };

    Ok(selection == 0)
}

/// Configure error handling settings
pub fn get_error_handling_config() -> Result<ErrorHandlingConfig> {
    show_selection_section(
        "Error Handling Configuration",
        "Configure retry and failure handling settings",
    );

    let use_advanced = {
        let choices = vec![
            "Use default error handling (recommended)",
            "Configure advanced error handling",
        ];

        let selection = match Select::with_theme(&create_theme())
            .with_prompt("Error handling")
            .items(&choices)
            .default(0)
            .interact()
        {
            Ok(selection) => selection,
            Err(_) => {
                show_result_section("Error handling cancelled", false);
                return Ok(ErrorHandlingConfig::default());
            }
        };

        selection == 1
    };

    if !use_advanced {
        show_result_section("Using default error handling", true);
        return Ok(ErrorHandlingConfig::default());
    }

    let mut config = ErrorHandlingConfig::default();

    // Max retries
    show_input_section(
        "Maximum Retries",
        "Maximum number of retry attempts for failed operations",
    );
    let retries_input: String = Input::with_theme(&create_theme())
        .with_prompt("Max retries")
        .default(config.max_retries.to_string())
        .interact_text()
        .context("Failed to get retries input")?;

    if let Ok(retries) = retries_input.trim().parse::<u32>() {
        config.max_retries = retries;
    }

    // Circuit breaker threshold
    show_input_section(
        "Circuit Breaker",
        "Number of failures before circuit breaker opens",
    );
    let threshold_input: String = Input::with_theme(&create_theme())
        .with_prompt("Failure threshold")
        .default(config.circuit_breaker_threshold.to_string())
        .interact_text()
        .context("Failed to get threshold input")?;

    if let Ok(threshold) = threshold_input.trim().parse::<u32>() {
        config.circuit_breaker_threshold = threshold;
    }

    show_result_section("Error handling configured", true);
    Ok(config)
}
