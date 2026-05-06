mod cli;
mod config;
mod database;
mod export;
mod types;
mod ui;
mod utils;

use anyhow::{Context, Result};
use clap::Parser;
use console::style;

use cli::{Cli, Commands};
use config::{parse_field_list, parse_sort_spec, ConfigManager, PerformanceConfig};
use database::{connect_to_mongodb, select_collection, select_database};
use export::{
    display_resumable_exports, print_export_stats, EnhancedEnterpriseExporter, ExportOptions,
    ResumableExportManager, UnifiedExportOptions, UnifiedExporter,
};
use types::{CompressionType, ExportFormat};
use ui::{
    get_advanced_options, get_compression_type, get_connection_profile, get_error_handling_config,
    get_export_format, get_field_selection, get_filter_query, get_output_path,
    get_performance_mode, handle_resume_sessions, show_banner, show_export_preview,
};
use utils::{get_uri_from_env_or_provided, validate_output_path, ErrorHandlingConfig};

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file if it exists
    // This allows users to store their MongoDB URI and other config in .env
    if let Err(e) = dotenvy::dotenv() {
        // Only show error if .env file exists but couldn't be read
        if e.not_found() {
            // .env file doesn't exist - this is fine, not all users need it
        } else {
            eprintln!("⚠️  Warning: Could not load .env file: {}", e);
        }
    }

    let cli = Cli::parse();

    let interactive = match &cli.command {
        Some(Commands::Export(opts)) => !opts.non_interactive,
        _ => true,
    };
    if interactive {
        show_banner();
    }

    match &cli.command {
        Some(Commands::Export(export_opts)) => {
            run_export_command(ExportParams {
                connection_uri: export_opts.uri.as_ref().or(cli.uri.as_ref()),
                database_name: export_opts.database.as_ref(),
                collection_name: export_opts.collection.as_ref(),
                fields: export_opts.fields.as_ref(),
                limit: export_opts.limit,
                skip: export_opts.skip,
                sort: export_opts.sort.as_ref(),
                format: export_opts.format.as_ref(),
                query: export_opts.query.as_ref(),
                output: export_opts.output.as_ref(),
                profile: export_opts.profile.as_ref(),
                perf_mode: export_opts.perf_mode.as_ref(),
                compression: export_opts.compression.as_ref(),
                force_resumable: export_opts.resumable,
                non_interactive: export_opts.non_interactive,
                resume_session_id: None, // No resume session
            })
            .await?;
        }
        Some(Commands::Resume { session_id }) => {
            run_resume_command(session_id.as_ref(), cli.uri.as_ref()).await?;
        }
        Some(Commands::List) => {
            run_list_command().await?;
        }
        None => {
            // Default to export command if no command specified
            run_export_command(ExportParams {
                connection_uri: cli.uri.as_ref(),
                database_name: None,
                collection_name: None,
                fields: None,
                limit: None,
                skip: None,
                sort: None,
                format: None,
                query: None,
                output: None,
                profile: None,
                perf_mode: None,
                compression: None,
                force_resumable: false,
                non_interactive: false,
                resume_session_id: None,
            })
            .await?;
        }
    }

    Ok(())
}

struct ExportParams<'a> {
    connection_uri: Option<&'a String>,
    database_name: Option<&'a String>,
    collection_name: Option<&'a String>,
    fields: Option<&'a String>,
    limit: Option<u64>,
    skip: Option<u64>,
    sort: Option<&'a String>,
    format: Option<&'a cli::ExportFormatArg>,
    query: Option<&'a String>,
    output: Option<&'a String>,
    profile: Option<&'a String>,
    perf_mode: Option<&'a cli::PerformanceModeArg>,
    compression: Option<&'a cli::CompressionArg>,
    force_resumable: bool,
    non_interactive: bool,
    resume_session_id: Option<String>,
}

async fn run_export_command(params: ExportParams<'_>) -> Result<()> {
    // Load configuration
    let config_manager = ConfigManager::new()?;

    // Check for resumable sessions first (interactive mode only)
    let resume_session_id = if !params.non_interactive && params.resume_session_id.is_none() {
        handle_resume_sessions()?
    } else {
        params.resume_session_id
    };

    // Get connection URI (priority: CLI arg -> environment -> profile -> interactive)
    let uri = if let Some(uri_from_env_or_cli) =
        get_uri_from_env_or_provided(params.connection_uri.map(|s| s.as_str()))
    {
        if params.connection_uri.is_some() {
            println!("{} Using provided MongoDB URI", style("🔗").cyan());
        } else {
            println!(
                "{} Using MongoDB URI from environment variable",
                style("🔗").cyan()
            );
        }
        uri_from_env_or_cli
    } else if let Some(profile_name) = params.profile {
        if let Some(profile) = config_manager.get_profile(profile_name) {
            println!(
                "{} Using profile '{}': {}",
                style("📋").cyan(),
                profile_name,
                profile.description.as_deref().unwrap_or("No description")
            );
            profile.uri.clone()
        } else {
            anyhow::bail!("Profile '{}' not found", profile_name);
        }
    } else if params.non_interactive {
        anyhow::bail!(
            "No URI provided and running in non-interactive mode.\n\
            Provide URI via:\n\
            • --uri flag\n\
            • MONGODB_URI environment variable\n\
            • MONGO_URI environment variable\n\
            • --profile flag"
        );
    } else {
        get_connection_profile()?
    };

    // Connect to MongoDB
    let client = connect_to_mongodb(&uri).await?;

    // Get database and collection (from profile, CLI args, or interactive)
    let (_database, collection) = if params.non_interactive {
        // Non-interactive mode - require database and collection from CLI
        let db_name = params.database_name.ok_or_else(|| {
            anyhow::anyhow!(
                "Database name required for non-interactive mode. Use --database option"
            )
        })?;
        let coll_name = params.collection_name.ok_or_else(|| {
            anyhow::anyhow!(
                "Collection name required for non-interactive mode. Use --collection option"
            )
        })?;

        println!(
            "{} Using database: {}",
            style("🗄️").cyan(),
            style(db_name).bold()
        );
        println!(
            "{} Using collection: {}",
            style("📋").cyan(),
            style(coll_name).bold()
        );

        let database = client.database(db_name);
        let collection = database.collection::<mongodb::bson::Document>(coll_name);
        (database, collection)
    } else {
        // Interactive mode or use CLI args if provided
        let database = if let Some(db_name) = params.database_name {
            println!(
                "{} Using specified database: {}",
                style("🗄️").cyan(),
                style(db_name).bold()
            );
            client.database(db_name)
        } else {
            select_database(&client).await?
        };

        let collection = if let Some(coll_name) = params.collection_name {
            println!(
                "{} Using specified collection: {}",
                style("📋").cyan(),
                style(coll_name).bold()
            );
            database.collection::<mongodb::bson::Document>(coll_name)
        } else {
            select_collection(&database).await?
        };

        (database, collection)
    };

    // Parse filter query
    let filter = if let Some(query_str) = params.query {
        serde_json::from_str(query_str).context("Failed to parse query JSON")?
    } else if params.non_interactive {
        mongodb::bson::Document::new() // Empty filter
    } else {
        get_filter_query()?
    };

    // Configure export options (CLI args take priority over interactive)
    let mut export_options = if params.fields.is_some()
        || params.limit.is_some()
        || params.skip.is_some()
        || params.sort.is_some()
    {
        // CLI arguments provided, use them
        ExportOptions {
            fields: params.fields.map(|s| parse_field_list(s)),
            limit: params.limit,
            skip: params.skip,
            sort: params
                .sort
                .map(|s| parse_sort_spec(s).context("Invalid --sort specification"))
                .transpose()?,
            validate_fields: true,
            collect_stats: true,
        }
    } else if params.non_interactive {
        ExportOptions {
            validate_fields: true,
            collect_stats: true,
            ..Default::default()
        }
    } else {
        // Interactive mode - get advanced options
        get_advanced_options()?
    };

    // Interactive field selection (if not specified via CLI and not in non-interactive mode)
    if export_options.fields.is_none() && !params.non_interactive {
        // Try to get a sample document for field discovery
        let sample_doc = collection
            .find_one(filter.clone(), None)
            .await
            .context("Failed to fetch sample document")?;

        if let Some(selected_fields) = get_field_selection(sample_doc.as_ref())? {
            export_options.fields = Some(selected_fields);
        }
    }

    // Get export format
    let export_format = if let Some(format_arg) = params.format {
        match format_arg {
            cli::ExportFormatArg::Jsonl => ExportFormat::JsonLines,
            cli::ExportFormatArg::Json => ExportFormat::JsonArray,
            cli::ExportFormatArg::Csv => ExportFormat::Csv,
            cli::ExportFormatArg::Parquet => ExportFormat::Parquet,
            cli::ExportFormatArg::Bson => ExportFormat::Bson,
        }
    } else if params.non_interactive {
        ExportFormat::JsonLines // Default
    } else {
        get_export_format()?
    };

    // Get compression
    let compression_type = if let Some(comp_arg) = params.compression {
        match comp_arg {
            cli::CompressionArg::None => CompressionType::None,
            cli::CompressionArg::Gzip => CompressionType::Gzip,
        }
    } else if params.non_interactive {
        CompressionType::None // Default
    } else {
        get_compression_type()?
    };

    // Get performance config
    let performance_config = if let Some(perf_arg) = params.perf_mode {
        match perf_arg {
            cli::PerformanceModeArg::Balanced => PerformanceConfig::default(),
            cli::PerformanceModeArg::Memory => PerformanceConfig::memory_optimized(),
            cli::PerformanceModeArg::Speed => PerformanceConfig::speed_optimized(),
        }
    } else if params.non_interactive {
        PerformanceConfig::default()
    } else {
        get_performance_mode()?
    };

    // Get error handling configuration (always available in unified mode)
    let error_config = if params.non_interactive {
        ErrorHandlingConfig::default()
    } else {
        get_error_handling_config()?
    };

    // Get total count for progress tracking (approximate if using limit/skip)
    let total_count = collection
        .count_documents(filter.clone(), None)
        .await
        .context("Failed to count documents")?;

    // Get output path and validate it
    let output_path = if let Some(path) = params.output {
        // Validate the provided path
        let validated = validate_output_path(path).context("Invalid output path")?;
        validated.to_string_lossy().to_string()
    } else if params.non_interactive {
        let default_path = format!(
            "export.{}",
            match export_format {
                ExportFormat::JsonLines => "jsonl",
                ExportFormat::JsonArray => "json",
                ExportFormat::Csv => "csv",
                ExportFormat::Parquet => "parquet",
                ExportFormat::Bson => "bson",
            }
        );
        // Validate the default path
        let validated = validate_output_path(&default_path).context("Invalid output path")?;
        validated.to_string_lossy().to_string()
    } else {
        get_output_path(&export_format, &compression_type)?
    };

    if total_count == 0 {
        println!();
        println!(
            "{}",
            style("⚠️  No documents match the filter criteria").yellow()
        );
        return Ok(());
    }

    // Show export preview and get confirmation (interactive mode only)
    if !params.non_interactive {
        let database_name = collection.namespace().db.clone();
        let collection_name = collection.namespace().coll.clone();

        let confirmed = show_export_preview(ui::ExportPreviewParams {
            database: &database_name,
            collection: &collection_name,
            filter: &filter,
            format: &export_format,
            compression: &compression_type,
            output_path: &output_path,
            options: &export_options,
            estimated_count: Some(total_count),
        })?;

        if !confirmed {
            println!("{}", style("Export cancelled by user").yellow());
            return Ok(());
        }
    }

    // Create unified export options
    let unified_options = UnifiedExportOptions {
        uri,
        filter,
        output_path,
        format: export_format,
        compression: compression_type,
        fields: export_options.fields,
        limit: export_options.limit,
        skip: export_options.skip,
        sort: export_options.sort,
        force_resumable: if params.force_resumable {
            Some(true)
        } else {
            None
        },
        collect_stats: true,
        validate_fields: None, // Let unified system decide
        resume_session_id,
        total_count_hint: Some(total_count),
        ..Default::default()
    };

    // Create and run unified exporter
    let unified_exporter =
        UnifiedExporter::new(collection, Some(performance_config), Some(error_config))?;

    unified_exporter.export(unified_options).await?;

    Ok(())
}

async fn run_resume_command(
    session_id: Option<&String>,
    cli_uri: Option<&String>,
) -> Result<()> {
    let resume_manager = ResumableExportManager::new(None)?;

    if let Some(session_id) = session_id {
        // Resume specific session. The checkpoint no longer persists the connection URI
        // (credential leak risk), so we require the user to re-supply it via --uri or env.
        let uri = get_uri_from_env_or_provided(cli_uri.map(|s| s.as_str())).ok_or_else(|| {
            anyhow::anyhow!(
                "Resume requires a MongoDB URI. Provide one via --uri or the \
                 MONGODB_URI/MONGO_URI environment variable."
            )
        })?;

        if let Some(mut checkpoint) = resume_manager.load_session(session_id)? {
            checkpoint.config.uri = uri.clone();

            println!(
                "{} Resuming export session: {}",
                style("🔄").cyan(),
                session_id
            );
            println!(
                "   Database: {}.{}",
                checkpoint.config.database, checkpoint.config.collection
            );
            println!(
                "   Progress: {:.1}% ({} docs)",
                checkpoint.progress.percentage_complete, checkpoint.progress.documents_exported
            );

            // Connect to MongoDB using the freshly supplied URI.
            let client = connect_to_mongodb(&uri).await?;
            let database = client.database(&checkpoint.config.database);
            let collection = database.collection(&checkpoint.config.collection);

            // Resume the export
            let enhanced_exporter = EnhancedEnterpriseExporter::new(
                PerformanceConfig::default(),
                Some(ErrorHandlingConfig::default()),
                None,
            )?;

            let exported_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let export_options = ExportOptions {
                fields: checkpoint.config.fields.clone(),
                sort: checkpoint.config.sort.clone(),
                limit: checkpoint.config.limit,
                skip: checkpoint.config.skip,
                collect_stats: true,
                validate_fields: true,
            };

            let stats = enhanced_exporter
                .export_with_enterprise_features(
                    &collection,
                    &checkpoint.config.filter,
                    &checkpoint.config.output_path,
                    &checkpoint.config.format,
                    &checkpoint.config.compression,
                    &export_options,
                    exported_count,
                    Some(session_id.clone()),
                    &checkpoint.config.uri,
                )
                .await?;

            print_export_stats(&stats);
            println!(
                "{}",
                style("✅ Export resumed and completed successfully!")
                    .green()
                    .bold()
            );
        } else {
            println!("{} Session '{}' not found", style("❌").red(), session_id);
        }
    } else {
        // List available sessions and let user choose
        let resumable = resume_manager.find_resumable_exports()?;
        if resumable.is_empty() {
            println!("{} No resumable export sessions found", style("📋").cyan());
        } else {
            println!(
                "{} Available resumable export sessions:",
                style("📋").cyan()
            );
            display_resumable_exports(&resumable);
            println!();
            println!("Use: mongo-exporter resume <session-id>");
        }
    }

    Ok(())
}

async fn run_list_command() -> Result<()> {
    let resume_manager = ResumableExportManager::new(None)?;
    let sessions = resume_manager.list_sessions()?;

    if sessions.is_empty() {
        println!("{} No export sessions found", style("📋").cyan());
    } else {
        println!("{} All export sessions:", style("📋").cyan());
        display_resumable_exports(&sessions);
    }

    Ok(())
}
