#![allow(dead_code)]

use crate::utils::error_handling::{AdvancedErrorHandler, ErrorHandlingConfig};
use crate::utils::{create_spinner, mask_error_message, mask_uri, validate_uri_format};
use anyhow::{Context, Result};
use console::style;
use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Collection, Database,
};

pub async fn connect_to_mongodb(uri: &str) -> Result<Client> {
    connect_to_mongodb_with_retry(uri, ErrorHandlingConfig::default()).await
}

/// Connect to MongoDB, retrying the initial `ping` with exponential backoff and circuit-breaker
/// protection (driven by `ErrorHandlingConfig`). Transient network/timeout failures are retried;
/// authentication/permission failures fail fast.
pub async fn connect_to_mongodb_with_retry(
    uri: &str,
    error_config: ErrorHandlingConfig,
) -> Result<Client> {
    validate_uri_format(uri)?;

    let spinner = create_spinner(&format!("Connecting to MongoDB at {}...", mask_uri(uri)));

    let client_options = match ClientOptions::parse(uri).await {
        Ok(options) => options,
        Err(e) => {
            spinner.finish_with_message(format!("{}", style("❌ Invalid MongoDB URI").red()));
            println!();
            println!(
                "{} {}",
                style("Error:").red().bold(),
                mask_error_message(&e.to_string())
            );
            println!();
            println!("{}", style("Examples of valid MongoDB URIs:").yellow());
            println!("  mongodb://localhost:27017");
            println!("  mongodb://username:password@cluster.mongodb.net/database");
            println!("  mongodb+srv://username:password@cluster.mongodb.net/");
            anyhow::bail!("Invalid MongoDB connection URI");
        }
    };

    let client = Client::with_options(client_options).context("Failed to create MongoDB client")?;

    // Probe the connection with retry. The handler classifies network/timeout errors as
    // retryable and authentication errors as fatal, so we don't burn retries on bad credentials.
    let handler = AdvancedErrorHandler::new(error_config);
    let ping_result = handler
        .execute_with_retry(|| {
            let client = client.clone();
            async move {
                client
                    .database("admin")
                    .run_command(doc! {"ping": 1}, None)
                    .await
                    .map_err(anyhow::Error::from)
            }
        })
        .await;

    match ping_result {
        Ok(_) => {
            spinner.finish_with_message(format!(
                "{} Connected to MongoDB at {}",
                style("✅").green(),
                mask_uri(uri)
            ));
            Ok(client)
        }
        Err(e) => {
            spinner.finish_with_message(format!("{}", style("❌ Connection failed").red()));
            println!();
            println!(
                "{} Unable to connect to MongoDB server",
                style("Error:").red().bold()
            );
            println!();
            println!("{}", style("Possible causes:").yellow());
            println!("  • MongoDB server is not running");
            println!("  • Incorrect hostname or port");
            println!("  • Network connectivity issues");
            println!("  • Authentication credentials are wrong");
            println!("  • Firewall blocking the connection");
            println!();
            println!(
                "{} {}",
                style("Technical details:").dim(),
                mask_error_message(&e.to_string())
            );
            anyhow::bail!("Failed to connect to MongoDB server");
        }
    }
}

pub async fn get_database_names(client: &Client) -> Result<Vec<String>> {
    match client.list_database_names(None, None).await {
        Ok(databases) => {
            let user_databases: Vec<String> = databases
                .into_iter()
                .filter(|name| !["admin", "local", "config"].contains(&name.as_str()))
                .collect();

            if user_databases.is_empty() {
                println!();
                println!(
                    "{} No user databases found",
                    style("Warning:").yellow().bold()
                );
                println!(
                    "The MongoDB server only contains system databases (admin, local, config)"
                );
                println!();
                anyhow::bail!("No exportable databases available");
            }

            Ok(user_databases)
        }
        Err(e) => {
            println!();
            println!("{} Failed to list databases", style("Error:").red().bold());
            println!("Make sure you have sufficient permissions to list databases");
            println!();
            println!("{} {}", style("Technical details:").dim(), e);
            anyhow::bail!("Database listing failed");
        }
    }
}

pub async fn get_collection_names(database: &Database) -> Result<Vec<String>> {
    match database.list_collection_names(None).await {
        Ok(collections) => {
            if collections.is_empty() {
                println!();
                println!(
                    "{} No collections found in database '{}'",
                    style("Warning:").yellow().bold(),
                    database.name()
                );
                println!("The selected database appears to be empty");
                println!();
                anyhow::bail!("No collections available for export");
            }
            Ok(collections)
        }
        Err(e) => {
            println!();
            println!(
                "{} Failed to list collections in database '{}'",
                style("Error:").red().bold(),
                database.name()
            );
            println!("Make sure you have read permissions for this database");
            println!();
            println!("{} {}", style("Technical details:").dim(), e);
            anyhow::bail!("Collection listing failed");
        }
    }
}

pub async fn select_database(client: &Client) -> Result<Database> {
    use crate::ui::{create_theme, show_result_section, show_selection_section};
    use dialoguer::Select;

    let db_names = get_database_names(client).await?;

    show_selection_section(
        "Database Selection",
        &format!(
            "Found {} database{} - Use ↑↓ to navigate, Enter to select",
            db_names.len(),
            if db_names.len() == 1 { "" } else { "s" }
        ),
    );

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select a database")
        .items(&db_names)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Database selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    show_result_section(
        &format!("Selected database: {}", &db_names[selection]),
        true,
    );

    Ok(client.database(&db_names[selection]))
}

pub async fn select_collection(database: &Database) -> Result<Collection<Document>> {
    use crate::ui::{create_theme, show_result_section, show_selection_section};
    use dialoguer::Select;

    let collection_names = get_collection_names(database).await?;

    show_selection_section(
        "Collection Selection",
        &format!(
            "Found {} collection{} in '{}' - Use ↑↓ to navigate, Enter to select",
            collection_names.len(),
            if collection_names.len() == 1 { "" } else { "s" },
            database.name()
        ),
    );

    let selection = match Select::with_theme(&create_theme())
        .with_prompt("Select a collection")
        .items(&collection_names)
        .default(0)
        .interact()
    {
        Ok(selection) => selection,
        Err(_) => {
            show_result_section("Collection selection cancelled", false);
            anyhow::bail!("Operation cancelled by user");
        }
    };

    show_result_section(
        &format!("Selected collection: {}", &collection_names[selection]),
        true,
    );

    Ok(database.collection(&collection_names[selection]))
}
