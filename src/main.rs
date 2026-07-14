mod app;
mod cli;
mod config;
mod database;
mod export;
mod types;
mod ui;
mod utils;

use clap::Parser;
use std::{ffi::OsString, process::ExitCode};

use cli::{Cli, LogFormatArg};

#[tokio::main]
async fn main() -> ExitCode {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let json_errors = json_log_requested(&raw_args);
    let cli = match Cli::try_parse_from(&raw_args) {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            if json_errors && exit_code != 0 {
                let event = serde_json::json!({
                    "schema_version": 1,
                    "event": "error",
                    "timestamp": chrono::Utc::now(),
                    "exit_code": exit_code,
                    "message": error.to_string(),
                });
                eprintln!("{event}");
            } else {
                let _ = error.print();
            }
            return ExitCode::from(u8::try_from(exit_code).unwrap_or(1));
        }
    };
    let config_path = cli.config.clone();
    let json_errors =
        json_errors || matches!(cli.command.requested_log_format(), Some(LogFormatArg::Json));
    match app::dispatch(cli.command, config_path.as_deref(), cli.color).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let exit_code = classify_error(&error);
            if json_errors {
                let event = serde_json::json!({
                    "schema_version": 1,
                    "event": "error",
                    "timestamp": chrono::Utc::now(),
                    "exit_code": exit_code,
                    "message": format!("{error:#}"),
                });
                eprintln!("{event}");
            } else {
                eprintln!("error: {error:#}");
            }
            ExitCode::from(exit_code)
        }
    }
}

fn json_log_requested(args: &[OsString]) -> bool {
    args.iter().any(|argument| argument == "--log-format=json")
        || args.windows(2).any(|arguments| {
            arguments[0] == "--log-format" && arguments[1].eq_ignore_ascii_case("json")
        })
}

fn classify_error(error: &anyhow::Error) -> u8 {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("output") || message.contains("overwrite") {
        6
    } else if message.contains("uri")
        || message.contains("connection")
        || message.contains("mongodb connection")
    {
        4
    } else if message.contains("schema") || message.contains("query") {
        5
    } else if message.contains("invalid")
        || message.contains("required")
        || message.contains("must be")
        || message.contains("cannot be")
    {
        2
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_both_json_log_argument_forms() {
        assert!(json_log_requested(&[
            "mongo-exporter".into(),
            "--log-format=json".into(),
        ]));
        assert!(json_log_requested(&[
            "mongo-exporter".into(),
            "--log-format".into(),
            "JSON".into(),
        ]));
        assert!(!json_log_requested(&[
            "mongo-exporter".into(),
            "--log-format".into(),
            "human".into(),
        ]));
    }
}
