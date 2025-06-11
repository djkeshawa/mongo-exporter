
# MongoDB Export CLI

[![GitHub Release](https://img.shields.io/github/v/release/djkeshawa/mongo-exporter)](https://github.com/djkeshawa/mongo-exporter/releases)
[![CI](https://github.com/djkeshawa/mongo-exporter/workflows/CI/badge.svg)](https://github.com/djkeshawa/mongo-exporter/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-brightgreen.svg)](https://www.rust-lang.org)

A powerful, beautiful command-line tool for exporting MongoDB collections with enterprise-grade features, multiple export modes, and comprehensive format support including analytics-optimized Parquet.

```
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

                    Export MongoDB collections with ease and style
```

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Export Formats](#export-formats)
- [Command Line Options](#command-line-options)
- [Example Sessions](#example-sessions)
- [Filter Query Examples](#filter-query-examples)
- [Commands](#commands)
- [Performance Tuning](#performance-tuning)
- [Enterprise Features](#enterprise-features)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

### 🚀 **Export Modes**
- **Basic Mode**: Fast, simple exports for quick data extraction
- **Enterprise Mode**: Advanced features with detailed statistics, field validation, and resumable exports

### 🔗 **Connection & Automation**
- **Easy Connection**: Connect using standard MongoDB URIs or interactive configuration
- **Full Automation**: Complete non-interactive mode for CI/CD pipelines and scripts
- **Connection Profiles**: Save and reuse connection configurations

### 🎯 **Interactive & Automated Selection**
- **Interactive Mode**: Browse databases and collections with arrow key navigation
- **CLI Arguments**: Specify database/collection directly for automation
- **Smart Defaults**: Intelligent suggestions based on your input

### 🔍 **Advanced Filtering & Options**
- **MongoDB Queries**: Apply complex MongoDB filters to export specific documents
- **Field Selection**: Choose specific fields to export
- **Sorting & Pagination**: Control document ordering and limit/skip functionality
- **Performance Tuning**: Configurable performance modes (balanced, memory, speed)

### 📊 **Multiple Export Formats**
- **JSON Lines (.jsonl)**: Streaming format for large datasets
- **JSON Array (.json)**: Pretty-printed format for smaller datasets  
- **CSV (.csv)**: Automatic field flattening with nested object support
- **Parquet (.parquet)**: Columnar format optimized for analytics workloads
- **BSON (.bson)**: MongoDB native binary format

### 🗜️ **Compression Support**
- **None**: Uncompressed for fastest export
- **Gzip**: Compressed for smaller file sizes (all formats)

### 🎨 **Beautiful User Experience**
- **Colorized Output**: Rich terminal styling with MongoDB green theming
- **Progress Tracking**: Real-time progress bars with ETA and throughput metrics
- **ASCII Art Banner**: Professional CLI presentation
- **Spinner Animations**: Visual feedback during operations

### 🛡️ **Enterprise Features**
- **Detailed Statistics**: Export metrics including throughput, field discovery, and error counts
- **Field Validation**: Verify field existence before export
- **Error Handling**: Robust error recovery with detailed reporting
- **Resumable Exports**: Continue interrupted exports from checkpoints
- **Memory Optimization**: Streaming architecture for large datasets

### ⚡ **High Performance**
- **Streaming Export**: Memory-efficient processing of large collections
- **Parallel Processing**: Multi-threaded operations for optimal performance
- **Optimized I/O**: Buffered writers and batch processing
- **Smart Batching**: Configurable batch sizes for different scenarios

## Installation

### Option 1: One-Line Install (Recommended)
Download and install the latest release automatically:

```bash
curl -sSL https://raw.githubusercontent.com/djkeshawa/mongo-exporter/main/install.sh | bash
```

### Option 2: Manual Download
1. Go to [Releases](https://github.com/djkeshawa/mongo-exporter/releases)
2. Download the appropriate binary for your platform:
   - **Linux**: `mongo-exporter-x86_64-unknown-linux-gnu.tar.gz`
   - **Windows**: `mongo-exporter-x86_64-pc-windows-msvc.zip`
   - **macOS**: `mongo-exporter-x86_64-apple-darwin.tar.gz`
3. Extract and run:
   ```bash
   # Linux/macOS
   tar -xzf mongo-exporter-*.tar.gz
   ./mongo-exporter --help
   
   # Windows (PowerShell)
   Expand-Archive mongo-exporter-*.zip
   .\mongo-exporter.exe --help
   ```

### Option 3: Build from Source
If you have Rust installed:

```bash
git clone https://github.com/djkeshawa/mongo-exporter
cd mongo-exporter
cargo build --release
./target/release/mongo-exporter
```

### Option 4: Cargo Install (Rust users)
```bash
cargo install --git https://github.com/djkeshawa/mongo-exporter
```

## Quick Start

After installation, run the CLI to start an interactive export session:

```bash
mongo-exporter
```

Or export directly with a MongoDB URI:

```bash
mongo-exporter export --uri "mongodb://localhost:27017"
```

For automation (scripts/CI/CD):

```bash
mongo-exporter export \
  --uri "mongodb://localhost:27017" \
  --database "myapp" \
  --collection "users" \
  --format json \
  --output "users.json" \
  --non-interactive
```

## Usage

### Interactive Mode (Recommended)
Start the CLI and it will guide you through the export process:

```bash
./target/release/mongo-exporter export
```

### Non-Interactive Mode (Automation)
Perfect for scripts and CI/CD pipelines:

```bash
./target/release/mongo-exporter export \
  --uri "mongodb://localhost:27017" \
  --database "myapp" \
  --collection "users" \
  --query '{"status": "active"}' \
  --format jsonl \
  --output users_active.jsonl \
  --mode enterprise \
  --non-interactive
```

### Export Mode Selection
Choose between Basic and Enterprise modes:

```bash
# Basic mode - fast and simple
./target/release/mongo-exporter export --mode basic

# Enterprise mode - advanced features and statistics  
./target/release/mongo-exporter export --mode enterprise
```

### Connection Examples

#### Local MongoDB
```bash
mongo-exporter export --uri "mongodb://localhost:27017"
```

#### With Authentication
```bash
mongo-exporter export --uri "mongodb://username:password@localhost:27017/database"
```

#### MongoDB Atlas
```bash
mongo-exporter export --uri "mongodb+srv://username:password@cluster.mongodb.net"
```

#### Docker MongoDB
```bash
mongo-exporter export --uri "mongodb://admin:password@localhost:27017"
```

## Export Formats

### JSON Lines (.jsonl) - Streaming Optimized
One JSON document per line - ideal for streaming and large datasets:
```json
{"_id":"507f1f77bcf86cd799439011","name":"John","age":25}
{"_id":"507f1f77bcf86cd799439012","name":"Jane","age":30}
```

### JSON Array (.json) - Pretty Printed  
Pretty-printed JSON array - good for smaller datasets:
```json
[
  {
    "_id": "507f1f77bcf86cd799439011",
    "name": "John", 
    "age": 25
  },
  {
    "_id": "507f1f77bcf86cd799439012",
    "name": "Jane",
    "age": 30
  }
]
```

### CSV (.csv) - Automatic Field Flattening
Comma-separated values with nested object support:
```csv
_id,name,age,address.city,address.country
507f1f77bcf86cd799439011,John,25,New York,USA
507f1f77bcf86cd799439012,Jane,30,London,UK
```

### Parquet (.parquet) - Analytics Optimized
Columnar format perfect for data analytics and warehousing:
- Columnar compression for optimal storage
- Schema discovery from document structure
- Compatible with Apache Spark, Pandas, and other analytics tools
- Supports compression (None, Gzip)

### BSON (.bson) - MongoDB Native
MongoDB's native binary format for perfect data fidelity:
- Preserves all MongoDB data types
- Efficient for MongoDB-to-MongoDB transfers
- Compact binary representation

## Command Line Options

### Core Options
```bash
--uri <URI>                 MongoDB connection URI
--database <DATABASE>       Database name (required for non-interactive)
--collection <COLLECTION>   Collection name (required for non-interactive)
--query <QUERY>            Filter query in JSON format
--output <OUTPUT>          Output file path
```

### Export Configuration
```bash
--format <FORMAT>          Export format: jsonl, json, csv, parquet, bson
--mode <MODE>             Export mode: basic, enterprise
--compression <TYPE>       Compression: none, gzip
--non-interactive         Run without user prompts
```

### Advanced Options
```bash
--fields <FIELDS>         Comma-separated list of fields to export
--limit <LIMIT>           Maximum number of documents to export
--skip <SKIP>             Number of documents to skip
--sort <SORT>             Sort specification (e.g., "created_at:1")
--perf-mode <MODE>        Performance mode: balanced, memory, speed
```

## Example Sessions

### Interactive Enterprise Export
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          MongoDB Export CLI
                      Export collections with ease and style
                                   v0.1.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

? Enter MongoDB connection URI › mongodb://localhost:27017

⣟ Connecting to MongoDB...
✅ Connected to MongoDB

📊 Found 3 databases
? Select a database ›
❯ my_app
  shop_db
  analytics

📊 Found 5 collections in 'my_app'
? Select a collection ›
  products
❯ users
  orders

? Select export mode ›
  Basic (Simple, fast exports)
❯ Enterprise (Advanced features, statistics, resumable)

? Enter the filter query (JSON format) › {"status": "active", "age": {"$gte": 18}}

? Select export format ›
  JSON Lines (.jsonl)
  JSON Array (.json)
  CSV (.csv)
❯ Parquet (.parquet) - Analytics optimized
  BSON (.bson) - MongoDB native format

🚀 Starting enterprise export...
📊 Starting Parquet export with columnar optimization...
🔍 Discovered 25 fields for Parquet schema

⠁ [00:00:15] [████████████████████████████████████████] 1247/1247 (83 docs/s) [00:00:00]

📊 Export Statistics
┌──────────────────────────────────────────────────────┐
│ Documents processed:                            1247 │
│ Documents exported:                             1247 │
│ Fields discovered:                                25 │
│ Bytes written:                              2.45 MB │
│ Processing time:                             15.2ms │
│ Throughput:                              82 docs/sec │
└──────────────────────────────────────────────────────┘

✅ Enterprise export completed successfully!
```

### Automated Script Example
```bash
#!/bin/bash

# Export user data for analytics
./mongo-exporter export \
  --uri "mongodb://localhost:27017" \
  --database "production" \
  --collection "users" \
  --query '{"created_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}' \
  --format parquet \
  --compression gzip \
  --output "analytics/users_2024.parquet.gz" \
  --mode enterprise \
  --non-interactive

# Export orders as CSV for reporting
./mongo-exporter export \
  --uri "mongodb://localhost:27017" \
  --database "production" \
  --collection "orders" \
  --fields "order_id,customer_id,total,status,created_at" \
  --query '{"status": {"$in": ["completed", "shipped"]}}' \
  --format csv \
  --output "reports/completed_orders.csv" \
  --mode basic \
  --non-interactive
```

## Filter Query Examples

### Basic Filters

#### All documents
```json
{}
```

#### Simple equality
```json
{"status": "active"}
```

#### Age range
```json
{"age": {"$gte": 18, "$lte": 65}}
```

#### Multiple conditions
```json
{"status": "active", "country": "USA"}
```

### Advanced Filters

#### OR conditions
```json
{"$or": [{"plan": "premium"}, {"credits": {"$gt": 100}}]}
```

#### Complex nested query
```json
{"$and": [{"status": "active"}, {"$or": [{"plan": "premium"}, {"age": {"$gte": 25}}]}]}
```

#### Date range filter
```json
{"created_at": {"$gte": {"$date": "2023-01-01T00:00:00Z"}, "$lt": {"$date": "2024-01-01T00:00:00Z"}}}
```

#### Text search (case-insensitive)
```json
{"name": {"$regex": "john", "$options": "i"}}
```

#### Array operations
```json
{"tags": {"$in": ["mongodb", "database", "export"]}}
```

## Commands

### `export`
Main export command with interactive or non-interactive modes.

**Options:**
- Interactive mode: Guided prompts for all configuration
- Non-interactive mode: Command-line arguments only
- Basic mode: Fast, simple exports
- Enterprise mode: Advanced features and statistics

### `resume`
Resume a previously interrupted export from checkpoint.

```bash
# List available resume sessions
mongo-exporter list

# Resume specific session
mongo-exporter resume <session-id>

# Resume interactively (will show available sessions)
mongo-exporter resume
```

### `list`
Display available resumable export sessions.

## Performance Tuning

### Performance Modes

#### Balanced (Default)
Optimized balance of speed and memory usage:
```bash
--perf-mode balanced
```

#### Memory Optimized
Reduced memory footprint for resource-constrained environments:
```bash
--perf-mode memory
```

#### Speed Optimized
Maximum performance with higher memory usage:
```bash
--perf-mode speed
```

### Format-Specific Performance

#### For Large Collections (>1M docs)
- **Format**: JSON Lines or Parquet
- **Mode**: Enterprise (automatically uses resumable exports)
- **Compression**: Gzip for network/storage efficiency

#### For Analytics Workloads
- **Format**: Parquet with compression
- **Benefits**: Columnar compression, schema inference, analytics tool compatibility

#### For Fast Development/Testing
- **Format**: JSON Lines
- **Mode**: Basic
- **Compression**: None

## Enterprise Features

### Export Statistics
Detailed metrics provided in Enterprise mode:
- Documents processed, exported, and skipped
- Fields discovered (for CSV/Parquet)
- Bytes written and processing time
- Throughput (documents per second)
- Error summaries with counts

### Field Validation
Validates specified fields exist in the collection before export:
- Samples documents to verify field presence
- Shows available fields if specified ones are missing
- Prevents exports with invalid field specifications

### Resumable Exports
Large exports can be resumed if interrupted:
- Automatic checkpoint creation for exports >1M documents
- Session management with unique identifiers
- Progress preservation across restarts

### Error Handling
Comprehensive error recovery and reporting:
- Individual document error tracking
- Network timeout resilience
- Detailed error summaries in statistics

## Code Architecture

The tool is built with a modular Rust architecture:

### Core Modules
- **CLI Module**: Command-line parsing with clap
- **Database Module**: MongoDB connectivity and operations
- **UI Module**: Interactive prompts and progress display
- **Export Module**: Multi-format streaming export engine
- **Enterprise Module**: Advanced features and statistics
- **Config Module**: Performance and connection management

### Export Engines
- **Basic Exporter**: Fast, simple exports
- **Enterprise Exporter**: Advanced features and statistics
- **Enhanced Enterprise**: Resumable exports with checkpoints

### Format Optimizers
- **JSON Optimizer**: Parallel JSON processing
- **CSV Optimizer**: Streaming field discovery and flattening
- **Parquet Engine**: Columnar export with Arrow integration

## Dependencies

### Core Runtime
- `tokio`: Async runtime for MongoDB operations
- `mongodb`: Official MongoDB Rust driver
- `futures`: Stream processing utilities

### CLI & UI
- `clap`: Command-line argument parsing
- `dialoguer`: Interactive terminal prompts
- `indicatif`: Progress bars and spinners
- `console`: Terminal styling and colors

### Data Processing
- `serde`/`serde_json`: JSON serialization
- `csv`: CSV file generation
- `arrow`/`parquet`: Columnar data processing
- `flate2`: Gzip compression

### Utilities
- `anyhow`: Error handling and context
- `chrono`: Date/time processing
- `rayon`: Parallel processing

## Development

### Building from Source
```bash
git clone <repository-url>
cd mongo-export-cli
cargo build --release
```

### Development Commands
```bash
# Build in debug mode
cargo build

# Run with specific options
cargo run -- export --uri "mongodb://localhost:27017"

# Check for errors
cargo check

# Run linter  
cargo clippy

# Format code
cargo fmt

# Run tests
cargo test
```

### Code Quality
```bash
# Check for unused dependencies
cargo machete  # if installed

# Security audit
cargo audit    # if installed

# Performance profiling
cargo build --release
perf record ./target/release/mongo-exporter export [options]
```

## Error Handling

The tool provides comprehensive error handling with user-friendly messages:

### Connection Issues
- Invalid URIs with format suggestions
- Network connectivity problems with troubleshooting
- Authentication failures with credential guidance
- Timeout handling with retry suggestions

### Database Issues  
- Permission errors with required privilege information
- Missing databases/collections with available options
- Schema validation errors with field suggestions

### Export Issues
- File permission errors with path validation
- Disk space warnings with size estimations
- Format-specific errors with alternative suggestions
- Memory issues with performance mode recommendations

## Contributing

We welcome contributions! Please see our contributing guidelines for:
- Code style and formatting requirements
- Testing procedures and coverage expectations
- Pull request workflow and review process
- Issue reporting and feature request templates

## License

This project is licensed under the MIT License - see the LICENSE file for details.