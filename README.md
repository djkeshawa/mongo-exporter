# mongo-exporter

An automation-first MongoDB collection exporter for reproducible, reviewable data
pipelines. The strict `export` command never prompts, never creates configuration
implicitly, refuses accidental overwrites, and publishes file outputs atomically.

## Quick start

```powershell
$env:MONGODB_URI = "mongodb://localhost:27017"
mongo-exporter export `
  --database app `
  --collection users `
  --format jsonl `
  --output exports/users.jsonl `
  --create-dirs
```

Use `--uri-env NAME` when the URI is supplied by a secret manager under a
non-standard variable. `--uri` is supported for one-off local use but should not
be placed in scripts or CI logs.

The guided workflow is deliberately separate:

```powershell
mongo-exporter wizard
```

## Contract

`export` requires `--database`, `--collection`, `--format`, and `--output`.
`--query` accepts MongoDB Extended JSON and defaults to `{}`. `--query-file` is
available for checked-in or generated filters. Events go to stderr, so data can
be sent to stdout with `--output -` for JSONL, CSV, or BSON. When supplied,
`--limit` must be a positive integer; omit it for an unlimited export.

File exports use a temporary file in the destination directory and publish it only
after the stream has been flushed and synced. Existing files require `--overwrite`.
Missing directories require the explicit `--create-dirs` flag. A report can be
written with `--report`; it contains the run id, query/schema digests, document and
byte counts, output SHA-256, consistency mode, and warnings.

## Formats and fidelity

- `jsonl` and `json` default to canonical MongoDB Extended JSON v2, preserving
  BSON types. Use `--json-mode relaxed` for more readable JSON or `--json-mode plain`
  when downstream tools require type-coerced JSON.
- `bson` writes concatenated BSON documents and is the fidelity-first binary option.
- `csv` and `parquet` require a versioned schema manifest. This prevents field order
  from changing between runs.
- Gzip is supported for file outputs. It is intentionally not allowed on stdout.

Generate a schema manifest from a sample:

```powershell
mongo-exporter inspect schema `
  --database app `
  --collection users `
  --sample-size 1000 `
  --output schemas/users.v1.json `
  --create-dirs

mongo-exporter export `
  --database app `
  --collection users `
  --format csv `
  --schema schemas/users.v1.json `
  --output exports/users.csv `
  --create-dirs
```

The current Parquet writer stores manifest columns as nullable UTF-8 values,
preserving empty strings while mapping missing, BSON null, and undefined values to
Parquet null. The manifest is therefore an explicit output schema, not an implicit
inference step.

## Consistency and large exports

The default `--consistency best-effort` streams a normal MongoDB cursor. Use
`--consistency snapshot` when a replica set or Atlas deployment can provide MongoDB
snapshot read concern; unsupported servers fail with an error rather than silently
downgrading. Snapshot mode cannot be combined with `--count` or checkpointing.

Checkpointing is opt-in and deliberately narrow: uncompressed, best-effort JSONL,
CSV, or BSON with deterministic `_id:1` ordering and no skip. The legacy resumable
engine currently emits plain JSON, so JSONL checkpointing requires
`--json-mode plain`.

```powershell
mongo-exporter export `
  --database app `
  --collection audit_events `
  --format jsonl `
  --json-mode plain `
  --sort _id:1 `
  --checkpoint-dir .checkpoints `
  --output exports/audit.jsonl `
  --create-dirs

mongo-exporter checkpoint list --directory .checkpoints
mongo-exporter checkpoint resume SESSION_ID --directory .checkpoints
mongo-exporter checkpoint delete SESSION_ID --directory .checkpoints
```

Checkpoint files never persist the MongoDB URI. Resuming requires `--uri`,
`--uri-env`, or the configured environment variable.

## Profiles and configuration

Profiles reference an environment variable and may carry non-secret metadata; the
export command still requires its database, collection, format, and output arguments
explicitly. Profiles do not store connection strings. Configuration is created only
by an explicit command:

```powershell
mongo-exporter config init --path .mongo-exporter.toml
mongo-exporter --config .mongo-exporter.toml config list
```

Example profile shape:

```toml
[profiles.reporting]
uri_env = "REPORTING_MONGODB_URI"
description = "Read-only reporting cluster"
```

## Automation helpers

Use `--log-format json` for machine-readable NDJSON events on stderr, `--count` for
an exact preflight count, and `--no-progress` to suppress periodic progress events.
Generate shell completion scripts with:

```powershell
mongo-exporter completions powershell
mongo-exporter completions bash
```

Exit codes are stable enough for automation: `1` is an operational failure, `2`
is invalid usage, `4` is a connection/URI failure, `5` is query/schema failure, and
`6` is an output/publication failure.

## Development

The repository targets the stable Rust channel, declares Rust 1.83 as its minimum
supported version, and includes a `rust-toolchain.toml` for consistent local
tooling. Run:

```powershell
cargo fmt --all
cargo check --offline
cargo test --offline
cargo clippy --all-targets --all-features -- -D warnings
```

The MongoDB integration paths require a reachable MongoDB deployment; unit tests
cover parsing, security-sensitive path handling, checkpoints, and atomic publication.

## License

MIT
