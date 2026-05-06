# Testing

Run the local unit suite:

```bash
cargo test
```

Run strict lint checks:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

The current automated tests cover parsing, formatting helpers, BSON/JSON conversion, URI masking, path validation, and resumable checkpoint edge cases. Database-backed behavior such as real MongoDB cursor resume, field projection, and exported file contents should be covered with integration tests against a temporary MongoDB instance before release.
