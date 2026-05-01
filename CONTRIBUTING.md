# Contributing

Thanks for working on `bedrock-world`. This crate is a library for reading and
inspecting Minecraft Bedrock world data, so changes should preserve data safety,
bounded memory use, and predictable behavior on mixed-version worlds.

## Local Checks

Run these before opening a PR or asking for a release review:

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps
cargo bench
```

`cargo bench` has two parts:

- Criterion microbenchmarks that always run with a synthetic `level.dat`.
- Optional large-world benchmarks that run only when
  `tests/fixtures/sample-bedrock-world` exists locally.

## Fixture Policy

Do not commit real Bedrock worlds, `.mcworld` exports, or large `.ldb` fixture
directories. The optional fixture path is ignored intentionally because real
worlds are large and may contain player data.

When a regression needs fixture coverage, prefer one of these approaches:

- Add a small synthetic in-memory storage test.
- Add a minimal byte-level fixture that contains only the required record.
- Document a manual large-fixture reproduction step in `docs/TESTING.md`.

## API Policy

- Library errors should use `BedrockWorldError` and expose a stable
  `BedrockWorldErrorKind` through `kind()`.
- Public APIs should avoid panics for data that can appear in normal Bedrock
  worlds. Return `Result` for malformed data and `Option` for absence.
- Keep full-world parsing opt-in. Launcher and UI paths should use targeted
  category APIs.
- Preserve unknown record bytes when doing so helps callers continue scanning.

## Dependency Policy

`bedrock-world` depends on `bedrock-leveldb` by pinned Git revision until both
crates are ready for crates.io publishing. Before publishing to crates.io,
publish `bedrock-leveldb`, replace the Git dependency with a versioned crates.io
dependency, and remove `publish = false` from `Cargo.toml`.
