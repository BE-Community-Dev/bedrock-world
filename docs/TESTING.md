# Testing And Benchmarks

## Required Checks

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps
```

These checks are expected to pass on a fresh checkout without private fixture
data.

## Optional Large Fixture

For end-to-end validation against a real Bedrock world, place a copied world at:

```text
tests/fixtures/sample-bedrock-world
```

The folder should contain `level.dat` and a `db/CURRENT` file. The integration
test and large fixture benchmark will detect it automatically. If the folder is
missing, those checks print a skip message and return successfully.

Do not commit this folder. Real worlds are large and may contain player data.

## Benchmarks

Run all benches with:

```powershell
cargo bench
```

`benches/world_parse.rs` always runs a synthetic `level.dat` parse benchmark and
adds LevelDB/chunk/subchunk benchmarks when the optional large fixture exists.

`benches/large_fixture.rs` is a one-shot harness for multi-million-entry scans.
It prints elapsed time and throughput once instead of asking Criterion to repeat
the scan many times.
