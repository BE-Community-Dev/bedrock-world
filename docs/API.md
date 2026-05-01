# API Guide

`bedrock-world` exposes two layers:

- File-level helpers for `level.dat` and Bedrock little-endian NBT.
- A lazy `BedrockWorld` handle backed by a `WorldStorage` implementation.

## Fast Metadata Path

Use `read_level_dat` when a launcher or management tool only needs world
metadata. This path does not open LevelDB.

```rust
let document = bedrock_world::read_level_dat("world/level.dat".as_ref())?;
println!("level.dat version={}", document.version());
```

Use `write_level_dat_atomic` for `level.dat` edits. It validates the serialized
bytes by parsing them back before replacing the file.

## Lazy World Path

`BedrockWorld::open(path, OpenOptions::default())` opens a world in read-only
mode. Use targeted APIs instead of full-world parsing for UI flows:

- `list_players_blocking`
- `classify_keys_blocking`
- `list_chunk_positions_blocking`
- `parse_chunk_blocking`
- `scan_entities_blocking`
- `scan_block_entities_blocking`
- `scan_items_blocking`

Async methods are wrappers over the blocking implementation and use
`tokio::task::spawn_blocking`.

## Parsing Modes

`WorldParseOptions::summary()` is the default for large scans. It keeps counters
and summaries while avoiding raw value retention.

`WorldParseOptions::structured()` keeps structured parsed entries without raw
values.

`WorldParseOptions::full_raw()` keeps raw values and full subchunk indices. Use
it for offline debugging, not interactive UI.

## Error Handling

All public fallible APIs return `bedrock_world::Result<T>`.

Match `BedrockWorldError::kind()` for stable categories:

```rust
match error.kind() {
    bedrock_world::BedrockWorldErrorKind::ReadOnly => {
        // Ask the caller to reopen with OpenOptions { read_only: false }.
    }
    bedrock_world::BedrockWorldErrorKind::Cancelled => {
        // A scan observed the caller's cancellation flag.
    }
    _ => eprintln!("{error}"),
}
```

Avoid parsing display strings; they are meant for humans.
