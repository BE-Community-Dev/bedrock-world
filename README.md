# bedrock-world

[English](README.md) | [简体中文](README.zh-CN.md)

`bedrock-world` is a Minecraft Bedrock world library built on top of
`bedrock-leveldb`. It provides fast `level.dat` access, little-endian NBT,
Bedrock DB key classification, player reads, chunk/subchunk parsing including
LevelDB-era legacy terrain records, entity and block-entity parsing, item
extraction, biome summaries, and selected map/village/global record
classification.

This crate does not generate map images and does not copy image/color generation
code from `bedrock-dev/bedrock-level`. That project is used only as a parsing
behavior reference.

## Recommended API

- `read_level_dat(path)` and `write_level_dat_atomic(path, document)` are the
  launcher fast path. They do not open LevelDB.
- `BedrockWorld::open(path, OpenOptions)` creates a lazy world handle backed by
  `bedrock-leveldb`; it does not parse the full world.
- Use category APIs for UI and tools:
  `classify_keys_blocking`, `list_players_blocking`,
  `list_chunk_positions_blocking`, `parse_chunk_blocking`,
  `parse_subchunk_blocking`, `scan_entities_blocking`,
  `scan_block_entities_blocking`, `scan_items_blocking`, `scan_maps_blocking`,
  `scan_villages_blocking`, and `scan_globals_blocking`.
- Async wrappers use `tokio::task::spawn_blocking`, so disk and decode work does
  not block the foreground async runtime.
- `WorldScanOptions` controls threading, cancellation, and progress callbacks.
- `WorldPipelineOptions` refines the bounded pipeline with queue depth, chunk
  batch size, subchunk decode worker budget, and progress cadence. Zero values
  choose automatic defaults.
- Render-specific APIs now have their own fast path:
  `list_render_chunk_positions_blocking`,
  `list_render_chunk_positions_in_region_blocking`,
  `load_render_chunk_blocking`, `load_render_chunks_blocking`, and
  `load_render_region_blocking`. These only read records needed to render
  chunks and can run with bounded parallelism.
- `parse_world_blocking(WorldParseOptions)` is an explicit advanced/offline API,
  not a launcher default path.
- Public fallible APIs return `bedrock_world::Result<T>`. Match
  `BedrockWorldError::kind()` for stable categories such as read-only handles,
  cancellation, malformed NBT, unsupported chunk formats, and backend errors.

More detailed API and testing notes are in [`docs/API.md`](docs/API.md) and
[`docs/TESTING.md`](docs/TESTING.md).

```rust
use bedrock_world::{
    read_level_dat, BedrockWorld, OpenOptions, WorldScanOptions, WorldThreadingOptions,
};

async fn inspect_world() -> bedrock_world::Result<()> {
    let level = read_level_dat("path/to/minecraftWorld/level.dat")?;
    println!("level.dat version={}", level.header.version);

    let world = BedrockWorld::open("path/to/minecraftWorld", OpenOptions::default()).await?;
    let players = world.list_players().await?;
    println!("players={}", players.len());

    let key_counts = world
        .classify_keys(WorldScanOptions {
            threading: WorldThreadingOptions::Auto,
            ..WorldScanOptions::default()
        })
        .await?;
    println!("key categories={}", key_counts.len());

    Ok(())
}
```

## Parse Strategies

| Strategy | Raw entries | Raw values | Subchunk indices | Actor resolution | Intended use |
| --- | ---: | ---: | ---: | --- | --- |
| `WorldParseOptions::summary()` | no | no | counts only | referenced actors | UI summaries, large scans |
| `WorldParseOptions::structured()` | selected parsed entries | no | counts only | referenced actors | inspection tools |
| `WorldParseOptions::full_raw()` / `full()` | yes | yes | full 4096 indices | all actors | debugging/offline analysis |

`WorldParseCategories` controls whether chunks, players, entities, block
entities, items, maps, villages, globals, and key counts are parsed. Summary mode
keeps counts and structured summaries while avoiding raw value retention.

## Performance Model

- Launcher operations that only need `level.dat` should use `read_level_dat` and
  `write_level_dat_atomic`; this path touches only `level.dat`.
- `BedrockWorld::open` is lazy and delegates DB access to `bedrock-leveldb`.
- Key classification uses key-only scans and does not retain values.
- Viewport rendering should use
  `list_render_chunk_positions_in_region_blocking` or its async wrapper before
  loading render chunks. This probes each visible chunk with key-only prefix
  scans and skips chunks that have no render records.
- Chunk parsing uses prefix scans and the LevelDB native block cache; repeated
  sample chunk reads avoid full table scans.
- Default world scans use automatic bounded parallel table scanning. Use
  `WorldThreadingOptions::Single` for deterministic debugging.
- `RenderChunkLoadOptions::threading` and `RenderRegionLoadOptions::threading`
  control parallel render chunk loading. Use `Single` when an outer renderer
  already owns the worker pool to avoid nested oversubscription.
- `RenderChunkLoadOptions::priority` can use
  `RenderChunkPriority::DistanceFrom { chunk_x, chunk_z }` so the current
  viewport center loads first. `RenderRegionData::stats` reports requested and
  loaded chunks, decoded subchunks, worker count, queue wait, and total load
  time.
- Long scans can be cancelled through `CancelFlag` and can report progress
  through `ProgressSink`.

### Migration: full chunk scan to viewport render index

Old map viewers often waited for a full-world chunk scan before rendering:

```rust
let all_chunks = world
    .list_chunk_positions_blocking(WorldScanOptions::default())?;
let visible = all_chunks
    .into_iter()
    .filter(|pos| viewport.contains(*pos))
    .collect::<Vec<_>>();
```

Prefer a render-only region query for the current viewport:

```rust
let visible = world.list_render_chunk_positions_in_region_blocking(
    bedrock_world::RenderChunkRegion {
        dimension,
        min_chunk_x,
        min_chunk_z,
        max_chunk_x,
        max_chunk_z,
    },
    WorldScanOptions {
        threading: WorldThreadingOptions::Auto,
        cancel: Some(cancel),
        progress: Some(progress),
        ..WorldScanOptions::default()
    },
)?;
```

Then load just those chunks for the tile or viewport batch:

```rust
let chunks = world.load_render_chunks_blocking(
    visible,
    bedrock_world::RenderChunkLoadOptions {
        threading: WorldThreadingOptions::Fixed(4),
        priority: bedrock_world::RenderChunkPriority::DistanceFrom {
            chunk_x: viewport_center_x,
            chunk_z: viewport_center_z,
        },
        ..bedrock_world::RenderChunkLoadOptions::default()
    },
)?;
```

## Fixture Result

The optional fixture at `tests/fixtures/sample-bedrock-world` is a large local
Bedrock world with native `.ldb` tables and WAL data. It is intentionally
ignored by Git because real worlds are large and may contain player data. When
the folder is missing, the fixture test and large-world benches print a skip
message and continue successfully.

```text
cargo test -p bedrock-world -- --nocapture

unit tests: 36 passed; 0 failed; finished in 0.03s
fixture test: 1 passed; 0 failed; finished in 15.85s

db.entries.count=4571643
db.entries.key_bytes=63624175
db.entries.value_bytes=8398184492
db.chunk.positions.count=237534
db.unknown_keys.first=[]
parsed.sample_chunk.pos=ChunkPos { x: 451, z: -457, dimension: End }
parsed.sample_chunk.records=10
parsed.sample_chunk.subchunks=5
parsed.sample_chunk.subchunk_storages=4
parsed.sample_chunk.palette_states=10
parsed.sample_chunk.block_entities=0
parsed.sample_chunk.biomes.records=1 storages=25
parsed.sample_chunk.errors=[]
players.count=290
```

## Benchmark Results

Last local run:

```text
cargo bench -p bedrock-world

large_fixture.level_dat elapsed_ms=0 version=10 payload_len=2889
large_fixture.db.open_lazy elapsed_ms=1
large_fixture.classify_keys.single elapsed_ms=14097 entries=4571643 entries_per_sec=324287.69
large_fixture.players elapsed_ms=54 count=290
large_fixture.sample_chunk elapsed_ms=48 records=17 subchunks=9 block_entities=0 parse_errors=0

bedrock_world/level_dat/parse_synthetic            [357.45 ns 364.34 ns 374.54 ns]
bedrock_world/level_dat/read_fixture               [53.089 us 53.509 us 53.942 us]
bedrock_world/db/open_lazy                         [765.74 us 776.76 us 794.67 us]
bedrock_world/world/list_players                   [37.395 ms 37.947 ms 39.316 ms]
bedrock_world/subchunk/decode_palette_full_indices [35.651 us 35.934 us 36.196 us]
bedrock_world/subchunk/decode_palette_counts_only  [36.772 us 37.286 us 38.240 us]
bedrock_world/chunk/parse_fixture_chunk            [37.881 ms 38.514 ms 40.018 ms]
```

Criterion used the Plotters backend because `gnuplot` was not installed.
Criterion measurement time is set to 4 seconds so slower fixture-backed benches
complete without short-sampling warnings. The one-shot large fixture harness is
intentionally separate from Criterion because multi-million-entry scans should
not be repeated inside microbenchmarks.

## Completeness

| Area | Status |
| --- | --- |
| `level.dat` header, warning, atomic write | Implemented |
| Bedrock little-endian NBT and consecutive roots | Implemented |
| DB key classification | Implemented for chunk, player, actor, digp, map, village, and common global keys |
| Legacy `LegacyTerrain` records | Implemented for 83,200-byte LevelDB-era terrain values |
| Legacy subchunk block arrays | Implemented for v0 and v2-v7 pre-paletted `SubChunkPrefix` values |
| Subchunk v1/v8/v9 palette parsing | Implemented, counts-only and full-indices modes |
| Data2D/Data3D biome summary | Implemented |
| `digp -> actorprefix` actor resolution | Implemented, configurable |
| Players, entities, block entities, item stacks | Implemented common field extraction |
| Unknown version-specific data | Preserved or counted according to retention mode |
| Full structured editing for every chunk version | Not implemented |
| Map image generation | Not implemented |

Historical Bedrock worlds that predate LevelDB and store chunk data in
`chunks.dat` / `entities.dat` are not database worlds; importing those files is
outside this crate's current scope.

## Operational Guidance

- Do not call `parse_world_blocking(WorldParseOptions::full_raw())` from UI code.
  It is an offline debugging path.
- Use `read_level_dat` for launcher metadata and `write_level_dat_atomic` for
  safe `level.dat` edits.
- Use category APIs when only one class of data is required. This avoids parsing
  entities, chunks, and global records unnecessarily.
- For renderers, build a viewport `RenderChunkRegion` first and use the
  render-index APIs. Keep full `list_chunk_positions_blocking` for metadata,
  search, and offline export workflows.
- This initial GitHub release is marked `publish = false` because
  `bedrock-leveldb` is still consumed by pinned Git revision. Publish
  `bedrock-leveldb` first, then switch this dependency to crates.io before
  publishing `bedrock-world`.
