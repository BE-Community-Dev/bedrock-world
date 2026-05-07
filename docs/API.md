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
mode and auto-detects `db/CURRENT` LevelDB worlds or old Pocket Edition
`chunks.dat` worlds. `BedrockWorld::open_blocking` exposes the same detection
for CLI tools and examples. Use targeted APIs instead of full-world parsing for
UI flows:

- `list_players_blocking`
- `classify_keys_blocking`
- `list_chunk_positions_blocking`
- `parse_chunk_blocking`
- `scan_entities_blocking`
- `scan_block_entities_blocking`
- `scan_items_blocking`

Async methods are wrappers over the blocking implementation and use
`tokio::task::spawn_blocking`.

`OpenOptions::format` can force `WorldFormatHint::LevelDb` or
`WorldFormatHint::PocketChunksDat`; the default `Auto` reports the result through
`BedrockWorld::format()`.

```rust
let world = bedrock_world::BedrockWorld::open_blocking(
    "world",
    bedrock_world::OpenOptions::default(),
)?;
assert!(matches!(
    world.format(),
    bedrock_world::WorldFormat::LevelDb
        | bedrock_world::WorldFormat::LevelDbLegacyTerrain
        | bedrock_world::WorldFormat::PocketChunksDat
));
```

## Render Index Path

Interactive renderers should not wait for `list_chunk_positions_blocking` before
painting the first viewport. Use the render-index APIs instead:

- `list_render_chunk_positions_blocking` lists chunks that have render records.
- `list_render_chunk_positions_in_region_blocking` probes only a viewport or
  export region using key-only prefix scans.
- `load_render_chunk_blocking`, `load_render_chunks_blocking`, and
  `load_render_region_blocking` accept `RenderChunkLoadOptions` /
  `RenderRegionLoadOptions` with `threading`, `pipeline`, `cancel`,
  `progress`, and `priority` policies.

Async wrappers with the same names are available behind the default `async`
feature. They use `spawn_blocking` and preserve cancellation/progress options.
The blocking implementation uses bounded local parallelism, not Rayon global
pool state.

```rust
let region = bedrock_world::RenderChunkRegion {
    dimension,
    min_chunk_x: -32,
    min_chunk_z: -32,
    max_chunk_x: 31,
    max_chunk_z: 31,
};
let positions = world.list_render_chunk_positions_in_region(
    region,
    bedrock_world::WorldScanOptions {
        pipeline: bedrock_world::WorldPipelineOptions {
            queue_depth: 64,
            ..Default::default()
        },
        ..Default::default()
    },
).await?;
let chunks = world.load_render_chunks(
    positions,
    bedrock_world::RenderChunkLoadOptions {
        priority: bedrock_world::RenderChunkPriority::DistanceFrom {
            chunk_x: 0,
            chunk_z: 0,
        },
        ..Default::default()
    },
).await?;
```

`load_render_region_blocking` returns `RenderRegionData { region, chunks, stats }`.
Use `stats.worker_threads`, `stats.queue_wait_ms`, and
`stats.subchunks_decoded` to tune worker budgets without baking fixed time
thresholds into tests.

`RenderChunkLoadOptions::request` selects one terrain contract, preventing
callers from mixing incompatible surface, raw-height, layer, and biome flags:

- `RenderChunkRequest::ExactSurface` loads block data and computes
  `RenderChunkData::column_samples` by scanning each X/Z column top-down.
  Surface block/height, relief support block/height, thin overlay, water
  context, biome, and source are derived from actual blocks, not raw heightmap
  records.
- `RenderChunkRequest::RawHeightMap` keeps the raw Data2D/Data3D or
  LegacyTerrain heightmap path for diagnostics and does not build surface
  samples.
- `RenderChunkRequest::Layer { y }` and `RenderChunkRequest::Biome { y, .. }`
  load only the data needed for fixed layer/cave or biome reads.

Raw heightmaps remain available through `RenderChunkData::height_map`, but map
renderers should treat them as hints or diagnostics. Stats expose
`computed_surface_columns`, `raw_height_mismatch_columns`,
`missing_subchunk_columns`, `legacy_fallback_columns`,
`legacy_biome_preferred_columns`, and `modern_biome_fallback_columns`.

For old LevelDB worlds, render exact-batch loading always requests
`ChunkRecordTag::LegacyTerrain` (`0x30`). If that record exists,
`RenderChunkData::legacy_terrain` is populated, the chunk is considered loaded
even without `Data2D`/`SubChunkPrefix`, and `RenderLoadStats::prefix_scans`
remains `0`.
Legacy biome samples are exposed through `RenderChunkData::legacy_biomes` as
`[biome_id, red, green, blue]`; `legacy_biome_colors` is retained only as a
compatibility `0x00RRGGBB` view. When both `LegacyTerrain` biome samples and
old Data2D/Data3D biome ids exist, exact surface sampling prefers the saved
legacy RGB sample and treats the numeric biome id as fallback only.

Exact render chunk batches preserve the input key/value association after
deduplication, priority sorting, and parallel decode. Regression fixtures should
shuffle and duplicate `ChunkPos` values, then assert each returned
`RenderChunkData.pos` still carries the matching block, height, and biome
sentinels.

`PocketChunksDatStorage` is a read-only `WorldStorage` backend. It maps each
old 82,176-byte `chunks.dat` terrain payload to an 83,200-byte virtual
`LegacyTerrain` record by appending default legacy biome samples. `put`,
`delete`, and `write_batch` return `UnsupportedChunkFormat`.

## Typed Bedrock Records

v0.2 adds typed APIs for BedrockLevelFormat records that map editors usually
need without forcing a full-world parse. The storage boundary remains raw
key/value: `bedrock-world` owns Bedrock key classification, NBT codecs,
coordinate validation, and write roundtrip checks.

Key helpers:

- `MapRecordId` validates and encodes `map_<id>` keys.
- `GlobalRecordKind` classifies `mobevents`, `Overworld`, `Nether`, `TheEnd`,
  `scoreboard`, `LocalPlayer`, `AutonomousEntities`, and preserved unknown
  global names.
- `ActorUid` and `ActorDigestKey` encode `actorprefix<uid>` and
  `digp<x><z>[dimension]`.

Map and global records:

- `read_map_record_blocking`, `scan_map_records_blocking`,
  `write_map_record_blocking`, and `delete_map_record_blocking`.
- `read_global_record_blocking`, `scan_global_records_blocking`,
  `write_global_record_blocking`, and `delete_global_record_blocking`.
- Async wrappers with the same names are available behind the default `async`
  feature.

`ParsedMapData` now carries the validated `record_id`, parsed NBT `roots`,
`known_fields`, optional `MapPixels`, and raw bytes for preservation. The core
crate exposes map pixel buffers only; PNG/image export belongs in an adapter or
feature crate.

Chunk payload helpers:

- `get_heightmap_blocking`, `put_heightmap_blocking`, and
  `put_biome_storage_blocking` cover Data2D/Data3D heightmap and biome storage.
- `scan_hsa_records_blocking`, `put_hsa_for_chunk_blocking`, and
  `delete_hsa_for_chunk_blocking` cover Hardcoded Spawn Areas.
- `block_entities_in_chunk_blocking`, `put_block_entities_blocking`,
  `edit_block_entity_at_blocking`, and `delete_block_entity_at_blocking` cover
  consecutive block-entity NBT compounds.
- `actors_in_chunk_blocking`, `put_actor_blocking`, `delete_actor_blocking`,
  and `move_actor_blocking` cover legacy inline `Entity` reads and modern
  `digp -> actorprefix` writes.

All high-level writes validate the serialized value by parsing it back before
committing. Actor writes update `actorprefix` and `digp` in one transaction.
Block-entity writes reject coordinates outside the target chunk. `chunks.dat`
backends stay read-only.

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
