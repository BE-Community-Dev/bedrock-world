# Changelog

All notable changes to `bedrock-world` are tracked here.

## 0.2.0 - Unreleased

### Added

- Added render-only chunk position indexes:
  `list_render_chunk_positions_blocking` and
  `list_render_chunk_positions_in_region_blocking`, plus async wrappers.
- Added async wrappers for render chunk, render chunk batch, render region, and
  render chunk position loading.
- Added `threading` to `RenderChunkLoadOptions` and
  `RenderRegionLoadOptions`, allowing bounded parallel render chunk loading.
- Added `WorldPipelineOptions` for queue depth, chunk batch sizing, subchunk
  decode worker budgeting, and scan progress cadence.
- Added `RenderChunkPriority::{RowMajor, DistanceFrom { .. }}` so viewport
  loaders can deliver chunks closest to the current view first.
- Added `cancel`, `progress`, and `priority` to render chunk and render region
  load options.
- Added `RenderRegionData::stats` / `RenderLoadStats` with requested/loaded
  chunk counts, subchunk decode counts, worker counts, queue wait, and load time.
- Added `log` facade diagnostics for render region indexing and render chunk
  worker execution.

### Breaking Changes

- `WorldScanOptions`, `RenderChunkLoadOptions`, and `RenderRegionLoadOptions`
  now include pipeline/cancel/progress/priority fields. Use
  `..Default::default()` or set `WorldThreadingOptions::Single` when an outer
  renderer already schedules work.

### Migration Notes

- Replace full-world chunk scans used only for visible map rendering with
  `list_render_chunk_positions_in_region_blocking` or its async wrapper.
- Use `load_render_chunks_blocking`/`load_render_chunks` for viewport batches
  instead of repeatedly loading single chunks on the UI thread.
- For interactive maps, set `RenderChunkPriority::DistanceFrom` to the center
  chunk of the visible viewport and inspect `RenderRegionData::stats` before
  increasing worker counts.

## 0.1.0 - 2026-05-01

### Added

- Initial public crate-ready world API for `level.dat`, little-endian NBT,
  Bedrock DB key classification, player reads, chunk/subchunk parsing, entity
  and block-entity parsing, item extraction, biome summaries, maps, villages,
  globals, and scan progress/cancellation.
- Legacy LevelDB-era chunk parsing for `LegacyTerrain` records and
  pre-paletted `SubChunkPrefix` payloads, including block ID, metadata, and
  optional light arrays.
- Paletted subchunk parsing for old single-storage v1 payloads and modern
  v8/v9 storage-count payloads.
- Lazy `BedrockWorld` APIs backed by `bedrock-leveldb`, async wrappers,
  benches, fixture tests, and English/Simplified Chinese documentation.
- Stable `BedrockWorldErrorKind` categories for application error handling.
- Read-only enforcement for batched world transactions and read-only LevelDB
  opens through `BedrockWorld::open`.
- Optional large-world fixture policy, contribution notes, API/testing guides,
  and dual MIT/Apache-2.0 license files.

### Notes

- Full semantic migration of historical gameplay data remains intentionally
  scoped to structured chunk/world parsers, not the lower-level storage crate.
- Complete structured editing for every chunk version is not implemented in this
  release.
