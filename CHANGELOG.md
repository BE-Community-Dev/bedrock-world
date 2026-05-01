# Changelog

All notable changes to `bedrock-world` are tracked here.

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
