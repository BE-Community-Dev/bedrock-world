//! Tools for inspecting and editing Minecraft Bedrock `LevelDB` worlds.
//!
//! `bedrock-world` focuses on world-level concepts layered above
//! `bedrock-leveldb`: `level.dat`, little-endian Bedrock NBT, chunk and
//! subchunk decoding, player records, entity summaries, biome data, map/village
//! records, and scan-oriented APIs for launchers or offline tools.
//!
//! The default APIs are deliberately lazy. Opening a [`BedrockWorld`] does not
//! parse the full database; callers choose targeted operations such as
//! [`read_level_dat`], [`BedrockWorld::list_players_blocking`],
//! [`BedrockWorld::parse_chunk_blocking`], or [`BedrockWorld::scan_items_blocking`].
//! Async wrappers use `tokio::task::spawn_blocking` so `LevelDB` and NBT work does
//! not run on foreground async tasks.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::items_after_test_module,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::struct_excessive_bools,
    clippy::type_complexity,
    clippy::wildcard_imports
)]

/// Chunk keys, subchunk formats, palette data, and legacy terrain helpers.
pub mod chunk;
/// Filesystem discovery for Bedrock world folders.
pub mod discover;
/// Crate-wide error and result types.
pub mod error;
/// `level.dat` header parsing, validation, and atomic write helpers.
pub mod level_dat;
/// Little-endian Bedrock NBT reader and writer.
pub mod nbt;
/// Structured parsers for world, chunk, entity, biome, map, and village data.
pub mod parsed;
/// Player identifiers and raw player record helpers.
pub mod player;
/// Storage abstraction and LevelDB backend adapters.
pub mod storage;
/// High-level lazy world handle and scan/render helpers.
pub mod world;

pub use chunk::{
    BedrockDbKey, BlockPalette, BlockPos, BlockState, Chunk, ChunkKey, ChunkPos, ChunkRecord,
    ChunkRecordTag, ChunkVersion, Dimension, EntityData, LEGACY_SUBCHUNK_BLOCK_COUNT,
    LEGACY_SUBCHUNK_MIN_VALUE_LEN, LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN,
    LEGACY_TERRAIN_BLOCK_COUNT, LEGACY_TERRAIN_VALUE_LEN, LegacySubChunk, LegacyTerrain,
    ParsedVillageKey, SubChunk, SubChunkDecodeMode, SubChunkFormat, VillageRecordKind,
    block_storage_index,
};
pub use discover::{WorldDiscovery, WorldSummary, discover_worlds};
pub use error::{BedrockWorldError, BedrockWorldErrorKind, Result};
pub use level_dat::{
    LevelDatDocument, LevelDatHeader, LevelDatReadWarning, parse_level_dat_document,
    read_level_dat, read_level_dat_document, write_level_dat_atomic, write_level_dat_document,
};
#[cfg(feature = "async")]
pub use level_dat::{read_level_dat_async, write_level_dat_atomic_async};
pub use nbt::{NbtReader, NbtRef, NbtTag, NbtValue, NbtWriter};
pub use parsed::{
    ActorResolution, HardcodedSpawnAreaKind, ItemStack, ParsedActorDigest, ParsedBiomeData,
    ParsedBiomeStorage, ParsedBlockEntity, ParsedChunkData, ParsedChunkRecord,
    ParsedChunkRecordValue, ParsedDbEntry, ParsedDbValue, ParsedEntity, ParsedGlobalData,
    ParsedHardcodedSpawnArea, ParsedMapData, ParsedPlayer, ParsedVillageData, ParsedWorld,
    RetentionMode, WorldParseCategories, WorldParseOptions, WorldParseReport,
};
pub use player::{PlayerData, PlayerId};
pub use storage::{
    MemoryStorage, StorageBatch, StorageCancelFlag, StorageEntry, StorageOp, StorageProgressSink,
    StorageReadOptions, StorageScanMode, StorageScanOutcome, StorageScanProgress,
    StorageThreadingOptions, StorageVisitorControl, WorldStorage, backend::BedrockLevelDbStorage,
};
pub use world::{
    BedrockWorld, CancelFlag, ChunkBounds, OpenOptions, ProgressSink, RenderBlockEntity,
    RenderChunkData, RenderChunkLoadOptions, RenderChunkPriority, RenderChunkRegion,
    RenderLoadStats, RenderRegionData, RenderRegionLoadOptions, RenderSurfaceSubchunkMode,
    SurfaceColumn, SurfaceColumnOptions, WorldPipelineOptions, WorldScanOptions, WorldScanProgress,
    WorldThreadingOptions, WorldTransaction,
};
