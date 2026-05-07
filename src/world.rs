//! High-level lazy world access built on top of the storage layer.
//!
//! The methods in this module are intentionally split into blocking and async
//! forms. Blocking methods are the canonical implementation and are appropriate
//! for CLI tools, background worker threads, and tests. Async methods are thin
//! wrappers that offload the same work with `tokio::task::spawn_blocking`.

use crate::chunk::{
    ActorDigestKey, ActorUid, BedrockDbKey, BlockPos, BlockState, Chunk, ChunkKey, ChunkPos,
    ChunkRecord, ChunkRecordTag, ChunkVersion, GlobalRecordKind, LegacyBiomeSample, LegacyTerrain,
    MapRecordId, SubChunk, SubChunkDecodeMode, parse_subchunk_with_mode,
};
use crate::error::{BedrockWorldError, Result};
use crate::level_dat::{LevelDatDocument, read_level_dat_document, write_level_dat_document};
use crate::nbt::{NbtTag, parse_consecutive_root_nbt, parse_root_nbt, serialize_root_nbt};
use crate::parsed::{
    ActorRecord, ActorSource, Biome2d, Biome3d, BlockEntityRecord, HeightMap2d, ItemStack,
    ParsedBiomeData, ParsedBiomeStorage, ParsedBlockEntity, ParsedChunkData, ParsedDbEntry,
    ParsedDbValue, ParsedEntity, ParsedGlobalData, ParsedHardcodedSpawnArea, ParsedMapData,
    ParsedVillageData, ParsedWorld, WorldParseOptions, WorldParseReport, collect_item_stacks,
    encode_actor_digest_ids, encode_consecutive_roots, encode_global_record,
    encode_hardcoded_spawn_area_records, encode_map_record, parse_actor_digest_ids,
    parse_block_entities_from_value, parse_chunk_records, parse_chunk_records_with_options,
    parse_data3d, parse_entities_from_value, parse_global_record, parse_global_storage_entries,
    parse_hardcoded_spawn_area_records, parse_legacy_data2d, parse_map_record, parse_world_storage,
};
use crate::player::{PlayerData, PlayerId};
use crate::storage::backend::BedrockLevelDbStorage;
use crate::storage::{
    PocketChunksDatStorage, StorageBatch, StorageCancelFlag, StorageOp, StorageProgressSink,
    StorageReadOptions, StorageScanMode, StorageThreadingOptions, StorageVisitorControl,
    WorldStorage,
};
use bytes::Bytes;
use rayon::{ThreadPoolBuilder, prelude::*};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

/// Options used when opening or constructing a [`BedrockWorld`].
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Reject mutating operations when set.
    pub read_only: bool,
    /// Preferred world storage format. [`WorldFormatHint::Auto`] detects the
    /// backend from `db/CURRENT` and old `chunks.dat` files.
    pub format: WorldFormatHint,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read_only: true,
            format: WorldFormatHint::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorldFormatHint {
    #[default]
    Auto,
    LevelDb,
    PocketChunksDat,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorldFormat {
    #[default]
    LevelDb,
    LevelDbLegacyTerrain,
    PocketChunksDat,
}

/// Lazy handle to a Minecraft Bedrock world folder.
///
/// A handle stores the world path and a storage backend. It does not scan or
/// parse the database until a query method is called.
pub struct BedrockWorld<S = Arc<dyn WorldStorage>> {
    path: PathBuf,
    options: OpenOptions,
    storage: S,
    format: WorldFormat,
}

/// Storage handle accepted by generic [`BedrockWorld`] methods.
pub trait WorldStorageHandle: Clone + Send + Sync + 'static {
    fn storage(&self) -> &dyn WorldStorage;
}

impl<T> WorldStorageHandle for T
where
    T: WorldStorage + Clone + Send + Sync + 'static,
{
    fn storage(&self) -> &dyn WorldStorage {
        self
    }
}

impl<T> WorldStorageHandle for Arc<T>
where
    T: WorldStorage + 'static,
{
    fn storage(&self) -> &dyn WorldStorage {
        self.as_ref()
    }
}

impl WorldStorageHandle for Arc<dyn WorldStorage> {
    fn storage(&self) -> &dyn WorldStorage {
        self.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SurfaceColumnOptions {
    pub skip_air: bool,
    pub transparent_water: bool,
}

impl Default for SurfaceColumnOptions {
    fn default() -> Self {
        Self {
            skip_air: true,
            transparent_water: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceColumn {
    pub y: i32,
    pub block_name: String,
    pub biome_id: Option<u32>,
    pub water_depth: u8,
    pub under_water_block_name: Option<String>,
    pub is_fallback: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactSurfaceSubchunkPolicy {
    Full,
    HintThenVerify,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorldPipelineOptions {
    pub queue_depth: usize,
    pub chunk_batch_size: usize,
    pub subchunk_decode_workers: usize,
    pub progress_interval: usize,
}

impl WorldPipelineOptions {
    #[must_use]
    pub fn resolve_queue_depth(self, workers: usize, work_items: usize) -> usize {
        self.queue_depth
            .max(if self.queue_depth == 0 {
                workers
                    .max(1)
                    .saturating_mul(2)
                    .max(work_items.clamp(1, 256))
            } else {
                1
            })
            .max(1)
    }

    #[must_use]
    pub fn resolve_progress_interval(self) -> usize {
        self.progress_interval
            .max(if self.progress_interval == 0 { 256 } else { 1 })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RenderChunkPriority {
    #[default]
    RowMajor,
    DistanceFrom {
        chunk_x: i32,
        chunk_z: i32,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExactSurfaceBiomeLoad {
    None,
    #[default]
    TopColumns,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderChunkRequest {
    ExactSurface {
        subchunks: ExactSurfaceSubchunkPolicy,
        biome: ExactSurfaceBiomeLoad,
        block_entities: bool,
    },
    RawHeightMap,
    Layer {
        y: i32,
    },
    Biome {
        y: i32,
        load_all: bool,
    },
}

impl Default for RenderChunkRequest {
    fn default() -> Self {
        Self::ExactSurface {
            subchunks: ExactSurfaceSubchunkPolicy::Full,
            biome: ExactSurfaceBiomeLoad::TopColumns,
            block_entities: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerrainSampleSource {
    Subchunk,
    LegacyTerrain,
    LegacyFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerrainColumnBiome {
    Id(u32),
    Legacy(LegacyBiomeSample),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerrainSurfaceRole {
    Air,
    Water,
    Overlay,
    Primary,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerrainColumnOverlay {
    pub y: i16,
    pub block_state: BlockState,
    pub source: TerrainSampleSource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerrainColumnWater {
    pub surface_y: i16,
    pub block_state: BlockState,
    pub depth: u8,
    pub underwater_y: Option<i16>,
    pub underwater_block_state: Option<BlockState>,
    pub source: TerrainSampleSource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerrainColumnSample {
    pub surface_y: i16,
    pub surface_block_state: BlockState,
    pub relief_y: i16,
    pub relief_block_state: BlockState,
    pub overlay: Option<TerrainColumnOverlay>,
    pub water: Option<TerrainColumnWater>,
    pub biome: Option<TerrainColumnBiome>,
    pub source: TerrainSampleSource,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TerrainColumnSamples {
    columns: Vec<Option<TerrainColumnSample>>,
}

impl TerrainColumnSamples {
    #[must_use]
    pub fn new() -> Self {
        Self {
            columns: vec![None; 16 * 16],
        }
    }

    #[must_use]
    pub fn get(&self, local_x: u8, local_z: u8) -> Option<&TerrainColumnSample> {
        self.columns
            .get(column_index(local_x, local_z)?)
            .and_then(Option::as_ref)
    }

    pub fn set(&mut self, local_x: u8, local_z: u8, sample: TerrainColumnSample) {
        if let Some(index) = column_index(local_x, local_z)
            && let Some(slot) = self.columns.get_mut(index)
        {
            *slot = Some(sample);
        }
    }

    #[must_use]
    pub fn sampled_columns(&self) -> usize {
        self.columns
            .iter()
            .filter(|sample| sample.is_some())
            .count()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TerrainColumnSample> {
        self.columns.iter().filter_map(Option::as_ref)
    }
}

impl Default for TerrainColumnSamples {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RenderLoadStats {
    pub requested_chunks: usize,
    pub loaded_chunks: usize,
    pub subchunks_decoded: usize,
    pub worker_threads: usize,
    pub queue_wait_ms: u128,
    pub load_ms: u128,
    pub keys_requested: usize,
    pub keys_found: usize,
    pub exact_get_batches: usize,
    pub prefix_scans: usize,
    pub decode_ms: u128,
    pub db_read_ms: u128,
    pub biome_parse_ms: u128,
    pub subchunk_parse_ms: u128,
    pub surface_scan_ms: u128,
    pub block_entity_parse_ms: u128,
    pub full_reload_ms: u128,
    pub legacy_terrain_records: usize,
    pub legacy_biome_samples: usize,
    pub legacy_biome_colors: usize,
    pub terrain_source_legacy: usize,
    pub terrain_source_subchunk: usize,
    pub legacy_pocket_chunks: usize,
    pub detected_format: WorldFormat,
    pub computed_surface_columns: usize,
    pub raw_height_mismatch_columns: usize,
    pub missing_subchunk_columns: usize,
    pub legacy_fallback_columns: usize,
    pub legacy_biome_preferred_columns: usize,
    pub modern_biome_fallback_columns: usize,
}

#[derive(Debug, Clone)]
pub struct RenderChunkLoadOptions {
    pub request: RenderChunkRequest,
    pub subchunk_decode: SubChunkDecodeMode,
    pub threading: WorldThreadingOptions,
    pub pipeline: WorldPipelineOptions,
    pub cancel: Option<CancelFlag>,
    pub progress: Option<ProgressSink>,
    pub priority: RenderChunkPriority,
}

impl Default for RenderChunkLoadOptions {
    fn default() -> Self {
        Self {
            request: RenderChunkRequest::default(),
            subchunk_decode: SubChunkDecodeMode::FullIndices,
            threading: WorldThreadingOptions::Auto,
            pipeline: WorldPipelineOptions::default(),
            cancel: None,
            progress: None,
            priority: RenderChunkPriority::RowMajor,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenderBlockEntity {
    pub id: Option<String>,
    pub position: Option<[i32; 3]>,
    pub nbt: NbtTag,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenderChunkData {
    pub pos: ChunkPos,
    pub is_loaded: bool,
    pub height_map: Option<[[Option<i16>; 16]; 16]>,
    pub legacy_biomes: Option<[[Option<LegacyBiomeSample>; 16]; 16]>,
    pub legacy_biome_colors: Option<[[Option<u32>; 16]; 16]>,
    pub biome_data: BTreeMap<i32, ParsedBiomeStorage>,
    pub subchunks: BTreeMap<i8, SubChunk>,
    pub block_entities: Vec<RenderBlockEntity>,
    pub legacy_terrain: Option<LegacyTerrain>,
    pub column_samples: Option<TerrainColumnSamples>,
    pub version: crate::ChunkVersion,
}

impl RenderChunkData {
    #[must_use]
    pub fn column_sample_at(&self, local_x: u8, local_z: u8) -> Option<&TerrainColumnSample> {
        self.column_samples.as_ref()?.get(local_x, local_z)
    }
}

#[derive(Debug, Clone)]
struct RawRenderChunkData {
    pos: ChunkPos,
    biome_record: Option<(crate::ChunkVersion, Bytes)>,
    subchunks: BTreeMap<i8, Bytes>,
    block_entities: Option<Bytes>,
    legacy_terrain: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)]
struct RenderChunkDecodeTiming {
    biome_parse_ms: u128,
    subchunk_parse_ms: u128,
    surface_scan_ms: u128,
    block_entity_parse_ms: u128,
}

impl RenderChunkDecodeTiming {
    fn add(&mut self, other: Self) {
        self.biome_parse_ms = self.biome_parse_ms.saturating_add(other.biome_parse_ms);
        self.subchunk_parse_ms = self
            .subchunk_parse_ms
            .saturating_add(other.subchunk_parse_ms);
        self.surface_scan_ms = self.surface_scan_ms.saturating_add(other.surface_scan_ms);
        self.block_entity_parse_ms = self
            .block_entity_parse_ms
            .saturating_add(other.block_entity_parse_ms);
    }
}

#[derive(Debug, Clone, Copy)]
enum RenderRecordKind {
    LegacyTerrain,
    Data3D,
    Data2D,
    Data2DLegacy,
    Subchunk(i8),
    BlockEntity,
}

#[derive(Debug, Clone, Copy)]
struct RenderRecordRequest {
    chunk_index: usize,
    kind: RenderRecordKind,
}

#[derive(Debug, Clone)]
pub struct RenderRegionLoadOptions {
    pub request: RenderChunkRequest,
    pub subchunk_decode: SubChunkDecodeMode,
    pub threading: WorldThreadingOptions,
    pub pipeline: WorldPipelineOptions,
    pub cancel: Option<CancelFlag>,
    pub progress: Option<ProgressSink>,
    pub priority: RenderChunkPriority,
}

impl Default for RenderRegionLoadOptions {
    fn default() -> Self {
        Self {
            request: RenderChunkRequest::default(),
            subchunk_decode: SubChunkDecodeMode::FullIndices,
            threading: WorldThreadingOptions::Auto,
            pipeline: WorldPipelineOptions::default(),
            cancel: None,
            progress: None,
            priority: RenderChunkPriority::RowMajor,
        }
    }
}

impl From<RenderRegionLoadOptions> for RenderChunkLoadOptions {
    fn from(options: RenderRegionLoadOptions) -> Self {
        Self {
            request: options.request,
            subchunk_decode: options.subchunk_decode,
            threading: options.threading,
            pipeline: options.pipeline,
            cancel: options.cancel,
            progress: options.progress,
            priority: options.priority,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenderChunkRegion {
    pub dimension: crate::Dimension,
    pub min_chunk_x: i32,
    pub min_chunk_z: i32,
    pub max_chunk_x: i32,
    pub max_chunk_z: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RenderRegionData {
    pub region: RenderChunkRegion,
    pub chunks: Vec<RenderChunkData>,
    pub stats: RenderLoadStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkBounds {
    pub dimension: crate::Dimension,
    pub min_chunk_x: i32,
    pub min_chunk_z: i32,
    pub max_chunk_x: i32,
    pub max_chunk_z: i32,
    pub chunk_count: usize,
}

impl ChunkBounds {
    fn from_first(pos: ChunkPos) -> Self {
        Self {
            dimension: pos.dimension,
            min_chunk_x: pos.x,
            min_chunk_z: pos.z,
            max_chunk_x: pos.x,
            max_chunk_z: pos.z,
            chunk_count: 1,
        }
    }

    fn include(&mut self, pos: ChunkPos) {
        self.min_chunk_x = self.min_chunk_x.min(pos.x);
        self.min_chunk_z = self.min_chunk_z.min(pos.z);
        self.max_chunk_x = self.max_chunk_x.max(pos.x);
        self.max_chunk_z = self.max_chunk_z.max(pos.z);
        self.chunk_count = self.chunk_count.saturating_add(1);
    }
}

#[derive(Debug, Clone)]
pub struct WorldScanOptions {
    pub threading: WorldThreadingOptions,
    pub pipeline: WorldPipelineOptions,
    pub cancel: Option<CancelFlag>,
    pub progress: Option<ProgressSink>,
}

impl Default for WorldScanOptions {
    fn default() -> Self {
        Self {
            threading: WorldThreadingOptions::Auto,
            pipeline: WorldPipelineOptions::default(),
            cancel: None,
            progress: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WorldThreadingOptions {
    #[default]
    Auto,
    Fixed(usize),
    Single,
}

pub const MAX_WORLD_THREADS: usize = 512;

impl WorldThreadingOptions {
    #[must_use]
    pub fn resolve(self, work_items: usize) -> usize {
        self.resolve_unchecked(work_items)
    }

    #[must_use]
    pub fn resolve_unchecked(self, work_items: usize) -> usize {
        match self {
            Self::Single => 1,
            Self::Fixed(threads) => threads.clamp(1, MAX_WORLD_THREADS),
            Self::Auto => std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(work_items.max(1)),
        }
    }

    pub fn resolve_checked(self, work_items: usize) -> Result<usize> {
        match self {
            Self::Fixed(0) => Err(BedrockWorldError::Validation(
                "thread count must be in 1..=512".to_string(),
            )),
            Self::Fixed(threads) if threads > MAX_WORLD_THREADS => Err(
                BedrockWorldError::Validation("thread count must be in 1..=512".to_string()),
            ),
            _ => Ok(self.resolve_unchecked(work_items)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn from_shared(cancelled: Arc<AtomicBool>) -> Self {
        Self(cancelled)
    }

    #[must_use]
    pub fn to_storage_cancel(&self) -> StorageCancelFlag {
        StorageCancelFlag::from_shared(Arc::clone(&self.0))
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct ProgressSink {
    inner: Arc<Mutex<Box<dyn FnMut(WorldScanProgress) + Send>>>,
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProgressSink")
            .finish_non_exhaustive()
    }
}

impl ProgressSink {
    #[must_use]
    pub fn new(callback: impl FnMut(WorldScanProgress) + Send + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(callback))),
        }
    }

    fn emit(&self, progress: WorldScanProgress) {
        if let Ok(mut callback) = self.inner.lock() {
            callback(progress);
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorldScanProgress {
    pub entries_seen: usize,
}

impl BedrockWorld<Arc<dyn WorldStorage>> {
    pub fn open_blocking(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let format = detect_world_format(&path, options.format)?;
        let storage: Arc<dyn WorldStorage> = match format {
            WorldFormat::LevelDb | WorldFormat::LevelDbLegacyTerrain => {
                let db_path = path.join("db");
                if options.read_only {
                    Arc::new(BedrockLevelDbStorage::open_read_only(db_path)?)
                } else {
                    Arc::new(BedrockLevelDbStorage::open(db_path)?)
                }
            }
            WorldFormat::PocketChunksDat => {
                if !options.read_only {
                    log::warn!(
                        "opening legacy chunks.dat world as read-only despite read_only=false"
                    );
                }
                Arc::new(PocketChunksDatStorage::open(&path)?)
            }
        };
        log::debug!(
            "opened Bedrock world (path={}, format={:?}, read_only={})",
            path.display(),
            format,
            options.read_only
        );
        Ok(Self {
            path,
            options,
            storage,
            format,
        })
    }

    #[cfg(feature = "async")]
    pub async fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || Self::open_blocking(path, options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[must_use]
    pub fn from_storage(
        path: impl Into<PathBuf>,
        storage: Arc<dyn WorldStorage>,
        options: OpenOptions,
    ) -> Self {
        Self {
            path: path.into(),
            options,
            storage,
            format: WorldFormat::LevelDb,
        }
    }

    #[must_use]
    pub fn from_storage_with_format(
        path: impl Into<PathBuf>,
        storage: Arc<dyn WorldStorage>,
        options: OpenOptions,
        format: WorldFormat,
    ) -> Self {
        Self {
            path: path.into(),
            options,
            storage,
            format,
        }
    }
}

impl BedrockWorld<BedrockLevelDbStorage> {
    pub fn open_typed_blocking(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let format = detect_world_format(&path, options.format)?;
        match format {
            WorldFormat::LevelDb | WorldFormat::LevelDbLegacyTerrain => {
                let db_path = path.join("db");
                let storage = if options.read_only {
                    BedrockLevelDbStorage::open_read_only(db_path)?
                } else {
                    BedrockLevelDbStorage::open(db_path)?
                };
                Ok(Self {
                    path,
                    options,
                    storage,
                    format,
                })
            }
            WorldFormat::PocketChunksDat => Err(BedrockWorldError::UnsupportedChunkFormat(
                "typed LevelDB open does not support legacy chunks.dat worlds".to_string(),
            )),
        }
    }
}

impl<S> BedrockWorld<S>
where
    S: WorldStorageHandle,
{
    #[must_use]
    pub fn from_typed_storage(path: impl Into<PathBuf>, storage: S, options: OpenOptions) -> Self {
        Self {
            path: path.into(),
            options,
            storage,
            format: WorldFormat::LevelDb,
        }
    }

    #[must_use]
    pub fn from_typed_storage_with_format(
        path: impl Into<PathBuf>,
        storage: S,
        options: OpenOptions,
        format: WorldFormat,
    ) -> Self {
        Self {
            path: path.into(),
            options,
            storage,
            format,
        }
    }

    #[must_use]
    pub fn storage(&self) -> &dyn WorldStorage {
        self.storage.storage()
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub const fn format(&self) -> WorldFormat {
        self.format
    }

    pub fn read_level_dat_blocking(&self) -> Result<LevelDatDocument> {
        read_level_dat_document(&self.path.join("level.dat"))
    }

    pub fn write_level_dat_blocking(&self, document: &LevelDatDocument) -> Result<()> {
        self.ensure_writable()?;
        write_level_dat_document(&self.path.join("level.dat"), document)
    }

    pub fn list_players_blocking(&self) -> Result<Vec<PlayerId>> {
        let mut players = Vec::new();
        if self.storage().get(b"~local_player")?.is_some() {
            players.push(PlayerId::Local);
        }
        self.storage().for_each_prefix(
            b"player_",
            StorageReadOptions::default(),
            &mut |key, _value| {
                if let Some(player) = PlayerId::from_storage_key(key) {
                    players.push(player);
                }
                Ok(StorageVisitorControl::Continue)
            },
        )?;
        Ok(players)
    }

    pub fn classify_keys_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<BTreeMap<String, usize>> {
        let mut counts = BTreeMap::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_key(to_storage_read_options(&options), &mut |key| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                let key = BedrockDbKey::decode(key);
                *counts.entry(key.summary_kind()).or_default() += 1;
                Ok(StorageVisitorControl::Continue)
            })?;
        emit_progress(&options, entries_seen);
        Ok(counts)
    }

    pub fn list_chunk_positions_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ChunkPos>> {
        let mut positions = BTreeSet::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_key(to_storage_read_options(&options), &mut |key| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key) {
                    positions.insert(chunk_key.pos);
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(positions.into_iter().collect())
    }

    pub fn list_render_chunk_positions_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ChunkPos>> {
        let started = Instant::now();
        log::debug!(
            "listing render chunk positions (threading={:?}, queue_depth={}, progress_interval={})",
            options.threading,
            options.pipeline.queue_depth,
            options.pipeline.progress_interval
        );
        let mut positions = BTreeSet::new();
        let mut entries_seen = 0usize;
        let outcome =
            self.storage()
                .for_each_key(to_storage_read_options(&options), &mut |key| {
                    check_cancelled(&options)?;
                    entries_seen = entries_seen.saturating_add(1);
                    if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                        && chunk_key.tag.is_render_chunk_record()
                    {
                        positions.insert(chunk_key.pos);
                    }
                    if entries_seen.is_multiple_of(8192) {
                        emit_progress(&options, entries_seen);
                    }
                    Ok(StorageVisitorControl::Continue)
                })?;
        let positions = positions.into_iter().collect::<Vec<_>>();
        log::debug!(
            "render chunk position listing complete (entries_seen={}, positions={}, visited={}, tables_scanned={}, worker_threads={}, queue_wait_ms={}, cancel_checks={}, elapsed_ms={})",
            entries_seen,
            positions.len(),
            outcome.visited,
            outcome.tables_scanned,
            outcome.worker_threads,
            outcome.queue_wait_ms,
            outcome.cancel_checks,
            started.elapsed().as_millis()
        );
        Ok(positions)
    }

    #[allow(clippy::too_many_lines)]
    pub fn list_render_chunk_positions_in_region_blocking(
        &self,
        region: RenderChunkRegion,
        options: WorldScanOptions,
    ) -> Result<Vec<ChunkPos>> {
        let started = Instant::now();
        validate_render_region(region)?;
        let x_count = i64::from(region.max_chunk_x) - i64::from(region.min_chunk_x) + 1;
        let z_count = i64::from(region.max_chunk_z) - i64::from(region.min_chunk_z) + 1;
        let capacity = usize::try_from(x_count.saturating_mul(z_count))
            .map_err(|_| BedrockWorldError::Validation("render region is too large".to_string()))?;
        let mut positions = Vec::with_capacity(capacity);
        for z in region.min_chunk_z..=region.max_chunk_z {
            for x in region.min_chunk_x..=region.max_chunk_x {
                positions.push(ChunkPos {
                    x,
                    z,
                    dimension: region.dimension,
                });
            }
        }
        if positions.is_empty() {
            return Ok(Vec::new());
        }

        let worker_count = options.threading.resolve_checked(positions.len())?;
        log::debug!(
            "indexing render chunk region (dimension={:?}, min=({}, {}), max=({}, {}), workers={})",
            region.dimension,
            region.min_chunk_x,
            region.min_chunk_z,
            region.max_chunk_x,
            region.max_chunk_z,
            worker_count
        );
        if worker_count == 1 {
            let render_positions = positions
                .into_iter()
                .filter_map(
                    |pos| match self.has_render_chunk_records_blocking(pos, &options) {
                        Ok(true) => Some(Ok(pos)),
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            log::debug!(
                "render chunk region index complete (dimension={:?}, candidates={}, positions={}, workers={}, queue_depth=0, elapsed_ms={})",
                region.dimension,
                capacity,
                render_positions.len(),
                worker_count,
                started.elapsed().as_millis()
            );
            return Ok(render_positions);
        }

        let scan_options = WorldScanOptions {
            threading: WorldThreadingOptions::Single,
            pipeline: options.pipeline,
            cancel: options.cancel.clone(),
            progress: options.progress.clone(),
        };
        let next_position = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let queue_depth = options
            .pipeline
            .resolve_queue_depth(worker_count, positions.len());
        let (sender, receiver) = mpsc::sync_channel::<Result<Option<ChunkPos>>>(queue_depth);
        let pool = world_pool(worker_count)?;
        pool.scope(|scope| {
            for worker_index in 0..worker_count {
                let next_position = Arc::clone(&next_position);
                let sender = sender.clone();
                let positions = &positions;
                let scan_options = scan_options.clone();
                scope.spawn(move |_| {
                    log::trace!("render region index worker {worker_index} started");
                    loop {
                        if scan_options
                            .cancel
                            .as_ref()
                            .is_some_and(CancelFlag::is_cancelled)
                        {
                            return;
                        }
                        let index = next_position.fetch_add(1, Ordering::Relaxed);
                        let Some(pos) = positions.get(index).copied() else {
                            log::trace!("render region index worker {worker_index} finished");
                            return;
                        };
                        let result = self
                            .has_render_chunk_records_blocking(pos, &scan_options)
                            .map(|is_renderable| is_renderable.then_some(pos));
                        if sender.send(result).is_err() {
                            return;
                        }
                    }
                });
            }
            drop(sender);

            let mut render_positions = Vec::new();
            for result in receiver {
                if let Some(pos) = result? {
                    render_positions.push(pos);
                }
            }
            render_positions.sort();
            log::debug!(
                "render chunk region index complete (dimension={:?}, candidates={}, positions={}, workers={}, queue_depth={}, elapsed_ms={})",
                region.dimension,
                positions.len(),
                render_positions.len(),
                worker_count,
                queue_depth,
                started.elapsed().as_millis()
            );
            Ok(render_positions)
        })
    }

    pub fn discover_chunk_bounds_blocking(
        &self,
        dimension: crate::Dimension,
        options: WorldScanOptions,
    ) -> Result<Option<ChunkBounds>> {
        let mut bounds: Option<ChunkBounds> = None;
        let mut seen_positions = BTreeSet::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_key(to_storage_read_options(&options), &mut |key| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                    && chunk_key.pos.dimension == dimension
                    && seen_positions.insert(chunk_key.pos)
                {
                    match &mut bounds {
                        Some(bounds) => bounds.include(chunk_key.pos),
                        None => bounds = Some(ChunkBounds::from_first(chunk_key.pos)),
                    }
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(bounds)
    }

    pub fn nearest_loaded_chunk_to_spawn_blocking(
        &self,
        dimension: crate::Dimension,
        spawn_block_x: i32,
        spawn_block_z: i32,
        options: WorldScanOptions,
    ) -> Result<Option<ChunkPos>> {
        let spawn_chunk = BlockPos {
            x: spawn_block_x,
            y: 0,
            z: spawn_block_z,
        }
        .to_chunk_pos(dimension);
        let mut best = None::<(i64, ChunkPos)>;
        let mut seen_positions = BTreeSet::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_key(to_storage_read_options(&options), &mut |key| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                    && chunk_key.pos.dimension == dimension
                    && seen_positions.insert(chunk_key.pos)
                {
                    let dx = i64::from(chunk_key.pos.x) - i64::from(spawn_chunk.x);
                    let dz = i64::from(chunk_key.pos.z) - i64::from(spawn_chunk.z);
                    let distance = dx.saturating_mul(dx).saturating_add(dz.saturating_mul(dz));
                    if best.is_none_or(|(best_distance, _)| distance < best_distance) {
                        best = Some((distance, chunk_key.pos));
                    }
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(best.map(|(_, pos)| pos))
    }

    pub fn get_player_blocking(&self, id: &PlayerId) -> Result<Option<PlayerData>> {
        let Some(key) = id.storage_key() else {
            if *id == PlayerId::LegacyLevelDat {
                let document = self.read_level_dat_blocking()?;
                return Ok(Some(PlayerData::from_nbt(id.clone(), document.root)?));
            }
            return Ok(None);
        };
        self.storage()
            .get(key.as_ref())?
            .map(|bytes| PlayerData::from_raw(id.clone(), bytes))
            .transpose()
    }

    pub fn put_player_blocking(&self, player: &PlayerData) -> Result<()> {
        self.ensure_writable()?;
        let Some(key) = player.id.storage_key() else {
            return Err(BedrockWorldError::Validation(
                "player id has no LevelDB key".to_string(),
            ));
        };
        self.storage().put(key.as_ref(), &player.raw)
    }

    pub fn get_chunk_blocking(&self, pos: ChunkPos) -> Result<Chunk> {
        let mut records = Vec::new();
        let prefix = chunk_record_prefix(pos);
        self.storage().for_each_prefix(
            &prefix,
            StorageReadOptions::default(),
            &mut |raw_key, value| {
                if let Ok(key) = ChunkKey::decode(raw_key) {
                    if key.pos == pos {
                        records.push(ChunkRecord {
                            key,
                            value: value.clone(),
                        });
                    }
                }
                Ok(StorageVisitorControl::Continue)
            },
        )?;
        let version = records
            .iter()
            .find(|record| record.key.tag == ChunkRecordTag::Version)
            .and_then(|record| record.value.first().copied());
        Ok(Chunk {
            pos,
            version,
            records,
        })
    }

    pub fn get_subchunk_blocking(&self, pos: ChunkPos, y: i8) -> Result<Option<crate::SubChunk>> {
        self.get_chunk_blocking(pos)?.get_subchunk(y)
    }

    pub fn parse_world_blocking(&self, options: WorldParseOptions) -> Result<ParsedWorld> {
        let level_dat = self.read_level_dat_blocking()?;
        parse_world_storage(level_dat, self.storage(), options)
    }

    pub fn parse_chunk_blocking(&self, pos: ChunkPos) -> Result<ParsedChunkData> {
        let chunk = self.get_chunk_blocking(pos)?;
        Ok(parse_chunk_records(pos, chunk.records))
    }

    pub fn parse_chunk_with_options_blocking(
        &self,
        pos: ChunkPos,
        options: WorldParseOptions,
    ) -> Result<ParsedChunkData> {
        let chunk = self.get_chunk_blocking(pos)?;
        Ok(parse_chunk_records_with_options(
            pos,
            chunk.records,
            options,
        ))
    }

    pub fn parse_subchunk_blocking(
        &self,
        pos: ChunkPos,
        y: i8,
        options: WorldParseOptions,
    ) -> Result<Option<crate::SubChunk>> {
        let key = ChunkKey::subchunk(pos, y);
        self.storage()
            .get(&key.encode())?
            .map(|value| parse_subchunk_with_mode(y, value, options.subchunk_decode_mode))
            .transpose()
    }

    pub fn get_biome_storage_blocking(
        &self,
        pos: ChunkPos,
        y: i32,
    ) -> Result<Option<ParsedBiomeStorage>> {
        let Some(biome_data) = self.get_biome_data_blocking(pos)? else {
            return Ok(None);
        };
        for storage in biome_data.storages {
            if biome_storage_contains_y(&storage, y) {
                return Ok(Some(storage));
            }
        }
        Ok(None)
    }

    pub fn get_biome_storages_blocking(
        &self,
        pos: ChunkPos,
    ) -> Result<Option<Vec<ParsedBiomeStorage>>> {
        Ok(self
            .get_biome_data_blocking(pos)?
            .map(|biome_data| biome_data.storages))
    }

    fn get_biome_data_blocking(&self, pos: ChunkPos) -> Result<Option<ParsedBiomeData>> {
        for (tag, version) in [
            (ChunkRecordTag::Data3D, crate::ChunkVersion::New),
            (ChunkRecordTag::Data2D, crate::ChunkVersion::Old),
            (ChunkRecordTag::Data2DLegacy, crate::ChunkVersion::Old),
        ] {
            let key = ChunkKey::new(pos, tag).encode();
            let Some(value) = self.storage().get(&key)? else {
                continue;
            };
            let biome_data = match version {
                crate::ChunkVersion::New => parse_data3d(&value),
                crate::ChunkVersion::Old => parse_legacy_data2d(&value),
            }
            .map_err(|error| BedrockWorldError::CorruptWorld(format!("biome data: {error}")))?;
            return Ok(Some(biome_data));
        }
        Ok(None)
    }

    fn has_render_chunk_records_blocking(
        &self,
        pos: ChunkPos,
        options: &WorldScanOptions,
    ) -> Result<bool> {
        let prefix = chunk_record_prefix(pos);
        let mut found = false;
        self.storage().for_each_prefix_key(
            &prefix,
            to_storage_read_options(options),
            &mut |key| {
                check_cancelled(options)?;
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                    && chunk_key.pos == pos
                    && chunk_key.tag.is_render_chunk_record()
                {
                    found = true;
                    return Ok(StorageVisitorControl::Stop);
                }
                Ok(StorageVisitorControl::Continue)
            },
        )?;
        Ok(found)
    }

    pub fn get_height_at_blocking(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
    ) -> Result<Option<i16>> {
        validate_local_column(local_x, local_z)?;
        Ok(self
            .get_height_map_blocking(pos)?
            .and_then(|heights| heights[usize::from(local_z)][usize::from(local_x)]))
    }

    pub fn get_height_map_blocking(
        &self,
        pos: ChunkPos,
    ) -> Result<Option<[[Option<i16>; 16]; 16]>> {
        if let Some(biome_data) = self
            .get_biome_data_blocking(pos)
            .map_err(|error| BedrockWorldError::CorruptWorld(format!("height data: {error}")))?
        {
            return Ok(Some(render_height_map_from_biome_data(pos, &biome_data)));
        }
        let key = ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode();
        if let Some(value) = self.storage().get(&key)? {
            let terrain = LegacyTerrain::parse(value)?;
            return Ok(Some(render_height_map_from_legacy_terrain(&terrain)));
        }
        Ok(None)
    }

    pub fn get_legacy_biome_colors_blocking(
        &self,
        pos: ChunkPos,
    ) -> Result<Option<[[Option<u32>; 16]; 16]>> {
        let key = ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode();
        let Some(value) = self.storage().get(&key)? else {
            return Ok(None);
        };
        let terrain = LegacyTerrain::parse(value)?;
        Ok(Some(render_biome_colors_from_legacy_terrain(&terrain)))
    }

    pub fn get_legacy_biome_samples_blocking(
        &self,
        pos: ChunkPos,
    ) -> Result<Option<[[Option<LegacyBiomeSample>; 16]; 16]>> {
        let key = ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode();
        let Some(value) = self.storage().get(&key)? else {
            return Ok(None);
        };
        let terrain = LegacyTerrain::parse(value)?;
        Ok(Some(render_biomes_from_legacy_terrain(&terrain)))
    }

    pub fn get_legacy_biome_color_blocking(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
    ) -> Result<Option<u32>> {
        validate_local_column(local_x, local_z)?;
        Ok(self
            .get_legacy_biome_colors_blocking(pos)?
            .and_then(|colors| colors[usize::from(local_z)][usize::from(local_x)]))
    }

    pub fn get_legacy_biome_sample_blocking(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
    ) -> Result<Option<LegacyBiomeSample>> {
        validate_local_column(local_x, local_z)?;
        Ok(self
            .get_legacy_biome_samples_blocking(pos)?
            .and_then(|samples| samples[usize::from(local_z)][usize::from(local_x)]))
    }

    pub fn get_biome_id_blocking(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
        y: i32,
    ) -> Result<Option<u32>> {
        validate_local_column(local_x, local_z)?;
        let Some(storage) = self.get_biome_storage_blocking(pos, y)? else {
            return Ok(None);
        };
        Ok(biome_id_from_storage(&storage, local_x, local_z, y))
    }

    pub fn get_surface_column_blocking(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
        options: SurfaceColumnOptions,
    ) -> Result<Option<SurfaceColumn>> {
        validate_local_column(local_x, local_z)?;
        let (min_y, max_y) = pos.y_range(crate::ChunkVersion::New);
        let start_y = match self.get_height_at_blocking(pos, local_x, local_z)? {
            Some(height) => i32::from(height).clamp(min_y, max_y),
            None => return Ok(None),
        };
        for y in (min_y..=start_y).rev() {
            let Some(block) = self.block_state_in_chunk_column(pos, local_x, y, local_z)? else {
                continue;
            };
            if options.skip_air && is_air_block_name(&block.name) {
                continue;
            }
            let biome_id = self.get_biome_id_blocking(pos, local_x, local_z, y)?;
            let (water_depth, under_water_block_name) =
                if options.transparent_water && is_water_block_name(&block.name) {
                    self.find_solid_under_water(pos, local_x, local_z, y, min_y)?
                } else {
                    (0, None)
                };
            return Ok(Some(SurfaceColumn {
                y,
                block_name: block.name,
                biome_id,
                water_depth,
                under_water_block_name,
                is_fallback: false,
            }));
        }
        Ok(None)
    }

    pub fn load_render_chunk_blocking(
        &self,
        pos: ChunkPos,
        options: RenderChunkLoadOptions,
    ) -> Result<RenderChunkData> {
        let (mut chunks, _) = self.load_render_chunks_with_stats_blocking([pos], options)?;
        chunks.pop().ok_or_else(|| {
            BedrockWorldError::CorruptWorld("exact render load returned no chunk".to_string())
        })
    }

    pub fn load_render_chunks_blocking(
        &self,
        positions: impl IntoIterator<Item = ChunkPos>,
        options: RenderChunkLoadOptions,
    ) -> Result<Vec<RenderChunkData>> {
        Ok(self
            .load_render_chunks_with_stats_blocking(positions, options)?
            .0)
    }

    pub fn load_render_chunks_with_stats_blocking(
        &self,
        positions: impl IntoIterator<Item = ChunkPos>,
        options: RenderChunkLoadOptions,
    ) -> Result<(Vec<RenderChunkData>, RenderLoadStats)> {
        let started = Instant::now();
        let positions = positions.into_iter().collect::<Vec<_>>();
        if positions.is_empty() {
            log::debug!("loading render chunks skipped (chunks=0)");
            return Ok((Vec::new(), RenderLoadStats::default()));
        }
        let mut positions = positions;
        sort_render_chunk_positions(&mut positions, options.priority);
        let worker_count = options.threading.resolve_checked(positions.len())?;
        log::debug!(
            "loading render chunks (chunks={}, workers={}, request={:?}, queue_depth={}, priority={:?})",
            positions.len(),
            worker_count,
            options.request,
            options
                .pipeline
                .resolve_queue_depth(worker_count, positions.len()),
            options.priority
        );
        self.load_render_chunks_exact_batch_blocking_sorted(
            positions,
            options,
            worker_count,
            started,
        )
    }

    #[allow(clippy::too_many_lines)]
    fn load_render_chunks_exact_batch_blocking_sorted(
        &self,
        positions: Vec<ChunkPos>,
        options: RenderChunkLoadOptions,
        worker_count: usize,
        started: Instant,
    ) -> Result<(Vec<RenderChunkData>, RenderLoadStats)> {
        check_render_load_cancelled(&options)?;
        let mut raw_chunks = positions
            .iter()
            .copied()
            .map(|pos| RawRenderChunkData {
                pos,
                biome_record: None,
                subchunks: BTreeMap::new(),
                block_entities: None,
                legacy_terrain: None,
            })
            .collect::<Vec<_>>();

        let mut keys = Vec::new();
        let mut requests = Vec::new();
        for (chunk_index, pos) in positions.iter().copied().enumerate() {
            push_render_record_request(
                &mut keys,
                &mut requests,
                chunk_index,
                pos,
                RenderRecordKind::LegacyTerrain,
            );
            if request_needs_biome_record(options.request) {
                push_render_record_request(
                    &mut keys,
                    &mut requests,
                    chunk_index,
                    pos,
                    RenderRecordKind::Data3D,
                );
                push_render_record_request(
                    &mut keys,
                    &mut requests,
                    chunk_index,
                    pos,
                    RenderRecordKind::Data2D,
                );
                push_render_record_request(
                    &mut keys,
                    &mut requests,
                    chunk_index,
                    pos,
                    RenderRecordKind::Data2DLegacy,
                );
            }
            if !request_uses_hint_surface_subchunks(options.request) {
                for y in planned_render_subchunk_ys(pos, &options, None)? {
                    push_render_record_request(
                        &mut keys,
                        &mut requests,
                        chunk_index,
                        pos,
                        RenderRecordKind::Subchunk(y),
                    );
                }
            }
            if request_loads_block_entities(options.request) {
                push_render_record_request(
                    &mut keys,
                    &mut requests,
                    chunk_index,
                    pos,
                    RenderRecordKind::BlockEntity,
                );
            }
        }

        let mut keys_requested = keys.len();
        let mut exact_get_batches = 0usize;
        let mut db_read_ms = 0u128;
        let db_started = Instant::now();
        let values = self.storage().get_many(&keys)?;
        db_read_ms = db_read_ms.saturating_add(db_started.elapsed().as_millis());
        exact_get_batches = exact_get_batches.saturating_add(usize::from(!keys.is_empty()));
        let mut keys_found = apply_render_record_values(&mut raw_chunks, &requests, values);

        if request_uses_hint_surface_subchunks(options.request) {
            let mut needed_keys = Vec::new();
            let mut needed_requests = Vec::new();
            for (chunk_index, raw) in raw_chunks.iter().enumerate() {
                let biome_data = parse_render_biome_record(raw.biome_record.as_ref())?;
                let height_map = if let Some(biome_data) = biome_data.as_ref() {
                    Some(render_height_map_from_biome_data(raw.pos, biome_data))
                } else {
                    legacy_height_map_from_raw(raw.legacy_terrain.as_ref())?
                };
                for y in planned_render_subchunk_ys(raw.pos, &options, height_map.as_ref())? {
                    if raw.subchunks.contains_key(&y) {
                        continue;
                    }
                    push_render_record_request(
                        &mut needed_keys,
                        &mut needed_requests,
                        chunk_index,
                        raw.pos,
                        RenderRecordKind::Subchunk(y),
                    );
                }
            }
            if !needed_keys.is_empty() {
                let db_started = Instant::now();
                let values = self.storage().get_many(&needed_keys)?;
                db_read_ms = db_read_ms.saturating_add(db_started.elapsed().as_millis());
                exact_get_batches = exact_get_batches.saturating_add(1);
                keys_requested = keys_requested.saturating_add(needed_keys.len());
                keys_found = keys_found.saturating_add(apply_render_record_values(
                    &mut raw_chunks,
                    &needed_requests,
                    values,
                ));
            }
        }

        check_render_load_cancelled(&options)?;
        let decode_started = Instant::now();
        let (mut chunks, decode_timing) = if worker_count == 1 {
            let mut chunks = Vec::with_capacity(raw_chunks.len());
            let mut timing = RenderChunkDecodeTiming::default();
            for raw in raw_chunks {
                check_render_load_cancelled(&options)?;
                let (chunk, chunk_timing) = render_chunk_from_raw(raw, &options)?;
                timing.add(chunk_timing);
                chunks.push(chunk);
                emit_render_load_progress(&options, chunks.len());
            }
            (chunks, timing)
        } else {
            let pool = world_pool(worker_count)?;
            let decoded = pool.install(|| {
                raw_chunks
                    .into_par_iter()
                    .map(|raw| {
                        check_render_load_cancelled(&options)?;
                        render_chunk_from_raw(raw, &options)
                    })
                    .collect::<Result<Vec<_>>>()
            })?;
            let mut chunks = Vec::with_capacity(decoded.len());
            let mut timing = RenderChunkDecodeTiming::default();
            for (chunk, chunk_timing) in decoded {
                timing.add(chunk_timing);
                chunks.push(chunk);
            }
            (chunks, timing)
        };
        let full_reload_ms =
            self.reload_incomplete_needed_exact_surface_chunks_blocking(&mut chunks, &options)?;
        let decode_ms = decode_started.elapsed().as_millis();
        let mut stats = render_load_stats(&chunks, worker_count, 0, started.elapsed().as_millis());
        stats.keys_requested = keys_requested;
        stats.keys_found = keys_found;
        stats.exact_get_batches = exact_get_batches;
        stats.prefix_scans = 0;
        stats.decode_ms = decode_ms;
        stats.db_read_ms = db_read_ms;
        stats.biome_parse_ms = decode_timing.biome_parse_ms;
        stats.subchunk_parse_ms = decode_timing.subchunk_parse_ms;
        stats.surface_scan_ms = decode_timing.surface_scan_ms;
        stats.block_entity_parse_ms = decode_timing.block_entity_parse_ms;
        stats.full_reload_ms = full_reload_ms;
        stats.detected_format = self.format;
        stats.legacy_pocket_chunks = if self.format == WorldFormat::PocketChunksDat {
            stats.legacy_terrain_records
        } else {
            0
        };
        log_render_load_complete(&stats);
        Ok((chunks, stats))
    }

    fn reload_incomplete_needed_exact_surface_chunks_blocking(
        &self,
        chunks: &mut [RenderChunkData],
        options: &RenderChunkLoadOptions,
    ) -> Result<u128> {
        if !request_uses_hint_surface_subchunks(options.request) {
            return Ok(0);
        }

        let mut full_options = options.clone();
        full_options.request = exact_surface_full_request(options.request);
        let mut reload_indexes = Vec::new();
        let mut reload_positions = Vec::new();
        for (index, chunk) in chunks.iter().enumerate() {
            if needed_exact_surface_chunk_requires_full_reload(chunk)? {
                reload_indexes.push(index);
                reload_positions.push(chunk.pos);
            }
        }
        if reload_positions.is_empty() {
            return Ok(0);
        }
        check_render_load_cancelled(options)?;
        let started = Instant::now();
        let worker_count = options.threading.resolve_checked(reload_positions.len())?;
        full_options.threading = if worker_count <= 1 {
            WorldThreadingOptions::Single
        } else {
            WorldThreadingOptions::Fixed(worker_count)
        };
        let (reloaded, stats) =
            self.load_render_chunks_with_stats_blocking(reload_positions, full_options)?;
        for (chunk_index, reloaded_chunk) in reload_indexes.into_iter().zip(reloaded) {
            if let Some(chunk) = chunks.get_mut(chunk_index) {
                *chunk = reloaded_chunk;
            }
        }
        let elapsed = started.elapsed().as_millis().max(stats.load_ms);
        log::debug!(
            "hint surface full reload complete (chunks={}, workers={}, load_ms={}, db_read_ms={}, decode_ms={})",
            stats.requested_chunks,
            stats.worker_threads,
            stats.load_ms,
            stats.db_read_ms,
            stats.decode_ms
        );
        Ok(elapsed)
    }

    pub fn load_render_region_blocking(
        &self,
        region: RenderChunkRegion,
        options: RenderRegionLoadOptions,
    ) -> Result<RenderRegionData> {
        if region.min_chunk_x > region.max_chunk_x || region.min_chunk_z > region.max_chunk_z {
            return Err(BedrockWorldError::Validation(format!(
                "invalid render region: min=({}, {}) max=({}, {})",
                region.min_chunk_x, region.min_chunk_z, region.max_chunk_x, region.max_chunk_z
            )));
        }
        let chunk_count_x = i64::from(region.max_chunk_x) - i64::from(region.min_chunk_x) + 1;
        let chunk_count_z = i64::from(region.max_chunk_z) - i64::from(region.min_chunk_z) + 1;
        let capacity = usize::try_from(chunk_count_x.saturating_mul(chunk_count_z))
            .map_err(|_| BedrockWorldError::Validation("render region is too large".to_string()))?;
        let mut positions = Vec::with_capacity(capacity);
        for z in region.min_chunk_z..=region.max_chunk_z {
            for x in region.min_chunk_x..=region.max_chunk_x {
                positions.push(ChunkPos {
                    x,
                    z,
                    dimension: region.dimension,
                });
            }
        }
        let (chunks, stats) =
            self.load_render_chunks_with_stats_blocking(positions, options.into())?;
        Ok(RenderRegionData {
            region,
            chunks,
            stats,
        })
    }

    pub fn get_block_state_at_blocking(
        &self,
        dimension: crate::Dimension,
        block_pos: BlockPos,
    ) -> Result<Option<BlockState>> {
        let chunk_pos = block_pos.to_chunk_pos(dimension);
        let (_, block_y, _) = block_pos.in_chunk_offset();
        let subchunk_y = block_y_to_subchunk_y(block_y)?;
        let Some(subchunk) = self.parse_subchunk_blocking(
            chunk_pos,
            subchunk_y,
            WorldParseOptions {
                subchunk_decode_mode: SubChunkDecodeMode::FullIndices,
                ..WorldParseOptions::summary()
            },
        )?
        else {
            return Ok(None);
        };
        let (local_x, _, local_z) = block_pos.in_chunk_offset();
        let local_y = u8::try_from(block_y - i32::from(subchunk_y) * 16).map_err(|_| {
            BedrockWorldError::Validation(format!("block y={block_y} is outside subchunk bounds"))
        })?;
        Ok(subchunk.block_state_at(local_x, local_y, local_z).cloned())
    }

    pub fn get_subchunk_layer_blocking(
        &self,
        pos: ChunkPos,
        y: i32,
        mode: SubChunkDecodeMode,
    ) -> Result<Option<SubChunk>> {
        let subchunk_y = block_y_to_subchunk_y(y)?;
        self.parse_subchunk_blocking(
            pos,
            subchunk_y,
            WorldParseOptions {
                subchunk_decode_mode: mode,
                ..WorldParseOptions::summary()
            },
        )
    }

    fn block_state_in_chunk_column(
        &self,
        pos: ChunkPos,
        local_x: u8,
        y: i32,
        local_z: u8,
    ) -> Result<Option<BlockState>> {
        let subchunk_y = block_y_to_subchunk_y(y)?;
        let Some(subchunk) = self.parse_subchunk_blocking(
            pos,
            subchunk_y,
            WorldParseOptions {
                subchunk_decode_mode: SubChunkDecodeMode::FullIndices,
                ..WorldParseOptions::summary()
            },
        )?
        else {
            return Ok(None);
        };
        let local_y = u8::try_from(y - i32::from(subchunk_y) * 16).map_err(|_| {
            BedrockWorldError::Validation(format!("block y={y} is outside subchunk bounds"))
        })?;
        Ok(subchunk.block_state_at(local_x, local_y, local_z).cloned())
    }

    fn find_solid_under_water(
        &self,
        pos: ChunkPos,
        local_x: u8,
        local_z: u8,
        water_y: i32,
        min_y: i32,
    ) -> Result<(u8, Option<String>)> {
        let mut depth = 0_u8;
        for y in (min_y..water_y).rev() {
            let Some(block) = self.block_state_in_chunk_column(pos, local_x, y, local_z)? else {
                continue;
            };
            if is_air_block_name(&block.name) || is_water_block_name(&block.name) {
                depth = depth.saturating_add(1);
                continue;
            }
            depth = depth.saturating_add(1);
            return Ok((depth, Some(block.name)));
        }
        Ok((depth, None))
    }

    pub fn parse_global_data_blocking(&self) -> Result<Vec<ParsedDbEntry>> {
        parse_global_storage_entries(self.storage(), WorldParseOptions::summary())
    }

    pub fn scan_entities_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ParsedEntity>, WorldParseReport)> {
        let mut report = WorldParseReport::default();
        let mut entities = Vec::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_entry(to_storage_read_options(&options), &mut |key, value| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                match BedrockDbKey::decode(key) {
                    BedrockDbKey::ActorPrefix { .. } => {
                        entities.extend(parse_entities_from_value(value, &mut report));
                    }
                    BedrockDbKey::Chunk(chunk_key) if chunk_key.tag == ChunkRecordTag::Entity => {
                        entities.extend(parse_entities_from_value(value, &mut report));
                    }
                    _ => {}
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok((entities, report))
    }

    pub fn scan_block_entities_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ParsedBlockEntity>, WorldParseReport)> {
        let mut report = WorldParseReport::default();
        let mut block_entities = Vec::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_entry(to_storage_read_options(&options), &mut |key, value| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key) {
                    if chunk_key.tag == ChunkRecordTag::BlockEntity {
                        block_entities.extend(parse_block_entities_from_value(value, &mut report));
                    }
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok((block_entities, report))
    }

    pub fn scan_items_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ItemStack>, WorldParseReport)> {
        let mut report = WorldParseReport::default();
        let mut items = Vec::new();
        let mut entries_seen = 0usize;
        self.storage()
            .for_each_entry(to_storage_read_options(&options), &mut |key, value| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                match BedrockDbKey::decode(key) {
                    BedrockDbKey::LocalPlayer | BedrockDbKey::RemotePlayer(_) => {
                        match parse_root_nbt(value) {
                            Ok(nbt) => {
                                let mut player_items = collect_item_stacks(&nbt);
                                report.item_count =
                                    report.item_count.saturating_add(player_items.len());
                                items.append(&mut player_items);
                            }
                            Err(error) => report
                                .parse_errors
                                .push(format!("player item scan failed: {error}")),
                        }
                    }
                    BedrockDbKey::ActorPrefix { .. } => {
                        for entity in parse_entities_from_value(value, &mut report) {
                            items.extend(entity.items);
                        }
                    }
                    BedrockDbKey::Chunk(chunk_key) if chunk_key.tag == ChunkRecordTag::Entity => {
                        for entity in parse_entities_from_value(value, &mut report) {
                            items.extend(entity.items);
                        }
                    }
                    BedrockDbKey::Chunk(chunk_key)
                        if chunk_key.tag == ChunkRecordTag::BlockEntity =>
                    {
                        for block_entity in parse_block_entities_from_value(value, &mut report) {
                            items.extend(block_entity.items);
                        }
                    }
                    _ => {}
                }
                if entries_seen.is_multiple_of(8192) {
                    emit_progress(&options, entries_seen);
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok((items, report))
    }

    /// Scans map records through the full global-data parser.
    ///
    /// Prefer [`Self::scan_map_records_blocking`] when only `map_` records are
    /// needed because it uses an exact prefix scan.
    ///
    /// # Errors
    ///
    /// Returns storage or parse errors from the underlying world scan.
    pub fn scan_maps_blocking(&self) -> Result<Vec<ParsedMapData>> {
        Ok(self
            .parse_global_data_blocking()?
            .into_iter()
            .filter_map(|entry| match entry.value {
                ParsedDbValue::MapData(value) => Some(value),
                _ => None,
            })
            .collect())
    }

    /// Reads a single typed map record by exact `map_<id>` key.
    ///
    /// # Errors
    ///
    /// Returns storage errors or map NBT parse errors.
    pub fn read_map_record_blocking(&self, id: &MapRecordId) -> Result<Option<ParsedMapData>> {
        self.storage()
            .get(&id.storage_key())?
            .map(|value| parse_map_record(id.clone(), value))
            .transpose()
    }

    /// Prefix-scans typed map records without scanning unrelated globals.
    ///
    /// # Errors
    ///
    /// Returns storage errors, cancellation, or map NBT parse errors.
    pub fn scan_map_records_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ParsedMapData>> {
        let mut records = Vec::new();
        self.storage().for_each_prefix_ref(
            b"map_",
            to_storage_read_options(&options),
            &mut |entry| {
                check_cancelled(&options)?;
                let Some(id) = MapRecordId::from_storage_key(entry.key) else {
                    return Ok(StorageVisitorControl::Continue);
                };
                records.push(parse_map_record(id, Bytes::copy_from_slice(entry.value))?);
                Ok(StorageVisitorControl::Continue)
            },
        )?;
        Ok(records)
    }

    /// Writes a map record after serialize -> parse roundtrip validation.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for malformed records, or storage errors from the commit.
    pub fn write_map_record_blocking(&self, record: &ParsedMapData) -> Result<()> {
        self.ensure_writable()?;
        let value = encode_map_record(record)?;
        parse_map_record(record.record_id.clone(), value.clone())?;
        let mut transaction = self.transaction();
        transaction.put_raw_key(record.record_id.storage_key(), value);
        transaction.commit()
    }

    /// Deletes a map record by exact id.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds or storage
    /// errors from the commit.
    pub fn delete_map_record_blocking(&self, id: &MapRecordId) -> Result<()> {
        self.ensure_writable()?;
        let mut transaction = self.transaction();
        transaction.delete_raw_key(id.storage_key());
        transaction.commit()
    }

    /// Scans village records through the full global-data parser.
    ///
    /// # Errors
    ///
    /// Returns storage or parse errors from the underlying world scan.
    pub fn scan_villages_blocking(&self) -> Result<Vec<ParsedVillageData>> {
        Ok(self
            .parse_global_data_blocking()?
            .into_iter()
            .filter_map(|entry| match entry.value {
                ParsedDbValue::VillageData(value) => Some(value),
                _ => None,
            })
            .collect())
    }

    pub fn scan_villages_lightweight_blocking(
        &self,
        cancel: &CancelFlag,
    ) -> Result<Vec<ParsedVillageData>> {
        let mut villages = Vec::new();
        let options = StorageReadOptions {
            cancel: Some(cancel.to_storage_cancel()),
            ..StorageReadOptions::default()
        };
        self.storage()
            .for_each_prefix_ref(b"VILLAGE_", options, &mut |entry| {
                if cancel.is_cancelled() {
                    return Err(BedrockWorldError::Cancelled {
                        operation: "village scan",
                    });
                }
                let BedrockDbKey::Village(key) = BedrockDbKey::decode(entry.key) else {
                    return Ok(StorageVisitorControl::Continue);
                };
                let roots = parse_consecutive_root_nbt(entry.value).unwrap_or_default();
                villages.push(ParsedVillageData {
                    key,
                    roots,
                    raw: Bytes::new(),
                });
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(villages)
    }

    /// Scans global records through the full global-data parser.
    ///
    /// Prefer [`Self::scan_global_records_blocking`] when only typed global
    /// records are needed.
    ///
    /// # Errors
    ///
    /// Returns storage or parse errors from the underlying world scan.
    pub fn scan_globals_blocking(&self) -> Result<Vec<ParsedGlobalData>> {
        Ok(self
            .parse_global_data_blocking()?
            .into_iter()
            .filter_map(|entry| match entry.value {
                ParsedDbValue::GlobalData(value) => Some(value),
                _ => None,
            })
            .collect())
    }

    /// Reads a single typed global record by exact key.
    ///
    /// # Errors
    ///
    /// Returns storage errors or global NBT parse errors.
    pub fn read_global_record_blocking(
        &self,
        kind: GlobalRecordKind,
    ) -> Result<Option<ParsedGlobalData>> {
        let key = kind.storage_key();
        self.storage()
            .get(&key)?
            .map(|value| parse_global_record(kind.clone(), kind.name(), value))
            .transpose()
    }

    /// Scans known global records while preserving each typed key kind.
    ///
    /// # Errors
    ///
    /// Returns storage errors, cancellation, or global NBT parse errors.
    pub fn scan_global_records_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ParsedGlobalData>> {
        let mut records = Vec::new();
        self.storage()
            .for_each_entry(to_storage_read_options(&options), &mut |key, value| {
                check_cancelled(&options)?;
                let BedrockDbKey::Global(kind) = BedrockDbKey::decode(key) else {
                    return Ok(StorageVisitorControl::Continue);
                };
                records.push(parse_global_record(
                    kind.clone(),
                    kind.name(),
                    value.clone(),
                )?);
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(records)
    }

    /// Writes a global record after serialize -> parse roundtrip validation.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for malformed records, or storage errors from the commit.
    pub fn write_global_record_blocking(&self, record: &ParsedGlobalData) -> Result<()> {
        self.ensure_writable()?;
        let value = encode_global_record(record)?;
        parse_global_record(record.kind.clone(), record.name.clone(), value.clone())?;
        let mut transaction = self.transaction();
        transaction.put_raw_key(record.kind.storage_key(), value);
        transaction.commit()
    }

    /// Deletes a typed global record.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds or storage
    /// errors from the commit.
    pub fn delete_global_record_blocking(&self, kind: GlobalRecordKind) -> Result<()> {
        self.ensure_writable()?;
        let mut transaction = self.transaction();
        transaction.delete_raw_key(kind.storage_key());
        transaction.commit()
    }

    /// Reads the Data2D/Data3D height map for a chunk.
    ///
    /// # Errors
    ///
    /// Returns storage errors or biome/heightmap parse errors.
    pub fn get_heightmap_blocking(&self, pos: ChunkPos) -> Result<Option<HeightMap2d>> {
        self.get_biome_data_blocking(pos)?
            .map(|data| HeightMap2d::new(data.height_map))
            .transpose()
    }

    /// Writes a chunk height map while preserving existing `Data3D` biome storages.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for invalid height map length, or storage errors.
    pub fn put_heightmap_blocking(
        &self,
        pos: ChunkPos,
        version: ChunkVersion,
        height_map: HeightMap2d,
    ) -> Result<()> {
        self.ensure_writable()?;
        let existing = self.get_biome_data_blocking(pos)?;
        let storages = existing.map_or_else(Vec::new, |data| data.storages);
        let value = match version {
            ChunkVersion::Old => Biome2d::new(height_map.values, vec![0; 256])?.encode()?,
            ChunkVersion::New => Biome3d::new(height_map.values, storages)?.encode()?,
        };
        let tag = match version {
            ChunkVersion::Old => ChunkRecordTag::Data2D,
            ChunkVersion::New => ChunkRecordTag::Data3D,
        };
        self.put_raw_record_blocking(&ChunkKey::new(pos, tag), &value)
    }

    /// Writes a full `Data3D` biome payload after roundtrip validation.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for malformed biome storage, or storage errors.
    pub fn put_biome_storage_blocking(&self, pos: ChunkPos, biome: Biome3d) -> Result<()> {
        self.ensure_writable()?;
        let value = biome.encode()?;
        Biome3d::parse(&value)?;
        self.put_raw_record_blocking(&ChunkKey::new(pos, ChunkRecordTag::Data3D), &value)
    }

    /// Scans hardcoded spawn area records across the world.
    ///
    /// # Errors
    ///
    /// Returns storage errors, cancellation, or HSA payload validation errors.
    pub fn scan_hsa_records_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<(ChunkPos, Vec<ParsedHardcodedSpawnArea>)>> {
        let mut records = Vec::new();
        self.storage()
            .for_each_entry(to_storage_read_options(&options), &mut |key, value| {
                check_cancelled(&options)?;
                let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key) else {
                    return Ok(StorageVisitorControl::Continue);
                };
                if chunk_key.tag == ChunkRecordTag::HardcodedSpawners {
                    records.push((chunk_key.pos, parse_hardcoded_spawn_area_records(value)?));
                }
                Ok(StorageVisitorControl::Continue)
            })?;
        Ok(records)
    }

    /// Writes hardcoded spawn areas for one chunk.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for invalid bounds/lengths, or storage errors.
    pub fn put_hsa_for_chunk_blocking(
        &self,
        pos: ChunkPos,
        areas: &[ParsedHardcodedSpawnArea],
    ) -> Result<()> {
        self.ensure_writable()?;
        let value = encode_hardcoded_spawn_area_records(areas)?;
        parse_hardcoded_spawn_area_records(&value)?;
        let mut transaction = self.transaction();
        transaction.put_raw_record(
            &ChunkKey::new(pos, ChunkRecordTag::HardcodedSpawners),
            value,
        );
        transaction.commit()
    }

    /// Deletes hardcoded spawn areas for one chunk.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds or storage
    /// errors.
    pub fn delete_hsa_for_chunk_blocking(&self, pos: ChunkPos) -> Result<()> {
        self.delete_raw_record_blocking(&ChunkKey::new(pos, ChunkRecordTag::HardcodedSpawners))
    }

    /// Reads all block entities from a chunk's consecutive NBT payload.
    ///
    /// # Errors
    ///
    /// Returns storage errors or block-entity NBT parse errors.
    pub fn block_entities_in_chunk_blocking(
        &self,
        pos: ChunkPos,
    ) -> Result<Vec<BlockEntityRecord>> {
        let key = ChunkKey::new(pos, ChunkRecordTag::BlockEntity).encode();
        let Some(value) = self.storage().get(&key)? else {
            return Ok(Vec::new());
        };
        let mut report = WorldParseReport::default();
        Ok(parse_block_entities_from_value(&value, &mut report)
            .into_iter()
            .enumerate()
            .map(|(index, entity)| BlockEntityRecord {
                chunk: pos,
                index,
                entity,
            })
            .collect())
    }

    /// Replaces a chunk's block entity payload after coordinate validation.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors when entity coordinates do not belong to `pos`, or storage errors.
    pub fn put_block_entities_blocking(
        &self,
        pos: ChunkPos,
        entities: &[ParsedBlockEntity],
    ) -> Result<()> {
        self.ensure_writable()?;
        validate_block_entities_in_chunk(pos, entities)?;
        let roots = entities
            .iter()
            .map(|entity| entity.nbt.clone())
            .collect::<Vec<_>>();
        let value = encode_consecutive_roots(&roots)?;
        let mut report = WorldParseReport::default();
        let parsed = parse_block_entities_from_value(&value, &mut report);
        validate_block_entities_in_chunk(pos, &parsed)?;
        let mut transaction = self.transaction();
        transaction.put_raw_record(&ChunkKey::new(pos, ChunkRecordTag::BlockEntity), value);
        transaction.commit()
    }

    /// Edits one block entity in place and rewrites the chunk payload.
    ///
    /// # Errors
    ///
    /// Returns validation errors when no block entity exists at `block`, when
    /// the edited NBT no longer parses as a block entity, or storage/read-only
    /// errors from the write.
    pub fn edit_block_entity_at_blocking<F>(
        &self,
        pos: ChunkPos,
        block: BlockPos,
        edit: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut NbtTag) -> Result<()>,
    {
        self.ensure_writable()?;
        let mut entities = self
            .block_entities_in_chunk_blocking(pos)?
            .into_iter()
            .map(|record| record.entity)
            .collect::<Vec<_>>();
        let Some(index) = entities
            .iter()
            .position(|entity| entity.position == Some([block.x, block.y, block.z]))
        else {
            return Err(BedrockWorldError::Validation(format!(
                "no block entity exists at {},{},{}",
                block.x, block.y, block.z
            )));
        };
        edit(&mut entities[index].nbt)?;
        let mut report = WorldParseReport::default();
        entities[index] = parse_block_entities_from_value(
            &Bytes::from(serialize_root_nbt(&entities[index].nbt)?),
            &mut report,
        )
        .into_iter()
        .next()
        .ok_or_else(|| BedrockWorldError::Validation("edited block entity vanished".to_string()))?;
        self.put_block_entities_blocking(pos, &entities)
    }

    /// Deletes one block entity by absolute block position.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds or storage
    /// errors from rewriting/deleting the payload.
    pub fn delete_block_entity_at_blocking(&self, pos: ChunkPos, block: BlockPos) -> Result<()> {
        self.ensure_writable()?;
        let entities = self
            .block_entities_in_chunk_blocking(pos)?
            .into_iter()
            .map(|record| record.entity)
            .filter(|entity| entity.position != Some([block.x, block.y, block.z]))
            .collect::<Vec<_>>();
        if entities.is_empty() {
            return self
                .delete_raw_record_blocking(&ChunkKey::new(pos, ChunkRecordTag::BlockEntity));
        }
        self.put_block_entities_blocking(pos, &entities)
    }

    /// Reads actors from both legacy inline `Entity` and modern digest/prefix storage.
    ///
    /// # Errors
    ///
    /// Returns storage errors or digest validation errors.
    pub fn actors_in_chunk_blocking(&self, pos: ChunkPos) -> Result<Vec<ActorRecord>> {
        let mut records = Vec::new();
        let inline_key = ChunkKey::new(pos, ChunkRecordTag::Entity);
        if let Some(value) = self.storage().get(&inline_key.encode())? {
            let mut report = WorldParseReport::default();
            records.extend(
                parse_entities_from_value(&value, &mut report)
                    .into_iter()
                    .map(|entity| ActorRecord {
                        uid: entity.unique_id.map(ActorUid),
                        source: ActorSource::InlineChunk(inline_key.clone()),
                        entity,
                        raw: value.clone(),
                    }),
            );
        }
        let digest_key = ActorDigestKey::new(pos).storage_key();
        let Some(digest) = self.storage().get(&digest_key)? else {
            return Ok(records);
        };
        let ids = parse_actor_digest_ids(&digest)?;
        let actor_keys = ids.iter().map(|id| id.storage_key()).collect::<Vec<_>>();
        let values = self.storage().get_many(&actor_keys)?;
        for (id, value) in ids.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let mut report = WorldParseReport::default();
            records.extend(
                parse_entities_from_value(&value, &mut report)
                    .into_iter()
                    .map(|entity| ActorRecord {
                        uid: Some(id),
                        source: ActorSource::ActorPrefix(id),
                        entity,
                        raw: value.clone(),
                    }),
            );
        }
        Ok(records)
    }

    /// Writes a modern actor record and updates the chunk actor digest.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors when `actor` has no `UniqueID`, or storage errors from the commit.
    pub fn put_actor_blocking(&self, pos: ChunkPos, actor: &ParsedEntity) -> Result<()> {
        self.ensure_writable()?;
        let uid = actor.unique_id.map(ActorUid).ok_or_else(|| {
            BedrockWorldError::Validation("actor UniqueID is required".to_string())
        })?;
        let value = Bytes::from(serialize_root_nbt(&actor.nbt)?);
        parse_entities_from_value(&value, &mut WorldParseReport::default());
        let mut transaction = self.transaction();
        transaction.put_actor(pos, uid, value)?;
        transaction.commit()
    }

    /// Deletes a modern actor record and removes it from the chunk digest.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds or storage
    /// errors from the commit.
    pub fn delete_actor_blocking(&self, pos: ChunkPos, uid: ActorUid) -> Result<()> {
        self.ensure_writable()?;
        let mut transaction = self.transaction();
        transaction.delete_actor(pos, uid)?;
        transaction.commit()
    }

    /// Moves a modern actor between chunk digests and rewrites its actorprefix payload.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors when `actor` has no `UniqueID`, or storage errors from the commit.
    pub fn move_actor_blocking(
        &self,
        from: ChunkPos,
        to: ChunkPos,
        actor: &ParsedEntity,
    ) -> Result<()> {
        self.ensure_writable()?;
        let uid = actor.unique_id.map(ActorUid).ok_or_else(|| {
            BedrockWorldError::Validation("actor UniqueID is required".to_string())
        })?;
        let value = Bytes::from(serialize_root_nbt(&actor.nbt)?);
        let mut transaction = self.transaction();
        transaction.delete_actor(from, uid)?;
        transaction.put_actor(to, uid, value)?;
        transaction.commit()
    }

    #[cfg(feature = "async")]
    pub async fn list_players(&self) -> Result<Vec<PlayerId>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.list_players_blocking())
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn classify_keys(
        &self,
        options: WorldScanOptions,
    ) -> Result<BTreeMap<String, usize>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.classify_keys_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn list_chunk_positions(&self, options: WorldScanOptions) -> Result<Vec<ChunkPos>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.list_chunk_positions_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn list_render_chunk_positions(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ChunkPos>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.list_render_chunk_positions_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn list_render_chunk_positions_in_region(
        &self,
        region: RenderChunkRegion,
        options: WorldScanOptions,
    ) -> Result<Vec<ChunkPos>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || {
            world.list_render_chunk_positions_in_region_blocking(region, options)
        })
        .await
        .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn discover_chunk_bounds(
        &self,
        dimension: crate::Dimension,
        options: WorldScanOptions,
    ) -> Result<Option<ChunkBounds>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || {
            world.discover_chunk_bounds_blocking(dimension, options)
        })
        .await
        .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn nearest_loaded_chunk_to_spawn(
        &self,
        dimension: crate::Dimension,
        spawn_block_x: i32,
        spawn_block_z: i32,
        options: WorldScanOptions,
    ) -> Result<Option<ChunkPos>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || {
            world.nearest_loaded_chunk_to_spawn_blocking(
                dimension,
                spawn_block_x,
                spawn_block_z,
                options,
            )
        })
        .await
        .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn parse_chunk(
        &self,
        pos: ChunkPos,
        options: WorldParseOptions,
    ) -> Result<ParsedChunkData> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.parse_chunk_with_options_blocking(pos, options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn load_render_chunk(
        &self,
        pos: ChunkPos,
        options: RenderChunkLoadOptions,
    ) -> Result<RenderChunkData> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.load_render_chunk_blocking(pos, options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn load_render_chunks(
        &self,
        positions: Vec<ChunkPos>,
        options: RenderChunkLoadOptions,
    ) -> Result<Vec<RenderChunkData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.load_render_chunks_blocking(positions, options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn load_render_region(
        &self,
        region: RenderChunkRegion,
        options: RenderRegionLoadOptions,
    ) -> Result<RenderRegionData> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.load_render_region_blocking(region, options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_entities(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ParsedEntity>, WorldParseReport)> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_entities_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_block_entities(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ParsedBlockEntity>, WorldParseReport)> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_block_entities_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_items(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ItemStack>, WorldParseReport)> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_items_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_maps(&self) -> Result<Vec<ParsedMapData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_maps_blocking())
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_villages(&self) -> Result<Vec<ParsedVillageData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_villages_blocking())
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    pub async fn scan_globals(&self) -> Result<Vec<ParsedGlobalData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_globals_blocking())
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::read_map_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, or map parse errors.
    #[cfg(feature = "async")]
    pub async fn read_map_record(&self, id: MapRecordId) -> Result<Option<ParsedMapData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.read_map_record_blocking(&id))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::scan_map_records_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, cancellation, or map parse errors.
    #[cfg(feature = "async")]
    pub async fn scan_map_records(&self, options: WorldScanOptions) -> Result<Vec<ParsedMapData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_map_records_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::write_map_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn write_map_record(&self, record: ParsedMapData) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.write_map_record_blocking(&record))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::delete_map_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, or storage errors.
    #[cfg(feature = "async")]
    pub async fn delete_map_record(&self, id: MapRecordId) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.delete_map_record_blocking(&id))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::read_global_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, or global parse errors.
    #[cfg(feature = "async")]
    pub async fn read_global_record(
        &self,
        kind: GlobalRecordKind,
    ) -> Result<Option<ParsedGlobalData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.read_global_record_blocking(kind))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::scan_global_records_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, cancellation, or global parse errors.
    #[cfg(feature = "async")]
    pub async fn scan_global_records(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<ParsedGlobalData>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_global_records_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::write_global_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn write_global_record(&self, record: ParsedGlobalData) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.write_global_record_blocking(&record))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::delete_global_record_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, or storage errors.
    #[cfg(feature = "async")]
    pub async fn delete_global_record(&self, kind: GlobalRecordKind) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.delete_global_record_blocking(kind))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::get_heightmap_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, or heightmap parse errors.
    #[cfg(feature = "async")]
    pub async fn get_heightmap(&self, pos: ChunkPos) -> Result<Option<HeightMap2d>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.get_heightmap_blocking(pos))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::put_heightmap_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn put_heightmap(
        &self,
        pos: ChunkPos,
        version: ChunkVersion,
        height_map: HeightMap2d,
    ) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.put_heightmap_blocking(pos, version, height_map))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::put_biome_storage_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn put_biome_storage(&self, pos: ChunkPos, biome: Biome3d) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.put_biome_storage_blocking(pos, biome))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::scan_hsa_records_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, cancellation, or HSA parse errors.
    #[cfg(feature = "async")]
    pub async fn scan_hsa_records(
        &self,
        options: WorldScanOptions,
    ) -> Result<Vec<(ChunkPos, Vec<ParsedHardcodedSpawnArea>)>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.scan_hsa_records_blocking(options))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::put_hsa_for_chunk_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn put_hsa_for_chunk(
        &self,
        pos: ChunkPos,
        areas: Vec<ParsedHardcodedSpawnArea>,
    ) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.put_hsa_for_chunk_blocking(pos, &areas))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::delete_hsa_for_chunk_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, or storage errors.
    #[cfg(feature = "async")]
    pub async fn delete_hsa_for_chunk(&self, pos: ChunkPos) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.delete_hsa_for_chunk_blocking(pos))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::block_entities_in_chunk_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, or block-entity parse errors.
    #[cfg(feature = "async")]
    pub async fn block_entities_in_chunk(&self, pos: ChunkPos) -> Result<Vec<BlockEntityRecord>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.block_entities_in_chunk_blocking(pos))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::put_block_entities_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn put_block_entities(
        &self,
        pos: ChunkPos,
        entities: Vec<ParsedBlockEntity>,
    ) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.put_block_entities_blocking(pos, &entities))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::edit_block_entity_at_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn edit_block_entity_at<F>(
        &self,
        pos: ChunkPos,
        block: BlockPos,
        edit: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut NbtTag) -> Result<()> + Send + 'static,
    {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.edit_block_entity_at_blocking(pos, block, edit))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::delete_block_entity_at_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, or storage errors.
    #[cfg(feature = "async")]
    pub async fn delete_block_entity_at(&self, pos: ChunkPos, block: BlockPos) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.delete_block_entity_at_blocking(pos, block))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::actors_in_chunk_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, storage, or actor digest validation errors.
    #[cfg(feature = "async")]
    pub async fn actors_in_chunk(&self, pos: ChunkPos) -> Result<Vec<ActorRecord>> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.actors_in_chunk_blocking(pos))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::put_actor_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn put_actor(&self, pos: ChunkPos, actor: ParsedEntity) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.put_actor_blocking(pos, &actor))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::delete_actor_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, or storage errors.
    #[cfg(feature = "async")]
    pub async fn delete_actor(&self, pos: ChunkPos, uid: ActorUid) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.delete_actor_blocking(pos, uid))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    /// Async wrapper for [`Self::move_actor_blocking`].
    ///
    /// # Errors
    ///
    /// Returns join, read-only, validation, or storage errors.
    #[cfg(feature = "async")]
    pub async fn move_actor(
        &self,
        from: ChunkPos,
        to: ChunkPos,
        actor: ParsedEntity,
    ) -> Result<()> {
        let world = self.blocking_clone();
        tokio::task::spawn_blocking(move || world.move_actor_blocking(from, to, &actor))
            .await
            .map_err(|error| BedrockWorldError::Join(error.to_string()))?
    }

    #[cfg(feature = "async")]
    #[must_use]
    fn blocking_clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            options: self.options.clone(),
            storage: self.storage.clone(),
            format: self.format,
        }
    }

    pub fn put_raw_record_blocking(&self, key: &ChunkKey, value: &[u8]) -> Result<()> {
        self.ensure_writable()?;
        self.storage().put(&key.encode(), value)
    }

    pub fn delete_raw_record_blocking(&self, key: &ChunkKey) -> Result<()> {
        self.ensure_writable()?;
        self.storage().delete(&key.encode())
    }

    #[must_use]
    pub fn transaction(&self) -> WorldTransaction<'_, S> {
        WorldTransaction {
            storage: &self.storage,
            batch: StorageBatch::new(),
            read_only: self.options.read_only,
        }
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.options.read_only {
            return Err(BedrockWorldError::ReadOnly);
        }
        Ok(())
    }
}

/// Batched raw record and player writes for a [`BedrockWorld`].
pub struct WorldTransaction<'a, S = Arc<dyn WorldStorage>>
where
    S: WorldStorageHandle,
{
    storage: &'a S,
    batch: StorageBatch,
    read_only: bool,
}

impl<S> WorldTransaction<'_, S>
where
    S: WorldStorageHandle,
{
    /// Stages a raw chunk record write.
    pub fn put_raw_record(&mut self, key: &ChunkKey, value: impl Into<Bytes>) {
        self.batch.put(key.encode(), value.into());
    }

    /// Stages a raw chunk record delete.
    pub fn delete_raw_record(&mut self, key: &ChunkKey) {
        self.batch.delete(key.encode());
    }

    /// Stages a raw key/value write.
    pub fn put_raw_key(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.batch.put(key.into(), value.into());
    }

    /// Stages a raw key delete.
    pub fn delete_raw_key(&mut self, key: impl Into<Bytes>) {
        self.batch.delete(key.into());
    }

    /// Stages a player record write using the player's storage key.
    ///
    /// # Errors
    ///
    /// Returns validation errors when the player id does not map to a `LevelDB`
    /// key.
    pub fn put_player(&mut self, player: &PlayerData) -> Result<()> {
        let Some(key) = player.id.storage_key() else {
            return Err(BedrockWorldError::Validation(
                "player id has no LevelDB key".to_string(),
            ));
        };
        self.batch
            .put(Bytes::copy_from_slice(key.as_ref()), player.raw.clone());
        Ok(())
    }

    /// Stages a typed map record write after roundtrip validation.
    ///
    /// # Errors
    ///
    /// Returns validation or serialization errors for malformed map data.
    pub fn put_map_record(&mut self, record: &ParsedMapData) -> Result<()> {
        let value = encode_map_record(record)?;
        parse_map_record(record.record_id.clone(), value.clone())?;
        self.batch.put(record.record_id.storage_key(), value);
        Ok(())
    }

    /// Stages a typed map record delete.
    pub fn delete_map_record(&mut self, id: &MapRecordId) {
        self.batch.delete(id.storage_key());
    }

    /// Stages a typed global record write after roundtrip validation.
    ///
    /// # Errors
    ///
    /// Returns validation or serialization errors for malformed global data.
    pub fn put_global_record(&mut self, record: &ParsedGlobalData) -> Result<()> {
        let value = encode_global_record(record)?;
        parse_global_record(record.kind.clone(), record.name.clone(), value.clone())?;
        self.batch.put(record.kind.storage_key(), value);
        Ok(())
    }

    /// Stages a typed global record delete.
    pub fn delete_global_record(&mut self, kind: &GlobalRecordKind) {
        self.batch.delete(kind.storage_key());
    }

    /// Stages a modern actor write and updates the chunk `digp` digest.
    ///
    /// # Errors
    ///
    /// Returns validation errors for malformed actor NBT or digest data.
    pub fn put_actor(&mut self, pos: ChunkPos, uid: ActorUid, value: Bytes) -> Result<()> {
        parse_entities_from_value(&value, &mut WorldParseReport::default());
        self.batch.put(uid.storage_key(), value);
        self.replace_actor_digest(pos, |ids| {
            if !ids.contains(&uid) {
                ids.push(uid);
            }
        })?;
        Ok(())
    }

    /// Stages a modern actor delete and removes it from the chunk `digp` digest.
    ///
    /// # Errors
    ///
    /// Returns validation errors for malformed existing digest data.
    pub fn delete_actor(&mut self, pos: ChunkPos, uid: ActorUid) -> Result<()> {
        self.batch.delete(uid.storage_key());
        self.replace_actor_digest(pos, |ids| ids.retain(|id| *id != uid))
    }

    /// Validates and commits all staged writes atomically through the storage backend.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockWorldError::ReadOnly`] for read-only worlds, validation
    /// errors for unsafe key/value combinations, or storage errors.
    pub fn commit(self) -> Result<()> {
        if self.read_only {
            return Err(BedrockWorldError::ReadOnly);
        }
        validate_batch(&self.batch)?;
        self.storage.storage().write_batch(&self.batch)?;
        self.storage.storage().flush()
    }

    fn replace_actor_digest<F>(&mut self, pos: ChunkPos, update: F) -> Result<()>
    where
        F: FnOnce(&mut Vec<ActorUid>),
    {
        let key = ActorDigestKey::new(pos).storage_key();
        let mut ids = self
            .storage
            .storage()
            .get(&key)?
            .map_or_else(|| Ok(Vec::new()), |value| parse_actor_digest_ids(&value))?;
        update(&mut ids);
        if ids.is_empty() {
            self.batch.delete(key);
        } else {
            self.batch.put(key, encode_actor_digest_ids(&ids));
        }
        Ok(())
    }
}

fn validate_batch(batch: &StorageBatch) -> Result<()> {
    for op in batch.ops() {
        match op {
            StorageOp::Put { key, value } => {
                if key.is_empty() {
                    return Err(BedrockWorldError::Validation(
                        "batch contains empty key".to_string(),
                    ));
                }
                if value.is_empty() {
                    return Err(BedrockWorldError::Validation(format!(
                        "batch put for key {key:?} contains empty value"
                    )));
                }
            }
            StorageOp::Delete { key } => {
                if key.is_empty() {
                    return Err(BedrockWorldError::Validation(
                        "batch contains empty delete key".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_block_entities_in_chunk(pos: ChunkPos, entities: &[ParsedBlockEntity]) -> Result<()> {
    for entity in entities {
        let Some([x, y, z]) = entity.position else {
            return Err(BedrockWorldError::Validation(
                "block entity is missing x/y/z position".to_string(),
            ));
        };
        let block_pos = BlockPos { x, y, z };
        if block_pos.to_chunk_pos(pos.dimension) != pos {
            return Err(BedrockWorldError::Validation(format!(
                "block entity at {x},{y},{z} is outside chunk {pos:?}"
            )));
        }
    }
    Ok(())
}

fn check_cancelled(options: &WorldScanOptions) -> Result<()> {
    if options
        .cancel
        .as_ref()
        .is_some_and(CancelFlag::is_cancelled)
    {
        return Err(BedrockWorldError::Cancelled {
            operation: "world scan",
        });
    }
    Ok(())
}

fn emit_progress(options: &WorldScanOptions, entries_seen: usize) {
    if let Some(progress) = &options.progress {
        progress.emit(WorldScanProgress { entries_seen });
    }
}

fn check_render_load_cancelled(options: &RenderChunkLoadOptions) -> Result<()> {
    if options
        .cancel
        .as_ref()
        .is_some_and(CancelFlag::is_cancelled)
    {
        return Err(BedrockWorldError::Cancelled {
            operation: "render chunk load",
        });
    }
    Ok(())
}

fn emit_render_load_progress(options: &RenderChunkLoadOptions, completed_chunks: usize) {
    if completed_chunks.is_multiple_of(options.pipeline.resolve_progress_interval())
        && let Some(progress) = &options.progress
    {
        progress.emit(WorldScanProgress {
            entries_seen: completed_chunks,
        });
    }
}

fn sort_render_chunk_positions(positions: &mut [ChunkPos], priority: RenderChunkPriority) {
    match priority {
        RenderChunkPriority::RowMajor => positions.sort(),
        RenderChunkPriority::DistanceFrom { chunk_x, chunk_z } => positions.sort_by_key(|pos| {
            let dx = i64::from(pos.x) - i64::from(chunk_x);
            let dz = i64::from(pos.z) - i64::from(chunk_z);
            (
                dx.saturating_mul(dx).saturating_add(dz.saturating_mul(dz)),
                pos.z,
                pos.x,
                pos.dimension,
            )
        }),
    }
}

fn push_render_record_request(
    keys: &mut Vec<Bytes>,
    requests: &mut Vec<RenderRecordRequest>,
    chunk_index: usize,
    pos: ChunkPos,
    kind: RenderRecordKind,
) {
    let key = match kind {
        RenderRecordKind::LegacyTerrain => {
            ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode()
        }
        RenderRecordKind::Data3D => ChunkKey::new(pos, ChunkRecordTag::Data3D).encode(),
        RenderRecordKind::Data2D => ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
        RenderRecordKind::Data2DLegacy => ChunkKey::new(pos, ChunkRecordTag::Data2DLegacy).encode(),
        RenderRecordKind::Subchunk(y) => ChunkKey::subchunk(pos, y).encode(),
        RenderRecordKind::BlockEntity => ChunkKey::new(pos, ChunkRecordTag::BlockEntity).encode(),
    };
    keys.push(key);
    requests.push(RenderRecordRequest { chunk_index, kind });
}

fn apply_render_record_values(
    chunks: &mut [RawRenderChunkData],
    requests: &[RenderRecordRequest],
    values: Vec<Option<Bytes>>,
) -> usize {
    let mut found = 0usize;
    for (request, value) in requests.iter().copied().zip(values) {
        let Some(value) = value else {
            continue;
        };
        found = found.saturating_add(1);
        let Some(chunk) = chunks.get_mut(request.chunk_index) else {
            continue;
        };
        match request.kind {
            RenderRecordKind::LegacyTerrain => {
                chunk.legacy_terrain = Some(value);
            }
            RenderRecordKind::Data3D => {
                if chunk.biome_record.is_none() {
                    chunk.biome_record = Some((crate::ChunkVersion::New, value));
                }
            }
            RenderRecordKind::Data2D | RenderRecordKind::Data2DLegacy => {
                if chunk.biome_record.is_none() {
                    chunk.biome_record = Some((crate::ChunkVersion::Old, value));
                }
            }
            RenderRecordKind::Subchunk(y) => {
                chunk.subchunks.insert(y, value);
            }
            RenderRecordKind::BlockEntity => {
                chunk.block_entities = Some(value);
            }
        }
    }
    found
}

fn planned_render_subchunk_ys(
    pos: ChunkPos,
    options: &RenderChunkLoadOptions,
    height_map: Option<&[[Option<i16>; 16]; 16]>,
) -> Result<BTreeSet<i8>> {
    let mut subchunk_ys = BTreeSet::new();
    match options.request {
        RenderChunkRequest::ExactSurface { subchunks, .. } => {
            let (min_y, max_y) = pos.subchunk_index_range(crate::ChunkVersion::New);
            match subchunks {
                ExactSurfaceSubchunkPolicy::Full => {
                    for y in min_y..=max_y {
                        subchunk_ys.insert(y);
                    }
                }
                ExactSurfaceSubchunkPolicy::HintThenVerify => {
                    if let Some(height_map) = height_map {
                        insert_needed_surface_subchunks(
                            &mut subchunk_ys,
                            Some(height_map),
                            min_y,
                            max_y,
                        );
                    } else {
                        for y in min_y..=max_y {
                            subchunk_ys.insert(y);
                        }
                    }
                }
            }
        }
        RenderChunkRequest::Layer { y } => {
            subchunk_ys.insert(block_y_to_subchunk_y(y)?);
        }
        RenderChunkRequest::RawHeightMap | RenderChunkRequest::Biome { .. } => {}
    }
    Ok(subchunk_ys)
}

const fn request_needs_biome_record(request: RenderChunkRequest) -> bool {
    match request {
        RenderChunkRequest::ExactSurface { biome, .. } => {
            !matches!(biome, ExactSurfaceBiomeLoad::None)
        }
        RenderChunkRequest::RawHeightMap | RenderChunkRequest::Biome { .. } => true,
        RenderChunkRequest::Layer { .. } => false,
    }
}

const fn request_loads_block_entities(request: RenderChunkRequest) -> bool {
    matches!(
        request,
        RenderChunkRequest::ExactSurface {
            block_entities: true,
            ..
        }
    )
}

const fn request_builds_column_samples(request: RenderChunkRequest) -> bool {
    matches!(request, RenderChunkRequest::ExactSurface { .. })
}

const fn request_uses_hint_surface_subchunks(request: RenderChunkRequest) -> bool {
    matches!(
        request,
        RenderChunkRequest::ExactSurface {
            subchunks: ExactSurfaceSubchunkPolicy::HintThenVerify,
            ..
        }
    )
}

const fn exact_surface_full_request(request: RenderChunkRequest) -> RenderChunkRequest {
    match request {
        RenderChunkRequest::ExactSurface {
            biome,
            block_entities,
            ..
        } => RenderChunkRequest::ExactSurface {
            subchunks: ExactSurfaceSubchunkPolicy::Full,
            biome,
            block_entities,
        },
        other => other,
    }
}

fn insert_render_biome_storages(
    render_biomes: &mut BTreeMap<i32, ParsedBiomeStorage>,
    biome_data: Option<ParsedBiomeData>,
    request: RenderChunkRequest,
) {
    let Some(biome_data) = biome_data else {
        return;
    };
    match request {
        RenderChunkRequest::ExactSurface {
            biome: ExactSurfaceBiomeLoad::TopColumns | ExactSurfaceBiomeLoad::All,
            ..
        }
        | RenderChunkRequest::Biome { load_all: true, .. } => {
            for storage in biome_data.storages {
                let key = storage.y.unwrap_or(i32::MIN);
                render_biomes.insert(key, storage);
            }
        }
        RenderChunkRequest::Biome { y, load_all: false } => {
            let mut fallback = None;
            for storage in biome_data.storages {
                if biome_storage_contains_y(&storage, y) {
                    render_biomes.insert(biome_storage_bucket_y(y), storage);
                    return;
                }
                fallback.get_or_insert(storage);
            }
            if let Some(storage) = fallback {
                render_biomes.insert(biome_storage_bucket_y(y), storage);
            }
        }
        RenderChunkRequest::ExactSurface {
            biome: ExactSurfaceBiomeLoad::None,
            ..
        }
        | RenderChunkRequest::RawHeightMap
        | RenderChunkRequest::Layer { .. } => {}
    }
}

fn parse_render_biome_record(
    record: Option<&(crate::ChunkVersion, Bytes)>,
) -> Result<Option<ParsedBiomeData>> {
    let Some((version, value)) = record else {
        return Ok(None);
    };
    let data = match version {
        crate::ChunkVersion::New => parse_data3d(value),
        crate::ChunkVersion::Old => parse_legacy_data2d(value),
    }
    .map_err(|error| BedrockWorldError::CorruptWorld(format!("biome data: {error}")))?;
    Ok(Some(data))
}

fn render_height_map_from_biome_data(
    pos: ChunkPos,
    biome_data: &ParsedBiomeData,
) -> [[Option<i16>; 16]; 16] {
    let mut heights = [[None; 16]; 16];
    for local_z in 0..16_u8 {
        for local_x in 0..16_u8 {
            let index = height_map_index(local_x, local_z);
            heights[usize::from(local_z)][usize::from(local_x)] = biome_data
                .height_map
                .get(index)
                .and_then(|height| normalize_biome_height(pos, biome_data.version, *height));
        }
    }
    heights
}

fn normalize_biome_height(
    pos: ChunkPos,
    version: crate::ChunkVersion,
    stored_height: i16,
) -> Option<i16> {
    let (min_y, _) = pos.y_range(version);
    i16::try_from(i32::from(stored_height) + min_y).ok()
}

fn legacy_height_map_from_raw(
    raw_legacy_terrain: Option<&Bytes>,
) -> Result<Option<[[Option<i16>; 16]; 16]>> {
    let Some(raw_legacy_terrain) = raw_legacy_terrain else {
        return Ok(None);
    };
    let terrain = LegacyTerrain::parse(raw_legacy_terrain.clone())?;
    Ok(Some(render_height_map_from_legacy_terrain(&terrain)))
}

fn render_height_map_from_legacy_terrain(terrain: &LegacyTerrain) -> [[Option<i16>; 16]; 16] {
    let mut heights = [[None; 16]; 16];
    for local_z in 0..16_u8 {
        for local_x in 0..16_u8 {
            heights[usize::from(local_z)][usize::from(local_x)] =
                terrain.height_at(local_x, local_z).map(i16::from);
        }
    }
    heights
}

fn render_biomes_from_legacy_terrain(
    terrain: &LegacyTerrain,
) -> [[Option<LegacyBiomeSample>; 16]; 16] {
    let mut samples = [[None; 16]; 16];
    for local_z in 0..16_u8 {
        for local_x in 0..16_u8 {
            samples[usize::from(local_z)][usize::from(local_x)] =
                terrain.biome_sample_at(local_x, local_z);
        }
    }
    samples
}

fn render_biome_colors_from_legacy_terrain(terrain: &LegacyTerrain) -> [[Option<u32>; 16]; 16] {
    let mut colors = [[None; 16]; 16];
    let samples = render_biomes_from_legacy_terrain(terrain);
    for local_z in 0..16 {
        for local_x in 0..16 {
            colors[local_z][local_x] = samples[local_z][local_x].map(LegacyBiomeSample::rgb_u32);
        }
    }
    colors
}

fn build_terrain_column_samples(
    pos: ChunkPos,
    version: crate::ChunkVersion,
    subchunks: &BTreeMap<i8, SubChunk>,
    legacy_terrain: Option<&LegacyTerrain>,
    height_map: Option<&[[Option<i16>; 16]; 16]>,
    legacy_biomes: Option<&[[Option<LegacyBiomeSample>; 16]; 16]>,
    render_biomes: &BTreeMap<i32, ParsedBiomeStorage>,
) -> Result<TerrainColumnSamples> {
    let mut columns = TerrainColumnSamples::new();
    let (min_y, max_y) = if legacy_terrain.is_some() && subchunks.is_empty() {
        (0, 127)
    } else {
        pos.y_range(version)
    };

    for local_z in 0..16_u8 {
        for local_x in 0..16_u8 {
            if let Some(sample) = sample_column_top_down(
                local_x,
                local_z,
                min_y,
                max_y,
                subchunks,
                legacy_terrain,
                height_map,
                legacy_biomes,
                render_biomes,
            )? {
                columns.set(local_x, local_z, sample);
            }
        }
    }
    Ok(columns)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn sample_column_top_down(
    local_x: u8,
    local_z: u8,
    min_y: i32,
    max_y: i32,
    subchunks: &BTreeMap<i8, SubChunk>,
    legacy_terrain: Option<&LegacyTerrain>,
    height_map: Option<&[[Option<i16>; 16]; 16]>,
    legacy_biomes: Option<&[[Option<LegacyBiomeSample>; 16]; 16]>,
    render_biomes: &BTreeMap<i32, ParsedBiomeStorage>,
) -> Result<Option<TerrainColumnSample>> {
    let mut overlay: Option<TerrainColumnOverlay> = None;
    let mut top_water: Option<(i16, BlockState, TerrainSampleSource)> = None;
    let mut water_depth = 0_u8;
    for y in (min_y..=max_y).rev() {
        let height = i16::try_from(y).unwrap_or(if y < 0 { i16::MIN } else { i16::MAX });

        let subchunk_y = block_y_to_subchunk_y(y)?;
        let local_y = u8::try_from(y - i32::from(subchunk_y) * 16).map_err(|_| {
            BedrockWorldError::Validation(format!("block y={y} has invalid local subchunk offset"))
        })?;
        let mut saw_subchunk_layer = false;
        if let Some(subchunk) = subchunks.get(&subchunk_y) {
            for state in subchunk.visible_block_states_at(local_x, local_y, local_z) {
                saw_subchunk_layer = true;
                if let Some(sample) = scan_terrain_surface_state(
                    local_x,
                    local_z,
                    y,
                    height,
                    state.clone(),
                    TerrainSampleSource::Subchunk,
                    &mut overlay,
                    &mut top_water,
                    &mut water_depth,
                    legacy_biomes,
                    render_biomes,
                ) {
                    return Ok(Some(sample));
                }
            }
            if saw_subchunk_layer {
                continue;
            }
            if let Some(id) = subchunk.legacy_block_id_at(local_x, local_y, local_z) {
                let data = subchunk
                    .legacy_block_data_at(local_x, local_y, local_z)
                    .unwrap_or(0);
                if let Some(sample) = scan_terrain_surface_state(
                    local_x,
                    local_z,
                    y,
                    height,
                    legacy_world_block_state(id, data),
                    TerrainSampleSource::Subchunk,
                    &mut overlay,
                    &mut top_water,
                    &mut water_depth,
                    legacy_biomes,
                    render_biomes,
                ) {
                    return Ok(Some(sample));
                }
                continue;
            }
        }

        if let Some((state, source)) =
            legacy_terrain_block_state_at(local_x, y, local_z, subchunks, legacy_terrain)
            && let Some(sample) = scan_terrain_surface_state(
                local_x,
                local_z,
                y,
                height,
                state,
                source,
                &mut overlay,
                &mut top_water,
                &mut water_depth,
                legacy_biomes,
                render_biomes,
            )
        {
            return Ok(Some(sample));
        }
    }

    if let Some((water_height, water_state, water_source)) = top_water {
        let biome = terrain_biome_at(
            local_x,
            local_z,
            i32::from(water_height),
            legacy_biomes,
            render_biomes,
        );
        let relief_y = raw_height_at(height_map, local_x, local_z).unwrap_or(water_height);
        return Ok(Some(TerrainColumnSample {
            surface_y: water_height,
            surface_block_state: water_state.clone(),
            relief_y,
            relief_block_state: water_state.clone(),
            overlay,
            water: Some(TerrainColumnWater {
                surface_y: water_height,
                block_state: water_state,
                depth: water_depth,
                underwater_y: None,
                underwater_block_state: None,
                source: water_source,
            }),
            biome,
            source: water_source,
        }));
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn scan_terrain_surface_state(
    local_x: u8,
    local_z: u8,
    y: i32,
    height: i16,
    state: BlockState,
    source: TerrainSampleSource,
    overlay: &mut Option<TerrainColumnOverlay>,
    top_water: &mut Option<(i16, BlockState, TerrainSampleSource)>,
    water_depth: &mut u8,
    legacy_biomes: Option<&[[Option<LegacyBiomeSample>; 16]; 16]>,
    render_biomes: &BTreeMap<i32, ParsedBiomeStorage>,
) -> Option<TerrainColumnSample> {
    match terrain_surface_role(&state.name) {
        TerrainSurfaceRole::Air => {
            if top_water.is_some() {
                *water_depth = (*water_depth).saturating_add(1);
            }
            None
        }
        TerrainSurfaceRole::Overlay => {
            if let Some((water_height, water_state, water_source)) = top_water.take() {
                let biome = terrain_biome_at(local_x, local_z, y, legacy_biomes, render_biomes);
                return Some(TerrainColumnSample {
                    surface_y: water_height,
                    surface_block_state: water_state.clone(),
                    relief_y: height,
                    relief_block_state: state.clone(),
                    overlay: overlay.take(),
                    water: Some(TerrainColumnWater {
                        surface_y: water_height,
                        block_state: water_state,
                        depth: (*water_depth).saturating_add(1),
                        underwater_y: Some(height),
                        underwater_block_state: Some(state),
                        source: water_source,
                    }),
                    biome,
                    source: water_source,
                });
            }
            if overlay.is_none() {
                *overlay = Some(TerrainColumnOverlay {
                    y: height,
                    block_state: state,
                    source,
                });
            }
            None
        }
        TerrainSurfaceRole::Water => {
            if top_water.is_none() {
                *top_water = Some((height, state, source));
            } else {
                *water_depth = (*water_depth).saturating_add(1);
            }
            None
        }
        TerrainSurfaceRole::Primary => {
            let biome = terrain_biome_at(local_x, local_z, y, legacy_biomes, render_biomes);
            if let Some((water_height, water_state, water_source)) = top_water.take() {
                return Some(TerrainColumnSample {
                    surface_y: water_height,
                    surface_block_state: water_state.clone(),
                    relief_y: height,
                    relief_block_state: state.clone(),
                    overlay: overlay.take(),
                    water: Some(TerrainColumnWater {
                        surface_y: water_height,
                        block_state: water_state,
                        depth: (*water_depth).saturating_add(1),
                        underwater_y: Some(height),
                        underwater_block_state: Some(state),
                        source: water_source,
                    }),
                    biome,
                    source: water_source,
                });
            }
            Some(TerrainColumnSample {
                surface_y: height,
                surface_block_state: state.clone(),
                relief_y: height,
                relief_block_state: state,
                overlay: overlay.take(),
                water: None,
                biome,
                source,
            })
        }
    }
}

fn legacy_terrain_block_state_at(
    local_x: u8,
    y: i32,
    local_z: u8,
    subchunks: &BTreeMap<i8, SubChunk>,
    legacy_terrain: Option<&LegacyTerrain>,
) -> Option<(BlockState, TerrainSampleSource)> {
    let terrain = legacy_terrain?;
    if !(0..=127).contains(&y) {
        return None;
    }
    let legacy_y = u8::try_from(y).ok()?;
    let id = terrain.block_id_at(local_x, legacy_y, local_z)?;
    let data = terrain
        .block_data_at(local_x, legacy_y, local_z)
        .unwrap_or(0);
    let source = if subchunks.is_empty() {
        TerrainSampleSource::LegacyTerrain
    } else {
        TerrainSampleSource::LegacyFallback
    };
    Some((legacy_world_block_state(id, data), source))
}

fn terrain_biome_at(
    local_x: u8,
    local_z: u8,
    y: i32,
    legacy_biomes: Option<&[[Option<LegacyBiomeSample>; 16]; 16]>,
    render_biomes: &BTreeMap<i32, ParsedBiomeStorage>,
) -> Option<TerrainColumnBiome> {
    legacy_biomes
        .and_then(|samples| samples[usize::from(local_z)][usize::from(local_x)])
        .map(TerrainColumnBiome::Legacy)
        .or_else(|| {
            render_biome_id_at(local_x, local_z, y, render_biomes).map(TerrainColumnBiome::Id)
        })
}

fn render_biome_id_at(
    local_x: u8,
    local_z: u8,
    y: i32,
    render_biomes: &BTreeMap<i32, ParsedBiomeStorage>,
) -> Option<u32> {
    let direct = render_biomes
        .get(&biome_storage_bucket_y(y))
        .or_else(|| render_biomes.values().next())
        .and_then(|storage| {
            biome_id_from_storage(storage, local_x, local_z, y).filter(|id| *id != 0)
        });
    if direct.is_some() {
        return direct;
    }
    for storage in render_biomes.values().rev() {
        if storage.y.is_none() {
            if let Some(id) = storage
                .biome_id_at(local_x, 0, local_z)
                .filter(|id| *id != 0)
            {
                return Some(id);
            }
            continue;
        }
        for local_y in (0..16_u8).rev() {
            if let Some(id) = storage
                .biome_id_at(local_x, local_y, local_z)
                .filter(|id| *id != 0)
            {
                return Some(id);
            }
        }
    }
    None
}

fn render_chunk_from_raw(
    raw: RawRenderChunkData,
    options: &RenderChunkLoadOptions,
) -> Result<(RenderChunkData, RenderChunkDecodeTiming)> {
    let mut timing = RenderChunkDecodeTiming::default();
    let biome_started = Instant::now();
    let legacy_terrain = raw.legacy_terrain.map(LegacyTerrain::parse).transpose()?;
    let version = raw.biome_record.as_ref().map_or_else(
        || {
            if legacy_terrain.is_some() {
                crate::ChunkVersion::Old
            } else {
                crate::ChunkVersion::New
            }
        },
        |(version, _)| *version,
    );
    let biome_data = parse_render_biome_record(raw.biome_record.as_ref())?;
    let height_map = biome_data
        .as_ref()
        .map(|biome_data| render_height_map_from_biome_data(raw.pos, biome_data))
        .or_else(|| {
            legacy_terrain
                .as_ref()
                .map(render_height_map_from_legacy_terrain)
        });
    let legacy_biomes = legacy_terrain
        .as_ref()
        .map(render_biomes_from_legacy_terrain);
    let legacy_biome_colors = legacy_terrain
        .as_ref()
        .map(render_biome_colors_from_legacy_terrain);
    let mut render_biomes = BTreeMap::new();
    insert_render_biome_storages(&mut render_biomes, biome_data, options.request);
    timing.biome_parse_ms = biome_started.elapsed().as_millis();

    let mut subchunks = BTreeMap::new();
    let subchunk_started = Instant::now();
    for (y, value) in raw.subchunks {
        check_render_load_cancelled(options)?;
        subchunks.insert(
            y,
            parse_subchunk_with_mode(y, value, options.subchunk_decode)?,
        );
    }
    timing.subchunk_parse_ms = subchunk_started.elapsed().as_millis();

    let block_entity_started = Instant::now();
    let block_entities = if request_loads_block_entities(options.request) {
        if let Some(value) = raw.block_entities {
            let mut report = WorldParseReport::default();
            parse_block_entities_from_value(&value, &mut report)
                .into_iter()
                .map(|entity| render_block_entity_from_nbt(entity.nbt))
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    timing.block_entity_parse_ms = block_entity_started.elapsed().as_millis();

    let surface_scan_started = Instant::now();
    let column_samples = if request_builds_column_samples(options.request) {
        Some(build_terrain_column_samples(
            raw.pos,
            version,
            &subchunks,
            legacy_terrain.as_ref(),
            height_map.as_ref(),
            legacy_biomes.as_ref(),
            &render_biomes,
        )?)
    } else {
        None
    };
    timing.surface_scan_ms = surface_scan_started.elapsed().as_millis();

    Ok((
        RenderChunkData {
            pos: raw.pos,
            is_loaded: height_map.is_some()
                || legacy_biome_colors.is_some()
                || legacy_biomes.is_some()
                || !render_biomes.is_empty()
                || !subchunks.is_empty()
                || !block_entities.is_empty()
                || legacy_terrain.is_some(),
            height_map,
            legacy_biomes,
            legacy_biome_colors,
            biome_data: render_biomes,
            subchunks,
            block_entities,
            legacy_terrain,
            column_samples,
            version,
        },
        timing,
    ))
}

fn render_load_stats(
    chunks: &[RenderChunkData],
    worker_threads: usize,
    queue_wait_ms: u128,
    load_ms: u128,
) -> RenderLoadStats {
    RenderLoadStats {
        requested_chunks: chunks.len(),
        loaded_chunks: chunks.iter().filter(|chunk| chunk.is_loaded).count(),
        subchunks_decoded: chunks
            .iter()
            .map(|chunk| chunk.subchunks.len())
            .sum::<usize>(),
        worker_threads,
        queue_wait_ms,
        load_ms,
        keys_requested: 0,
        keys_found: 0,
        exact_get_batches: 0,
        prefix_scans: 0,
        decode_ms: 0,
        db_read_ms: 0,
        biome_parse_ms: 0,
        subchunk_parse_ms: 0,
        surface_scan_ms: 0,
        block_entity_parse_ms: 0,
        full_reload_ms: 0,
        legacy_terrain_records: chunks
            .iter()
            .filter(|chunk| chunk.legacy_terrain.is_some())
            .count(),
        legacy_biome_samples: chunks
            .iter()
            .filter(|chunk| chunk.legacy_biomes.is_some())
            .count(),
        legacy_biome_colors: chunks
            .iter()
            .filter(|chunk| chunk.legacy_biome_colors.is_some())
            .count(),
        terrain_source_legacy: chunks
            .iter()
            .filter(|chunk| chunk.legacy_terrain.is_some() && chunk.subchunks.is_empty())
            .count(),
        terrain_source_subchunk: chunks
            .iter()
            .filter(|chunk| !chunk.subchunks.is_empty())
            .count(),
        legacy_pocket_chunks: 0,
        detected_format: WorldFormat::LevelDb,
        computed_surface_columns: chunks
            .iter()
            .filter_map(|chunk| chunk.column_samples.as_ref())
            .map(TerrainColumnSamples::sampled_columns)
            .sum(),
        raw_height_mismatch_columns: chunks.iter().map(raw_height_mismatch_columns).sum(),
        missing_subchunk_columns: chunks.iter().map(missing_surface_columns).sum(),
        legacy_fallback_columns: chunks
            .iter()
            .filter_map(|chunk| chunk.column_samples.as_ref())
            .flat_map(TerrainColumnSamples::iter)
            .filter(|sample| sample.source == TerrainSampleSource::LegacyFallback)
            .count(),
        legacy_biome_preferred_columns: chunks
            .iter()
            .filter_map(|chunk| chunk.column_samples.as_ref())
            .flat_map(TerrainColumnSamples::iter)
            .filter(|sample| matches!(sample.biome, Some(TerrainColumnBiome::Legacy(_))))
            .count(),
        modern_biome_fallback_columns: chunks
            .iter()
            .filter(|chunk| chunk.legacy_biomes.is_some())
            .filter_map(|chunk| chunk.column_samples.as_ref())
            .flat_map(TerrainColumnSamples::iter)
            .filter(|sample| matches!(sample.biome, Some(TerrainColumnBiome::Id(_))))
            .count(),
    }
}

fn log_render_load_complete(stats: &RenderLoadStats) {
    log::debug!(
        "render chunk load complete (requested_chunks={}, loaded_chunks={}, missing_chunks={}, subchunks_decoded={}, legacy_terrain_records={}, legacy_biome_samples={}, legacy_biome_colors={}, terrain_source_legacy={}, terrain_source_subchunk={}, legacy_pocket_chunks={}, detected_format={:?}, computed_surface_columns={}, raw_height_mismatch_columns={}, missing_subchunk_columns={}, legacy_fallback_columns={}, legacy_biome_preferred_columns={}, modern_biome_fallback_columns={}, worker_threads={}, queue_wait_ms={}, load_ms={}, exact_get_batches={}, keys_requested={}, keys_found={}, prefix_scans={}, db_read_ms={}, decode_ms={}, biome_parse_ms={}, subchunk_parse_ms={}, surface_scan_ms={}, block_entity_parse_ms={}, full_reload_ms={})",
        stats.requested_chunks,
        stats.loaded_chunks,
        stats.requested_chunks.saturating_sub(stats.loaded_chunks),
        stats.subchunks_decoded,
        stats.legacy_terrain_records,
        stats.legacy_biome_samples,
        stats.legacy_biome_colors,
        stats.terrain_source_legacy,
        stats.terrain_source_subchunk,
        stats.legacy_pocket_chunks,
        stats.detected_format,
        stats.computed_surface_columns,
        stats.raw_height_mismatch_columns,
        stats.missing_subchunk_columns,
        stats.legacy_fallback_columns,
        stats.legacy_biome_preferred_columns,
        stats.modern_biome_fallback_columns,
        stats.worker_threads,
        stats.queue_wait_ms,
        stats.load_ms,
        stats.exact_get_batches,
        stats.keys_requested,
        stats.keys_found,
        stats.prefix_scans,
        stats.db_read_ms,
        stats.decode_ms,
        stats.biome_parse_ms,
        stats.subchunk_parse_ms,
        stats.surface_scan_ms,
        stats.block_entity_parse_ms,
        stats.full_reload_ms
    );
}

fn world_pool(worker_count: usize) -> Result<rayon::ThreadPool> {
    ThreadPoolBuilder::new()
        .num_threads(worker_count.max(1).saturating_add(1))
        .thread_name(|index| format!("bedrock-world-worker-{index}"))
        .build()
        .map_err(|error| {
            BedrockWorldError::Validation(format!("failed to build world worker pool: {error}"))
        })
}

fn to_storage_read_options(options: &WorldScanOptions) -> StorageReadOptions {
    StorageReadOptions {
        threading: match options.threading {
            WorldThreadingOptions::Auto => StorageThreadingOptions::Auto,
            WorldThreadingOptions::Fixed(threads) => StorageThreadingOptions::Fixed(threads),
            WorldThreadingOptions::Single => StorageThreadingOptions::Single,
        },
        scan_mode: match options.threading {
            WorldThreadingOptions::Single => StorageScanMode::Sequential,
            WorldThreadingOptions::Auto | WorldThreadingOptions::Fixed(_) => {
                StorageScanMode::ParallelTables
            }
        },
        pipeline: crate::storage::StoragePipelineOptions {
            queue_depth: options.pipeline.queue_depth,
            table_batch_size: options.pipeline.chunk_batch_size,
            progress_interval: options.pipeline.progress_interval,
        },
        cancel: options
            .cancel
            .as_ref()
            .map(|cancel| StorageCancelFlag::from_shared(cancel.0.clone())),
        progress: options.progress.as_ref().map(|progress| {
            let progress = progress.clone();
            StorageProgressSink::new(move |storage_progress| {
                progress.emit(WorldScanProgress {
                    entries_seen: storage_progress.entries_seen,
                });
            })
        }),
    }
}

fn chunk_record_prefix(pos: ChunkPos) -> Bytes {
    let mut bytes = Vec::with_capacity(if pos.dimension == crate::Dimension::Overworld {
        8
    } else {
        12
    });
    bytes.extend_from_slice(&pos.x.to_le_bytes());
    bytes.extend_from_slice(&pos.z.to_le_bytes());
    if pos.dimension != crate::Dimension::Overworld {
        bytes.extend_from_slice(&pos.dimension.id().to_le_bytes());
    }
    Bytes::from(bytes)
}

fn validate_render_region(region: RenderChunkRegion) -> Result<()> {
    if region.min_chunk_x > region.max_chunk_x || region.min_chunk_z > region.max_chunk_z {
        return Err(BedrockWorldError::Validation(format!(
            "invalid render region: min=({}, {}) max=({}, {})",
            region.min_chunk_x, region.min_chunk_z, region.max_chunk_x, region.max_chunk_z
        )));
    }
    Ok(())
}

fn render_block_entity_from_nbt(nbt: NbtTag) -> RenderBlockEntity {
    let root = match &nbt {
        NbtTag::Compound(root) => Some(root),
        _ => None,
    };
    RenderBlockEntity {
        id: root
            .and_then(|root| nbt_string_field(root, "id"))
            .map(ToString::to_string),
        position: root.and_then(|root| {
            Some([
                nbt_int_field(root, "x")?,
                nbt_int_field(root, "y")?,
                nbt_int_field(root, "z")?,
            ])
        }),
        nbt,
    }
}

fn nbt_string_field<'a>(
    root: &'a indexmap::IndexMap<String, NbtTag>,
    key: &str,
) -> Option<&'a str> {
    match root.get(key) {
        Some(NbtTag::String(value)) => Some(value),
        _ => None,
    }
}

fn nbt_int_field(root: &indexmap::IndexMap<String, NbtTag>, key: &str) -> Option<i32> {
    match root.get(key) {
        Some(NbtTag::Byte(value)) => Some(i32::from(*value)),
        Some(NbtTag::Short(value)) => Some(i32::from(*value)),
        Some(NbtTag::Int(value)) => Some(*value),
        Some(NbtTag::Long(value)) => i32::try_from(*value).ok(),
        _ => None,
    }
}

fn detect_world_format(path: &Path, hint: WorldFormatHint) -> Result<WorldFormat> {
    match hint {
        WorldFormatHint::Auto => {
            if path.join("db").join("CURRENT").is_file() {
                return Ok(detect_leveldb_world_format(path));
            }
            if path.join("chunks.dat").is_file() {
                return Ok(WorldFormat::PocketChunksDat);
            }
            Err(BedrockWorldError::Validation(format!(
                "could not detect Bedrock world storage at {}; expected db/CURRENT or chunks.dat",
                path.display()
            )))
        }
        WorldFormatHint::LevelDb => {
            let current = path.join("db").join("CURRENT");
            if !current.is_file() {
                return Err(BedrockWorldError::Validation(format!(
                    "LevelDB world missing {}",
                    current.display()
                )));
            }
            Ok(detect_leveldb_world_format(path))
        }
        WorldFormatHint::PocketChunksDat => {
            let chunks = path.join("chunks.dat");
            if !chunks.is_file() {
                return Err(BedrockWorldError::Validation(format!(
                    "Pocket chunks.dat world missing {}",
                    chunks.display()
                )));
            }
            Ok(WorldFormat::PocketChunksDat)
        }
    }
}

fn detect_leveldb_world_format(path: &Path) -> WorldFormat {
    let Ok(document) = read_level_dat_document(&path.join("level.dat")) else {
        return WorldFormat::LevelDb;
    };
    let NbtTag::Compound(root) = &document.root else {
        return WorldFormat::LevelDb;
    };
    let storage_version = nbt_int_field(root, "StorageVersion");
    let network_version = nbt_int_field(root, "NetworkVersion");
    if storage_version.is_some_and(|version| version <= 4)
        || network_version.is_some_and(|version| version <= 91)
    {
        WorldFormat::LevelDbLegacyTerrain
    } else {
        WorldFormat::LevelDb
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Dimension, HardcodedSpawnAreaKind, MemoryStorage, NbtTag, block_storage_index};
    use indexmap::IndexMap;
    use std::sync::Arc;

    #[cfg(feature = "backend-bedrock-leveldb")]
    fn temp_world_dir(name: &str) -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

        std::env::temp_dir().join(format!(
            "bedrock-world-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    const fn exact_surface_request(
        subchunks: ExactSurfaceSubchunkPolicy,
        biome: ExactSurfaceBiomeLoad,
        block_entities: bool,
    ) -> RenderChunkRequest {
        RenderChunkRequest::ExactSurface {
            subchunks,
            biome,
            block_entities,
        }
    }

    #[test]
    fn world_threading_validates_fixed_range_and_auto_is_not_capped_to_eight() {
        let expected_auto = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(10_000);
        assert_eq!(
            WorldThreadingOptions::Auto
                .resolve_checked(10_000)
                .expect("auto threads"),
            expected_auto
        );
        assert_eq!(
            WorldThreadingOptions::Fixed(MAX_WORLD_THREADS)
                .resolve_checked(10_000)
                .expect("max fixed threads"),
            MAX_WORLD_THREADS
        );
        assert!(WorldThreadingOptions::Fixed(0).resolve_checked(10).is_err());
        assert!(
            WorldThreadingOptions::Fixed(MAX_WORLD_THREADS + 1)
                .resolve_checked(10)
                .is_err()
        );
    }

    #[test]
    fn map_and_global_records_roundtrip_through_world_transactions() {
        let storage = Arc::new(MemoryStorage::new());
        let world = BedrockWorld::from_storage(
            "memory",
            storage.clone(),
            OpenOptions {
                read_only: false,
                ..OpenOptions::default()
            },
        );
        let map_id = MapRecordId::new("9").expect("map id");
        let map = ParsedMapData {
            id: map_id.to_string(),
            record_id: map_id.clone(),
            roots: vec![NbtTag::Compound(IndexMap::from([(
                "scale".to_string(),
                NbtTag::Byte(1),
            )]))],
            known_fields: crate::MapKnownFields::default(),
            pixels: None,
            raw: Bytes::new(),
        };

        world.write_map_record_blocking(&map).expect("write map");
        let read_map = world
            .read_map_record_blocking(&map_id)
            .expect("read map")
            .expect("map exists");
        assert_eq!(read_map.known_fields.scale, Some(1));

        let global = ParsedGlobalData {
            name: "scoreboard".to_string(),
            kind: GlobalRecordKind::Scoreboard,
            roots: vec![NbtTag::Compound(IndexMap::new())],
            raw: Bytes::new(),
        };
        world
            .write_global_record_blocking(&global)
            .expect("write global");
        assert!(
            world
                .read_global_record_blocking(GlobalRecordKind::Scoreboard)
                .expect("read global")
                .is_some()
        );

        world
            .delete_map_record_blocking(&map_id)
            .expect("delete map");
        assert!(
            world
                .read_map_record_blocking(&map_id)
                .expect("read deleted")
                .is_none()
        );
    }

    #[test]
    fn hsa_and_block_entities_roundtrip_with_chunk_validation() {
        let storage = Arc::new(MemoryStorage::new());
        let world = BedrockWorld::from_storage(
            "memory",
            storage,
            OpenOptions {
                read_only: false,
                ..OpenOptions::default()
            },
        );
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let area = ParsedHardcodedSpawnArea {
            kind: HardcodedSpawnAreaKind::NetherFortress,
            min: [0, 32, 0],
            max: [15, 80, 15],
        };
        world
            .put_hsa_for_chunk_blocking(pos, std::slice::from_ref(&area))
            .expect("write hsa");
        assert_eq!(
            world
                .scan_hsa_records_blocking(WorldScanOptions::default())
                .expect("scan hsa")[0]
                .1,
            vec![area]
        );

        let block_entity = ParsedBlockEntity {
            id: Some("Chest".to_string()),
            position: Some([1, 64, 1]),
            is_movable: Some(true),
            custom_name: None,
            items: Vec::new(),
            nbt: NbtTag::Compound(IndexMap::from([
                ("id".to_string(), NbtTag::String("Chest".to_string())),
                ("x".to_string(), NbtTag::Int(1)),
                ("y".to_string(), NbtTag::Int(64)),
                ("z".to_string(), NbtTag::Int(1)),
            ])),
        };
        world
            .put_block_entities_blocking(pos, std::slice::from_ref(&block_entity))
            .expect("write block entity");
        assert_eq!(
            world
                .block_entities_in_chunk_blocking(pos)
                .expect("read block entities")[0]
                .entity
                .position,
            Some([1, 64, 1])
        );
    }

    #[test]
    fn actor_write_updates_digest_and_prefix_together() {
        let storage = Arc::new(MemoryStorage::new());
        let world = BedrockWorld::from_storage(
            "memory",
            storage.clone(),
            OpenOptions {
                read_only: false,
                ..OpenOptions::default()
            },
        );
        let pos = ChunkPos {
            x: 2,
            z: 3,
            dimension: Dimension::Overworld,
        };
        let actor_nbt = NbtTag::Compound(IndexMap::from([
            (
                "identifier".to_string(),
                NbtTag::String("minecraft:pig".to_string()),
            ),
            ("UniqueID".to_string(), NbtTag::Long(77)),
            (
                "Pos".to_string(),
                NbtTag::List(vec![
                    NbtTag::Float(32.0),
                    NbtTag::Float(64.0),
                    NbtTag::Float(48.0),
                ]),
            ),
        ]));
        let actor = ParsedEntity {
            identifier: Some("minecraft:pig".to_string()),
            definitions: Vec::new(),
            unique_id: Some(77),
            position: Some([32.0, 64.0, 48.0]),
            rotation: None,
            motion: None,
            items: Vec::new(),
            nbt: actor_nbt,
        };

        world.put_actor_blocking(pos, &actor).expect("put actor");
        let digest = storage
            .get(&ActorDigestKey::new(pos).storage_key())
            .expect("get digest")
            .expect("digest exists");
        assert_eq!(
            parse_actor_digest_ids(&digest).expect("parse digest"),
            vec![ActorUid(77)]
        );
        assert!(
            storage
                .get(&ActorUid(77).storage_key())
                .expect("get actor")
                .is_some()
        );

        world
            .delete_actor_blocking(pos, ActorUid(77))
            .expect("delete actor");
        assert!(
            storage
                .get(&ActorDigestKey::new(pos).storage_key())
                .expect("get deleted digest")
                .is_none()
        );
        assert!(
            storage
                .get(&ActorUid(77).storage_key())
                .expect("get deleted actor")
                .is_none()
        );
    }

    #[test]
    fn render_chunk_priority_distance_orders_from_center() {
        let mut positions = vec![
            ChunkPos {
                x: 12,
                z: 0,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: 1,
                z: 0,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: -3,
                z: 0,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: 0,
                z: 0,
                dimension: Dimension::Overworld,
            },
        ];

        sort_render_chunk_positions(
            &mut positions,
            RenderChunkPriority::DistanceFrom {
                chunk_x: 0,
                chunk_z: 0,
            },
        );

        let ordered = positions
            .iter()
            .map(|pos| (pos.x, pos.z))
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![(0, 0), (1, 0), (-3, 0), (12, 0)]);
    }

    #[test]
    fn world_pipeline_options_resolve_automatic_bounds() {
        let options = WorldPipelineOptions::default();

        assert!(options.resolve_queue_depth(4, 64) >= 1);
        assert_eq!(options.resolve_progress_interval(), 256);

        let explicit = WorldPipelineOptions {
            queue_depth: 7,
            progress_interval: 9,
            ..WorldPipelineOptions::default()
        };
        assert_eq!(explicit.resolve_queue_depth(4, 64), 7);
        assert_eq!(explicit.resolve_progress_interval(), 9);
    }

    #[test]
    fn generic_memory_storage_matches_dynamic_storage_queries() {
        let storage = MemoryStorage::new();
        storage
            .put(b"~local_player", b"local")
            .expect("put local player");
        storage
            .put(b"player_remote", b"remote")
            .expect("put remote player");

        let generic_world =
            BedrockWorld::from_typed_storage("memory", storage.clone(), OpenOptions::default());
        let dynamic_world = BedrockWorld::from_storage(
            "memory",
            Arc::new(storage) as Arc<dyn WorldStorage>,
            OpenOptions::default(),
        );

        assert_eq!(
            generic_world.list_players_blocking().expect("generic"),
            dynamic_world.list_players_blocking().expect("dynamic")
        );
        assert_eq!(
            generic_world
                .classify_keys_blocking(WorldScanOptions::default())
                .expect("generic classify"),
            dynamic_world
                .classify_keys_blocking(WorldScanOptions::default())
                .expect("dynamic classify")
        );
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    #[test]
    fn generic_leveldb_storage_matches_dynamic_storage_queries() {
        let temp = temp_world_dir("generic-leveldb");
        std::fs::create_dir_all(&temp).expect("temp dir");
        let db_path = temp.join("db");
        let db = bedrock_leveldb::Db::open(&db_path, bedrock_leveldb::OpenOptions::default())
            .expect("initialize db");
        drop(db);
        let storage = BedrockLevelDbStorage::open(&db_path).expect("open storage");
        storage
            .put(b"~local_player", b"local")
            .expect("put local player");
        storage
            .put(b"player_remote", b"remote")
            .expect("put remote player");
        storage.flush().expect("flush");

        let generic_world =
            BedrockWorld::from_typed_storage(&temp, storage.clone(), OpenOptions::default());
        let dynamic_world = BedrockWorld::from_storage(
            &temp,
            Arc::new(storage) as Arc<dyn WorldStorage>,
            OpenOptions::default(),
        );

        assert_eq!(
            generic_world.list_players_blocking().expect("generic"),
            dynamic_world.list_players_blocking().expect("dynamic")
        );
        assert_eq!(
            generic_world
                .classify_keys_blocking(WorldScanOptions::default())
                .expect("generic classify"),
            dynamic_world
                .classify_keys_blocking(WorldScanOptions::default())
                .expect("dynamic classify")
        );
        std::fs::remove_dir_all(temp).expect("cleanup");
    }

    #[test]
    fn transaction_respects_read_only_option() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let key = ChunkKey::new(pos, ChunkRecordTag::Version);
        let encoded = key.encode();
        let storage = Arc::new(MemoryStorage::new());
        let read_only_world =
            BedrockWorld::from_storage("memory", storage.clone(), OpenOptions::default());
        let mut transaction = read_only_world.transaction();
        transaction.put_raw_record(&key, Bytes::from_static(b"\x01"));

        let error = transaction.commit().expect_err("read-only commit");

        assert_eq!(error.kind(), crate::BedrockWorldErrorKind::ReadOnly);
        assert_eq!(storage.get(&encoded).expect("get"), None);

        let writable_world = BedrockWorld::from_storage(
            "memory",
            storage.clone(),
            OpenOptions {
                read_only: false,
                ..OpenOptions::default()
            },
        );
        let mut transaction = writable_world.transaction();
        transaction.put_raw_record(&key, Bytes::from_static(b"\x02"));
        transaction.commit().expect("writable commit");

        assert_eq!(
            storage.get(&encoded).expect("get"),
            Some(Bytes::from_static(b"\x02"))
        );
    }

    #[test]
    fn biome_and_height_queries_read_legacy_data2d_in_zx_column_order() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_asymmetric_data2d_bytes(),
            )
            .expect("put Data2D");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        assert_eq!(
            world
                .get_biome_id_blocking(pos, 3, 2, 64)
                .expect("biome id"),
            Some(32)
        );
        assert_eq!(
            world
                .get_biome_id_blocking(pos, 2, 3, 64)
                .expect("biome id"),
            Some(23)
        );
        assert_eq!(
            world.get_height_at_blocking(pos, 3, 2).expect("height"),
            Some(132)
        );
        assert_eq!(
            world.get_height_at_blocking(pos, 2, 3).expect("height"),
            Some(123)
        );
    }

    #[test]
    fn data3d_height_map_is_normalized_to_dimension_min_y() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data3D).encode(),
                &test_data3d_height_bytes(130),
            )
            .expect("put Data3D");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        assert_eq!(
            world.get_height_at_blocking(pos, 4, 2).expect("height"),
            Some(66)
        );
        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: RenderChunkRequest::RawHeightMap,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load render chunk");

        assert_eq!(
            chunk.height_map.expect("height map")[usize::from(2_u8)][usize::from(4_u8)],
            Some(66)
        );
        assert!(chunk.column_samples.is_none());
    }

    #[test]
    fn render_chunk_exact_load_preserves_data2d_xz_height_and_biome_coordinates() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_asymmetric_data2d_bytes(),
            )
            .expect("put Data2D");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load render chunk");
        let height_map = chunk.height_map.as_ref().expect("height map");
        let biome_storage = chunk
            .biome_data
            .values()
            .next()
            .expect("render biome storage");

        assert_eq!(height_map[3][1], Some(113));
        assert_eq!(height_map[1][3], Some(131));
        assert_eq!(biome_storage.biome_id_at(1, 0, 3), Some(13));
        assert_eq!(biome_storage.biome_id_at(3, 0, 1), Some(31));
    }

    #[test]
    fn subchunk_layer_query_uses_block_y() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(&ChunkKey::subchunk(pos, -1).encode(), &[8, 0])
            .expect("put subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let subchunk = world
            .get_subchunk_layer_blocking(pos, -1, SubChunkDecodeMode::CountsOnly)
            .expect("query")
            .expect("subchunk");
        assert_eq!(subchunk.y, -1);
    }

    #[test]
    fn render_chunk_needed_surface_subchunks_avoids_full_y_range() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(65, 7),
            )
            .expect("put Data2D");
        storage
            .put(
                &ChunkKey::subchunk(pos, 4).encode(),
                &test_surface_subchunk_bytes(),
            )
            .expect("put subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let needed = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::HintThenVerify,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");
        let full = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("full render chunk");
        assert!(needed.subchunks.contains_key(&4));
        assert_eq!(needed.subchunks.get(&4), full.subchunks.get(&4));
        assert!(needed.subchunks.len() <= full.subchunks.len());
    }

    #[test]
    fn render_chunk_needed_surface_subchunks_include_lookup_above_heightmap() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(64, 7),
            )
            .expect("put Data2D");
        storage
            .put(
                &ChunkKey::subchunk(pos, 4).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:stone"),
            )
            .expect("put heightmap subchunk");
        storage
            .put(
                &ChunkKey::subchunk(pos, 5).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:oak_leaves"),
            )
            .expect("put upper subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::HintThenVerify,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");

        assert!(chunk.subchunks.contains_key(&4));
        assert!(chunk.subchunks.contains_key(&5));
        assert!(!chunk.subchunks.contains_key(&9));
        let sample = chunk
            .column_sample_at(0, 0)
            .expect("computed surface sample");
        assert_eq!(sample.surface_y, 95);
        assert_eq!(sample.surface_block_state.name, "minecraft:oak_leaves");
    }

    #[test]
    fn render_chunk_needed_exact_surface_reloads_full_when_window_top_is_touched() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(64, 7),
            )
            .expect("put Data2D");
        storage
            .put(
                &ChunkKey::subchunk(pos, 8).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:stone"),
            )
            .expect("put window-top subchunk");
        storage
            .put(
                &ChunkKey::subchunk(pos, 9).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:oak_leaves"),
            )
            .expect("put hidden upper subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::HintThenVerify,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");

        assert!(chunk.subchunks.contains_key(&8));
        assert!(chunk.subchunks.contains_key(&9));
        let sample = chunk
            .column_sample_at(0, 0)
            .expect("computed surface sample");
        assert_eq!(sample.surface_y, 159);
        assert_eq!(sample.surface_block_state.name, "minecraft:oak_leaves");
    }

    #[test]
    fn render_chunk_needed_exact_surface_reloads_full_when_raw_height_is_stale() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(0, 7),
            )
            .expect("put stale Data2D");
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:stone"),
            )
            .expect("put stale-height subchunk");
        storage
            .put(
                &ChunkKey::subchunk(pos, 4).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:air"),
            )
            .expect("put high empty hint-window subchunk");
        storage
            .put(
                &ChunkKey::subchunk(pos, 10).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:oak_leaves"),
            )
            .expect("put true roof subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::HintThenVerify,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");

        assert!(chunk.subchunks.contains_key(&10));
        let sample = chunk
            .column_sample_at(0, 0)
            .expect("computed surface sample");
        assert_eq!(sample.surface_y, 175);
        assert_eq!(sample.surface_block_state.name, "minecraft:oak_leaves");
    }

    #[test]
    fn render_chunk_raw_heightmap_request_does_not_build_surface_samples() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(0, 7),
            )
            .expect("put raw height");
        storage
            .put(
                &ChunkKey::subchunk(pos, 10).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:oak_leaves"),
            )
            .expect("put high surface subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: RenderChunkRequest::RawHeightMap,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load raw heightmap chunk");

        assert_eq!(chunk.height_map.as_ref().unwrap()[0][0], Some(0));
        assert!(chunk.column_samples.is_none());
        assert!(chunk.subchunks.is_empty());
    }

    #[test]
    fn render_chunk_needed_surface_subchunks_fall_back_to_full_without_heightmap() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::subchunk(pos, 5).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:oak_leaves"),
            )
            .expect("put upper subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::HintThenVerify,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");

        assert!(chunk.subchunks.contains_key(&5));
        let sample = chunk
            .column_sample_at(0, 0)
            .expect("computed surface sample");
        assert_eq!(sample.surface_y, 95);
        assert_eq!(sample.surface_block_state.name, "minecraft:oak_leaves");
    }

    #[test]
    fn render_chunk_loads_block_entities_when_requested() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        let block_entity = NbtTag::Compound(IndexMap::from([
            ("id".to_string(), NbtTag::String("Banner".to_string())),
            ("x".to_string(), NbtTag::Int(3)),
            ("y".to_string(), NbtTag::Int(65)),
            ("z".to_string(), NbtTag::Int(4)),
        ]));
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::BlockEntity).encode(),
                &crate::nbt::serialize_root_nbt(&block_entity).expect("serialize block entity"),
            )
            .expect("put block entity");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let without_entities = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load render chunk without block entities");
        let with_entities = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::TopColumns,
                        true,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load render chunk with block entities");

        assert!(without_entities.block_entities.is_empty());
        assert_eq!(with_entities.block_entities.len(), 1);
        assert_eq!(
            with_entities.block_entities[0].id.as_deref(),
            Some("Banner")
        );
        assert_eq!(with_entities.block_entities[0].position, Some([3, 65, 4]));
    }

    #[test]
    fn surface_column_query_returns_top_block_and_water_context() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(65, 7),
            )
            .expect("put Data2D");
        storage
            .put(
                &ChunkKey::subchunk(pos, 4).encode(),
                &test_surface_subchunk_bytes(),
            )
            .expect("put subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let column = world
            .get_surface_column_blocking(pos, 0, 0, SurfaceColumnOptions::default())
            .expect("surface query")
            .expect("surface column");

        assert_eq!(column.y, 65);
        assert_eq!(column.block_name, "minecraft:water");
        assert_eq!(column.biome_id, Some(7));
        assert_eq!(column.water_depth, 1);
        assert_eq!(
            column.under_water_block_name.as_deref(),
            Some("minecraft:sand")
        );
    }

    #[test]
    fn chunk_bounds_and_nearest_loaded_chunk_use_key_only_scan() {
        let storage = Arc::new(MemoryStorage::new());
        let positions = [
            ChunkPos {
                x: -4,
                z: 3,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: 2,
                z: -1,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: 9,
                z: 9,
                dimension: Dimension::Nether,
            },
        ];
        for pos in positions {
            storage
                .put(&ChunkKey::new(pos, ChunkRecordTag::Version).encode(), &[1])
                .expect("put chunk version");
        }
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let bounds = world
            .discover_chunk_bounds_blocking(Dimension::Overworld, WorldScanOptions::default())
            .expect("bounds")
            .expect("overworld bounds");
        assert_eq!(bounds.min_chunk_x, -4);
        assert_eq!(bounds.max_chunk_z, 3);
        assert_eq!(bounds.chunk_count, 2);

        let nearest = world
            .nearest_loaded_chunk_to_spawn_blocking(
                Dimension::Overworld,
                0,
                0,
                WorldScanOptions::default(),
            )
            .expect("nearest")
            .expect("nearest chunk");
        assert_eq!(nearest.x, 2);
        assert_eq!(nearest.z, -1);
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn render_region_index_uses_key_only_scan_and_parallel_load_keeps_order() {
        let storage = Arc::new(MemoryStorage::new());
        let render_positions = [
            ChunkPos {
                x: 0,
                z: 0,
                dimension: Dimension::Overworld,
            },
            ChunkPos {
                x: 1,
                z: 0,
                dimension: Dimension::Overworld,
            },
        ];
        for pos in render_positions {
            storage
                .put(
                    &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                    &test_data2d_bytes(64, 3),
                )
                .expect("put render chunk");
        }
        storage
            .put(
                &ChunkKey::new(
                    ChunkPos {
                        x: 2,
                        z: 0,
                        dimension: Dimension::Overworld,
                    },
                    ChunkRecordTag::Version,
                )
                .encode(),
                &[1],
            )
            .expect("put non-render chunk");
        storage
            .put(
                &ChunkKey::new(
                    ChunkPos {
                        x: 0,
                        z: 0,
                        dimension: Dimension::Nether,
                    },
                    ChunkRecordTag::Data2D,
                )
                .encode(),
                &test_data2d_bytes(64, 3),
            )
            .expect("put nether chunk");

        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());
        let visible = world
            .list_render_chunk_positions_in_region_blocking(
                RenderChunkRegion {
                    dimension: Dimension::Overworld,
                    min_chunk_x: 0,
                    min_chunk_z: 0,
                    max_chunk_x: 2,
                    max_chunk_z: 0,
                },
                WorldScanOptions {
                    threading: WorldThreadingOptions::Fixed(2),
                    ..WorldScanOptions::default()
                },
            )
            .expect("render region index");

        assert_eq!(visible, render_positions.to_vec());

        let chunks = world
            .load_render_chunks_blocking(
                visible,
                RenderChunkLoadOptions {
                    threading: WorldThreadingOptions::Fixed(2),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("parallel render chunk load");
        assert_eq!(
            chunks.iter().map(|chunk| chunk.pos).collect::<Vec<_>>(),
            render_positions.to_vec()
        );
    }

    #[test]
    fn legacy_terrain_is_renderable_and_exact_batch_loaded() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode(),
                &test_legacy_terrain_bytes(2, 65),
            )
            .expect("put legacy terrain");
        let world = BedrockWorld::from_storage_with_format(
            "memory",
            storage,
            OpenOptions::default(),
            WorldFormat::LevelDbLegacyTerrain,
        );

        let positions = world
            .list_render_chunk_positions_in_region_blocking(
                RenderChunkRegion {
                    dimension: Dimension::Overworld,
                    min_chunk_x: 0,
                    min_chunk_z: 0,
                    max_chunk_x: 0,
                    max_chunk_z: 0,
                },
                WorldScanOptions::default(),
            )
            .expect("legacy render index");
        assert_eq!(positions, vec![pos]);

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                positions,
                RenderChunkLoadOptions {
                    threading: WorldThreadingOptions::Single,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("legacy exact render load");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_loaded);
        assert!(chunks[0].legacy_terrain.is_some());
        assert_eq!(chunks[0].height_map.as_ref().unwrap()[0][0], Some(65));
        assert!(chunks[0].legacy_biomes.is_some());
        assert!(chunks[0].legacy_biome_colors.is_some());
        assert_eq!(stats.prefix_scans, 0);
        assert_eq!(stats.legacy_terrain_records, 1);
        assert_eq!(stats.legacy_biome_samples, 1);
        assert_eq!(stats.legacy_biome_colors, 1);
        assert_eq!(stats.terrain_source_legacy, 1);
        assert_eq!(stats.detected_format, WorldFormat::LevelDbLegacyTerrain);
    }

    #[test]
    fn legacy_terrain_biome_rgb_takes_priority_over_data2d_biome_id() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let mut terrain = test_legacy_terrain_bytes(2, 65);
        write_legacy_biome_sample(&mut terrain, 0, 0, 12, 0x0034_a853);
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode(),
                &terrain,
            )
            .expect("put legacy terrain");
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(2, 24),
            )
            .expect("put conflicting old data2d");
        let world = BedrockWorld::from_storage_with_format(
            "memory",
            storage,
            OpenOptions::default(),
            WorldFormat::LevelDbLegacyTerrain,
        );

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                [pos],
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::All,
                        false,
                    ),
                    threading: WorldThreadingOptions::Single,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load conflicting legacy render chunk");

        let sample = chunks[0]
            .column_sample_at(0, 0)
            .expect("computed column sample");
        assert_eq!(
            sample.biome,
            Some(TerrainColumnBiome::Legacy(LegacyBiomeSample {
                biome_id: 12,
                red: 0x34,
                green: 0xa8,
                blue: 0x53,
            }))
        );
        assert_eq!(stats.legacy_biome_preferred_columns, 256);
        assert_eq!(stats.modern_biome_fallback_columns, 0);
    }

    #[test]
    fn modern_data2d_biome_remains_available_without_legacy_terrain() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(2, 24),
            )
            .expect("put modern data2d");
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:grass_block"),
            )
            .expect("put surface subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                [pos],
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::All,
                        false,
                    ),
                    threading: WorldThreadingOptions::Single,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load modern render chunk");

        let sample = chunks[0]
            .column_sample_at(0, 0)
            .expect("computed column sample");
        assert_eq!(sample.biome, Some(TerrainColumnBiome::Id(24)));
        assert_eq!(stats.legacy_biome_preferred_columns, 0);
        assert_eq!(stats.modern_biome_fallback_columns, 0);
    }

    #[test]
    fn legacy_terrain_exposes_biome_colors_without_transposing_columns() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let mut terrain = test_legacy_terrain_bytes(2, 65);
        write_legacy_biome_sample(&mut terrain, 0, 0, 1, 0x0011_2233);
        write_legacy_biome_sample(&mut terrain, 15, 0, 2, 0x0044_5566);
        write_legacy_biome_sample(&mut terrain, 0, 15, 3, 0x0077_8899);
        write_legacy_biome_sample(&mut terrain, 15, 15, 4, 0x00aa_bbcc);
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode(),
                &terrain,
            )
            .expect("put legacy terrain");
        let world = BedrockWorld::from_storage_with_format(
            "memory",
            storage,
            OpenOptions::default(),
            WorldFormat::LevelDbLegacyTerrain,
        );

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load legacy render chunk");
        let colors = chunk.legacy_biome_colors.expect("legacy biome colors");
        let samples = chunk.legacy_biomes.expect("legacy biome samples");
        assert_eq!(colors[0][0], Some(0x0011_2233));
        assert_eq!(colors[0][15], Some(0x0044_5566));
        assert_eq!(colors[15][0], Some(0x0077_8899));
        assert_eq!(colors[15][15], Some(0x00aa_bbcc));
        assert_eq!(samples[0][0].map(|sample| sample.biome_id), Some(1));
        assert_eq!(samples[0][15].map(|sample| sample.biome_id), Some(2));
        assert_eq!(samples[15][0].map(|sample| sample.biome_id), Some(3));
        assert_eq!(samples[15][15].map(|sample| sample.biome_id), Some(4));
        assert_eq!(
            world
                .get_legacy_biome_color_blocking(pos, 15, 0)
                .expect("legacy biome color"),
            Some(0x0044_5566)
        );
        assert_eq!(
            world
                .get_legacy_biome_sample_blocking(pos, 15, 0)
                .expect("legacy biome sample")
                .map(|sample| (sample.biome_id, sample.rgb_u32())),
            Some((2, 0x0044_5566))
        );
    }

    #[test]
    fn render_load_keeps_subchunks_when_legacy_terrain_is_also_present() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::LegacyTerrain).encode(),
                &test_legacy_terrain_bytes(1, 1),
            )
            .expect("put legacy terrain");
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_surface_subchunk_bytes(),
            )
            .expect("put subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                [pos],
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load mixed render chunk");

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].legacy_terrain.is_some());
        assert!(chunks[0].subchunks.contains_key(&0));
        assert_eq!(stats.legacy_terrain_records, 1);
        assert_eq!(stats.terrain_source_subchunk, 1);
        assert_eq!(stats.terrain_source_legacy, 0);
    }

    #[test]
    fn exact_surface_column_samples_use_top_block_not_raw_heightmap() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &test_data2d_bytes(1, 3),
            )
            .expect("put misleading raw height");
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_uniform_named_subchunk_bytes("minecraft:grass_block"),
            )
            .expect("put surface subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                [pos],
                RenderChunkLoadOptions {
                    request: exact_surface_request(
                        ExactSurfaceSubchunkPolicy::Full,
                        ExactSurfaceBiomeLoad::TopColumns,
                        false,
                    ),
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load exact surface chunk");

        let sample = chunks[0]
            .column_sample_at(0, 0)
            .expect("computed column sample");
        assert_eq!(sample.surface_y, 15);
        assert_eq!(sample.surface_block_state.name, "minecraft:grass_block");
        assert_eq!(sample.source, TerrainSampleSource::Subchunk);
        assert_eq!(stats.computed_surface_columns, 256);
        assert_eq!(stats.raw_height_mismatch_columns, 256);
    }

    #[test]
    fn exact_surface_samples_keep_visual_overlay_and_primary_thin_blocks() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_named_subchunk_bytes_with_values(
                    &[
                        "minecraft:air",
                        "minecraft:grass_block",
                        "minecraft:stone_button",
                        "minecraft:red_carpet",
                        "minecraft:snow_layer",
                        "minecraft:vine",
                    ],
                    |local_x, _, local_y| match (local_x, local_y) {
                        (_, 0) => 1,
                        (0, 1) => 2,
                        (1, 1) => 3,
                        (2, 1) => 4,
                        (3, 1) => 5,
                        _ => 0,
                    },
                ),
            )
            .expect("put overlay subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load exact surface chunk");
        let button = chunk.column_sample_at(0, 0).expect("button column");
        assert_eq!(button.surface_y, 0);
        assert_eq!(button.surface_block_state.name, "minecraft:grass_block");
        assert_eq!(
            button
                .overlay
                .as_ref()
                .map(|overlay| overlay.block_state.name.as_str()),
            Some("minecraft:stone_button")
        );
        let carpet = chunk.column_sample_at(1, 0).expect("carpet column");
        assert_eq!(carpet.surface_y, 1);
        assert_eq!(carpet.surface_block_state.name, "minecraft:red_carpet");
        assert!(carpet.overlay.is_none());
        let snow = chunk.column_sample_at(2, 0).expect("snow column");
        assert_eq!(snow.surface_y, 1);
        assert_eq!(snow.surface_block_state.name, "minecraft:snow_layer");
        assert!(snow.overlay.is_none());
        let vine = chunk.column_sample_at(3, 0).expect("vine column");
        assert_eq!(vine.surface_y, 0);
        assert_eq!(
            vine.overlay
                .as_ref()
                .map(|overlay| overlay.block_state.name.as_str()),
            Some("minecraft:vine")
        );
    }

    #[test]
    fn exact_surface_samples_high_roof_from_secondary_storage() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(&ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(), &{
                let mut bytes = Vec::with_capacity(768);
                for _ in 0..256 {
                    bytes.extend_from_slice(&0_i16.to_le_bytes());
                }
                bytes.extend(std::iter::repeat_n(1_u8, 256));
                bytes
            })
            .expect("put low raw height map");
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_named_subchunk_bytes_with_values(
                    &["minecraft:air", "minecraft:stone"],
                    |_, _, local_y| u16::from(local_y == 0),
                ),
            )
            .expect("put low ground subchunk");
        storage
            .put(
                &ChunkKey::subchunk(pos, 10).encode(),
                &test_named_layered_subchunk_bytes(
                    &["minecraft:air"],
                    &["minecraft:air", "minecraft:copper_block"],
                    |_, _, _| 0,
                    |_, _, local_y| u16::from(local_y == 15),
                ),
            )
            .expect("put high secondary-storage roof");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load exact surface chunk");
        let sample = chunk.column_sample_at(0, 0).expect("roof column");

        assert_eq!(sample.surface_y, 175);
        assert_eq!(sample.surface_block_state.name, "minecraft:copper_block");
        assert_eq!(sample.source, TerrainSampleSource::Subchunk);
        assert_eq!(
            chunk.height_map.as_ref().expect("raw height map")[0][0],
            Some(0)
        );
    }

    #[test]
    fn exact_surface_samples_process_secondary_storage_water_and_overlay() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_named_layered_subchunk_bytes(
                    &["minecraft:air", "minecraft:sand", "minecraft:grass_block"],
                    &["minecraft:air", "minecraft:water", "minecraft:stone_button"],
                    |local_x, _, local_y| match (local_x, local_y) {
                        (0, 0) => 1,
                        (1, 1) => 2,
                        _ => 0,
                    },
                    |local_x, _, local_y| match (local_x, local_y) {
                        (0, 0) => 1,
                        (1, 1) => 2,
                        _ => 0,
                    },
                ),
            )
            .expect("put layered water and overlay");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load exact surface chunk");
        let water = chunk.column_sample_at(0, 0).expect("water column");
        assert_eq!(water.surface_y, 0);
        assert_eq!(water.surface_block_state.name, "minecraft:water");
        assert_eq!(water.relief_y, 0);
        assert_eq!(water.relief_block_state.name, "minecraft:sand");
        assert_eq!(
            water.water.as_ref().and_then(|water| water.underwater_y),
            Some(0)
        );
        let overlay = chunk.column_sample_at(1, 0).expect("overlay column");
        assert_eq!(overlay.surface_y, 1);
        assert_eq!(overlay.surface_block_state.name, "minecraft:grass_block");
        assert_eq!(
            overlay
                .overlay
                .as_ref()
                .map(|overlay| overlay.block_state.name.as_str()),
            Some("minecraft:stone_button")
        );
    }

    #[test]
    fn exact_surface_samples_keep_transparent_water_relief_context() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_named_subchunk_bytes_with_values(
                    &["minecraft:air", "minecraft:sand", "minecraft:water"],
                    |_, _, local_y| match local_y {
                        0 => 1,
                        1 | 2 => 2,
                        _ => 0,
                    },
                ),
            )
            .expect("put water subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(pos, RenderChunkLoadOptions::default())
            .expect("load exact surface chunk");
        let sample = chunk.column_sample_at(0, 0).expect("water column");
        let water = sample.water.as_ref().expect("water context");
        assert_eq!(sample.surface_y, 2);
        assert_eq!(sample.surface_block_state.name, "minecraft:water");
        assert_eq!(sample.relief_y, 0);
        assert_eq!(sample.relief_block_state.name, "minecraft:sand");
        assert_eq!(water.depth, 2);
        assert_eq!(water.underwater_y, Some(0));
        assert_eq!(
            water
                .underwater_block_state
                .as_ref()
                .map(|state| state.name.as_str()),
            Some("minecraft:sand")
        );
    }

    #[test]
    fn render_chunk_exact_load_preserves_legacy_subchunk_xzy_coordinates() {
        let storage = Arc::new(MemoryStorage::new());
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        storage
            .put(
                &ChunkKey::subchunk(pos, 0).encode(),
                &test_asymmetric_legacy_subchunk_bytes(),
            )
            .expect("put legacy subchunk");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let chunk = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    request: RenderChunkRequest::Layer { y: 10 },
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load legacy subchunk render chunk");
        let subchunk = chunk.subchunks.get(&0).expect("loaded legacy subchunk");

        assert_eq!(subchunk.legacy_block_id_at(0, 10, 0), Some(1));
        assert_eq!(subchunk.legacy_block_id_at(15, 10, 0), Some(12));
        assert_eq!(subchunk.legacy_block_id_at(0, 10, 15), Some(24));
        assert_eq!(subchunk.legacy_block_id_at(15, 10, 15), Some(45));
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn render_chunk_exact_batch_keeps_shuffled_positions_bound_to_records() {
        let storage = Arc::new(MemoryStorage::new());
        let fixtures = [
            (
                ChunkPos {
                    x: -3,
                    z: 1,
                    dimension: Dimension::Overworld,
                },
                "minecraft:signature_a",
            ),
            (
                ChunkPos {
                    x: 2,
                    z: -4,
                    dimension: Dimension::Overworld,
                },
                "minecraft:signature_b",
            ),
            (
                ChunkPos {
                    x: 0,
                    z: 0,
                    dimension: Dimension::Overworld,
                },
                "minecraft:signature_c",
            ),
        ];
        for (pos, block_name) in fixtures.iter().copied() {
            storage
                .put(
                    &ChunkKey::subchunk(pos, 4).encode(),
                    &test_uniform_named_subchunk_bytes(block_name),
                )
                .expect("put named subchunk");
        }
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        let (chunks, stats) = world
            .load_render_chunks_with_stats_blocking(
                vec![fixtures[1].0, fixtures[0].0, fixtures[2].0, fixtures[1].0],
                RenderChunkLoadOptions {
                    request: RenderChunkRequest::Layer { y: 64 },
                    threading: WorldThreadingOptions::Fixed(4),
                    priority: RenderChunkPriority::DistanceFrom {
                        chunk_x: 0,
                        chunk_z: 0,
                    },
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("load shuffled render chunks");

        assert_eq!(chunks.len(), 4);
        assert_eq!(stats.prefix_scans, 0);
        assert!(stats.exact_get_batches > 0);
        for chunk in chunks {
            let expected = fixtures
                .iter()
                .find_map(|(pos, block_name)| (*pos == chunk.pos).then_some(*block_name))
                .expect("known chunk position");
            let subchunk = chunk.subchunks.get(&4).expect("loaded subchunk");
            let state = subchunk
                .block_state_at(0, 0, 0)
                .expect("decoded signature block");
            assert_eq!(state.name, expected, "chunk {:?}", chunk.pos);
        }
    }

    fn test_surface_subchunk_bytes() -> Vec<u8> {
        let palette = ["minecraft:air", "minecraft:sand", "minecraft:water"];
        let mut bytes = vec![8, 1, 2 << 1];
        let values_per_word = 16_usize;
        let mut words = vec![0_u32; 256];
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                for (local_y, value) in [(0_u8, 1_u32), (1, 2)] {
                    let block_index = block_storage_index(local_x, local_y, local_z);
                    let word_index = block_index / values_per_word;
                    let bit_offset = (block_index % values_per_word) * 2;
                    words[word_index] |= value << bit_offset;
                }
            }
        }
        for word in words {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&(palette.len() as i32).to_le_bytes());
        for name in palette {
            let tag = NbtTag::Compound(IndexMap::from([
                ("name".to_string(), NbtTag::String(name.to_string())),
                ("states".to_string(), NbtTag::Compound(IndexMap::new())),
                ("version".to_string(), NbtTag::Int(1)),
            ]));
            bytes.extend_from_slice(&crate::nbt::serialize_root_nbt(&tag).expect("nbt"));
        }
        bytes
    }

    fn test_uniform_named_subchunk_bytes(block_name: &str) -> Vec<u8> {
        let palette = ["minecraft:air", block_name];
        let mut bytes = vec![8, 1, 1 << 1];
        let mut words = vec![0_u32; 128];
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                for local_y in 0..16_u8 {
                    let block_index = block_storage_index(local_x, local_y, local_z);
                    let word_index = block_index / 32;
                    let bit_offset = block_index % 32;
                    words[word_index] |= 1_u32 << bit_offset;
                }
            }
        }
        for word in words {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&(palette.len() as i32).to_le_bytes());
        for name in palette {
            let tag = NbtTag::Compound(IndexMap::from([
                ("name".to_string(), NbtTag::String(name.to_string())),
                ("states".to_string(), NbtTag::Compound(IndexMap::new())),
                ("version".to_string(), NbtTag::Int(1)),
            ]));
            bytes.extend_from_slice(&crate::nbt::serialize_root_nbt(&tag).expect("nbt"));
        }
        bytes
    }

    fn test_named_subchunk_bytes_with_values(
        palette: &[&str],
        value_at: impl Fn(u8, u8, u8) -> u16,
    ) -> Vec<u8> {
        let bits_per_value = match palette.len() {
            0..=2 => 1_u8,
            3..=4 => 2_u8,
            5..=16 => 4_u8,
            _ => 8_u8,
        };
        let values_per_word = usize::from(32 / bits_per_value);
        let word_count = 4096_usize.div_ceil(values_per_word);
        let mut bytes = vec![8, 1, bits_per_value << 1];
        let mut words = vec![0_u32; word_count];
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                for local_y in 0..16_u8 {
                    let value = value_at(local_x, local_z, local_y);
                    if value == 0 {
                        continue;
                    }
                    let block_index = block_storage_index(local_x, local_y, local_z);
                    let word_index = block_index / values_per_word;
                    let bit_offset = (block_index % values_per_word) * usize::from(bits_per_value);
                    words[word_index] |= u32::from(value) << bit_offset;
                }
            }
        }
        for word in words {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&(palette.len() as i32).to_le_bytes());
        for name in palette {
            let tag = NbtTag::Compound(IndexMap::from([
                ("name".to_string(), NbtTag::String((*name).to_string())),
                ("states".to_string(), NbtTag::Compound(IndexMap::new())),
                ("version".to_string(), NbtTag::Int(1)),
            ]));
            bytes.extend_from_slice(&crate::nbt::serialize_root_nbt(&tag).expect("nbt"));
        }
        bytes
    }

    fn test_named_layered_subchunk_bytes(
        lower_palette: &[&str],
        upper_palette: &[&str],
        lower_value_at: impl Fn(u8, u8, u8) -> u16,
        upper_value_at: impl Fn(u8, u8, u8) -> u16,
    ) -> Vec<u8> {
        let mut bytes = vec![8, 2];
        append_named_palette_storage(&mut bytes, lower_palette, lower_value_at);
        append_named_palette_storage(&mut bytes, upper_palette, upper_value_at);
        bytes
    }

    fn append_named_palette_storage(
        bytes: &mut Vec<u8>,
        palette: &[&str],
        value_at: impl Fn(u8, u8, u8) -> u16,
    ) {
        let bits_per_value = match palette.len() {
            0..=2 => 1_u8,
            3..=4 => 2_u8,
            5..=16 => 4_u8,
            _ => 8_u8,
        };
        let values_per_word = usize::from(32 / bits_per_value);
        let word_count = 4096_usize.div_ceil(values_per_word);
        let mut words = vec![0_u32; word_count];
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                for local_y in 0..16_u8 {
                    let value = value_at(local_x, local_z, local_y);
                    if value == 0 {
                        continue;
                    }
                    let block_index = block_storage_index(local_x, local_y, local_z);
                    let word_index = block_index / values_per_word;
                    let bit_offset = (block_index % values_per_word) * usize::from(bits_per_value);
                    words[word_index] |= u32::from(value) << bit_offset;
                }
            }
        }
        bytes.push(bits_per_value << 1);
        for word in words {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&(palette.len() as i32).to_le_bytes());
        for name in palette {
            let tag = NbtTag::Compound(IndexMap::from([
                ("name".to_string(), NbtTag::String((*name).to_string())),
                ("states".to_string(), NbtTag::Compound(IndexMap::new())),
                ("version".to_string(), NbtTag::Int(1)),
            ]));
            bytes.extend_from_slice(&crate::nbt::serialize_root_nbt(&tag).expect("nbt"));
        }
    }

    fn test_asymmetric_legacy_subchunk_bytes() -> Vec<u8> {
        let mut bytes = vec![0_u8; crate::LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN];
        bytes[0] = 2;
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                let block_id = match (local_x >= 8, local_z >= 8) {
                    (false, false) => 1,
                    (true, false) => 12,
                    (false, true) => 24,
                    (true, true) => 45,
                };
                let index = crate::LegacySubChunk::block_index(local_x, 10, local_z)
                    .expect("legacy subchunk index");
                bytes[1 + index] = block_id;
            }
        }
        bytes
    }

    fn test_data2d_bytes(height: i16, biome: u8) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(768);
        for _ in 0..256 {
            bytes.extend_from_slice(&height.to_le_bytes());
        }
        bytes.extend(std::iter::repeat_n(biome, 256));
        bytes
    }

    fn test_data3d_height_bytes(height: i16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(512);
        for _ in 0..256 {
            bytes.extend_from_slice(&height.to_le_bytes());
        }
        bytes
    }

    fn test_asymmetric_data2d_bytes() -> Vec<u8> {
        let mut bytes = Vec::with_capacity(768);
        for local_z in 0..16_i16 {
            for local_x in 0..16_i16 {
                let height = 100 + local_x * 10 + local_z;
                bytes.extend_from_slice(&height.to_le_bytes());
            }
        }
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                bytes.push(local_x * 10 + local_z);
            }
        }
        bytes
    }

    fn test_legacy_terrain_bytes(block_id: u8, height: u8) -> Vec<u8> {
        let mut bytes = vec![0_u8; crate::LEGACY_TERRAIN_VALUE_LEN];
        for local_z in 0..16_u8 {
            for local_x in 0..16_u8 {
                for local_y in 0..=height.min(127) {
                    let index = crate::LegacyTerrain::block_index(local_x, local_y, local_z)
                        .expect("legacy block index");
                    bytes[index] = block_id;
                }
                bytes[crate::LEGACY_TERRAIN_BLOCK_COUNT
                    + crate::LEGACY_TERRAIN_BLOCK_COUNT / 2 * 3
                    + raw_2d_column_index(local_x, local_z)] = height;
            }
        }
        bytes
    }

    fn write_legacy_biome_sample(
        bytes: &mut [u8],
        local_x: u8,
        local_z: u8,
        biome_id: u8,
        color: u32,
    ) {
        let offset = crate::LEGACY_TERRAIN_BLOCK_COUNT
            + crate::LEGACY_TERRAIN_BLOCK_COUNT / 2 * 3
            + 16 * 16
            + raw_2d_column_index(local_x, local_z) * 4;
        bytes[offset] = biome_id;
        bytes[offset + 1] = ((color >> 16) & 0xff) as u8;
        bytes[offset + 2] = ((color >> 8) & 0xff) as u8;
        bytes[offset + 3] = (color & 0xff) as u8;
    }

    fn raw_2d_column_index(local_x: u8, local_z: u8) -> usize {
        usize::from(local_z) * 16 + usize::from(local_x)
    }
}

fn validate_local_column(local_x: u8, local_z: u8) -> Result<()> {
    if local_x >= 16 || local_z >= 16 {
        return Err(BedrockWorldError::Validation(format!(
            "local biome coordinates must be 0..15, got x={local_x}, z={local_z}"
        )));
    }
    Ok(())
}

fn insert_needed_surface_subchunks(
    subchunk_ys: &mut BTreeSet<i8>,
    height_map: Option<&[[Option<i16>; 16]; 16]>,
    min_subchunk_y: i8,
    max_subchunk_y: i8,
) {
    const SURFACE_LOOKDOWN_SUBCHUNKS: i8 = 6;
    const SURFACE_LOOKUP_SUBCHUNKS: i8 = 4;
    let Some(height_map) = height_map else {
        return;
    };
    for row in height_map {
        for height in row.iter().flatten() {
            if let Ok(surface_y) = block_y_to_subchunk_y(i32::from(*height)) {
                let lower_y = surface_y
                    .saturating_sub(SURFACE_LOOKDOWN_SUBCHUNKS)
                    .max(min_subchunk_y);
                let upper_y = surface_y
                    .saturating_add(SURFACE_LOOKUP_SUBCHUNKS)
                    .clamp(min_subchunk_y, max_subchunk_y);
                for subchunk_y in lower_y..=upper_y {
                    subchunk_ys.insert(subchunk_y);
                }
            }
        }
    }
}

fn block_y_to_subchunk_y(y: i32) -> Result<i8> {
    let subchunk_y = y.div_euclid(16);
    i8::try_from(subchunk_y).map_err(|_| {
        BedrockWorldError::Validation(format!(
            "block y={y} cannot be represented as a Bedrock subchunk index"
        ))
    })
}

fn biome_storage_contains_y(storage: &ParsedBiomeStorage, y: i32) -> bool {
    storage
        .y
        .is_none_or(|start_y| (start_y..start_y + 16).contains(&y))
}

fn biome_storage_bucket_y(y: i32) -> i32 {
    y.div_euclid(16) * 16
}

fn biome_id_from_storage(
    storage: &ParsedBiomeStorage,
    local_x: u8,
    local_z: u8,
    y: i32,
) -> Option<u32> {
    let local_y = if let Some(start_y) = storage.y {
        u8::try_from(y - start_y).ok()?
    } else {
        0
    };
    storage.biome_id_at(local_x, local_y, local_z)
}

fn height_map_index(local_x: u8, local_z: u8) -> usize {
    usize::from(local_z) * 16 + usize::from(local_x)
}

fn column_index(local_x: u8, local_z: u8) -> Option<usize> {
    (local_x < 16 && local_z < 16).then_some(height_map_index(local_x, local_z))
}

fn raw_height_at(
    height_map: Option<&[[Option<i16>; 16]; 16]>,
    local_x: u8,
    local_z: u8,
) -> Option<i16> {
    height_map?[usize::from(local_z)][usize::from(local_x)]
}

fn raw_height_mismatch_columns(chunk: &RenderChunkData) -> usize {
    let Some(samples) = chunk.column_samples.as_ref() else {
        return 0;
    };
    let Some(height_map) = chunk.height_map.as_ref() else {
        return 0;
    };
    let mut mismatches = 0usize;
    for local_z in 0..16_u8 {
        for local_x in 0..16_u8 {
            if let Some(sample) = samples.get(local_x, local_z)
                && height_map[usize::from(local_z)][usize::from(local_x)]
                    .is_some_and(|raw_height| raw_height != sample.surface_y)
            {
                mismatches = mismatches.saturating_add(1);
            }
        }
    }
    mismatches
}

fn missing_surface_columns(chunk: &RenderChunkData) -> usize {
    chunk.column_samples.as_ref().map_or(0, |samples| {
        256usize.saturating_sub(samples.sampled_columns())
    })
}

fn needed_exact_surface_chunk_requires_full_reload(chunk: &RenderChunkData) -> Result<bool> {
    let Some(samples) = chunk.column_samples.as_ref() else {
        return Ok(false);
    };
    if samples.sampled_columns() < 16 * 16 {
        return Ok(true);
    }
    if raw_height_mismatch_columns(chunk) > 0 {
        return Ok(true);
    }
    let Some(loaded_max_subchunk_y) = chunk.subchunks.keys().next_back().copied() else {
        return Ok(true);
    };
    let (_, world_max_subchunk_y) = chunk.pos.subchunk_index_range(chunk.version);
    if loaded_max_subchunk_y >= world_max_subchunk_y {
        return Ok(false);
    }
    for sample in samples.iter() {
        if block_y_to_subchunk_y(i32::from(sample.surface_y))? == loaded_max_subchunk_y {
            return Ok(true);
        }
        if let Some(overlay) = sample.overlay.as_ref()
            && block_y_to_subchunk_y(i32::from(overlay.y))? == loaded_max_subchunk_y
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn legacy_world_block_state(id: u8, data: u8) -> BlockState {
    let mut states = BTreeMap::new();
    states.insert("data".to_string(), NbtTag::Byte(data as i8));
    BlockState {
        name: legacy_world_block_name(id, data),
        states,
        version: None,
    }
}

#[allow(clippy::too_many_lines)]
fn legacy_world_block_name(id: u8, data: u8) -> String {
    let name = match id {
        0 => "minecraft:air",
        1 => match data & 0x7 {
            1 => "minecraft:granite",
            2 => "minecraft:polished_granite",
            3 => "minecraft:diorite",
            4 => "minecraft:polished_diorite",
            5 => "minecraft:andesite",
            6 => "minecraft:polished_andesite",
            _ => "minecraft:stone",
        },
        2 => "minecraft:grass_block",
        3 => match data & 0x3 {
            1 => "minecraft:coarse_dirt",
            2 => "minecraft:podzol",
            _ => "minecraft:dirt",
        },
        4 => "minecraft:cobblestone",
        5 => legacy_world_wood_name(data, "planks"),
        6 => "minecraft:oak_sapling",
        7 => "minecraft:bedrock",
        8 | 9 => "minecraft:water",
        10 | 11 => "minecraft:lava",
        12 => match data & 0x1 {
            1 => "minecraft:red_sand",
            _ => "minecraft:sand",
        },
        13 => "minecraft:gravel",
        14 => "minecraft:gold_ore",
        15 => "minecraft:iron_ore",
        16 => "minecraft:coal_ore",
        17 => legacy_world_wood_name(data, "log"),
        18 => legacy_world_wood_name(data, "leaves"),
        19 => "minecraft:sponge",
        20 => "minecraft:glass",
        21 => "minecraft:lapis_ore",
        22 => "minecraft:lapis_block",
        24 => "minecraft:sandstone",
        26 => "minecraft:bed",
        30 => "minecraft:cobweb",
        31 => match data {
            1 => "minecraft:short_grass",
            2 => "minecraft:fern",
            _ => "minecraft:dead_bush",
        },
        32 => "minecraft:dead_bush",
        35 => legacy_world_wool_name(data),
        37 => "minecraft:dandelion",
        38 => "minecraft:poppy",
        39 => "minecraft:brown_mushroom",
        40 => "minecraft:red_mushroom",
        41 => "minecraft:gold_block",
        42 => "minecraft:iron_block",
        43 | 44 => "minecraft:stone_slab",
        45 => "minecraft:bricks",
        46 => "minecraft:tnt",
        47 => "minecraft:bookshelf",
        48 => "minecraft:mossy_cobblestone",
        49 => "minecraft:obsidian",
        50 => "minecraft:torch",
        51 => "minecraft:fire",
        52 => "minecraft:spawner",
        53 => "minecraft:oak_stairs",
        54 => "minecraft:chest",
        56 => "minecraft:diamond_ore",
        57 => "minecraft:diamond_block",
        58 => "minecraft:crafting_table",
        59 => "minecraft:wheat",
        60 => "minecraft:farmland",
        61 | 62 => "minecraft:furnace",
        63 | 68 => "minecraft:oak_sign",
        64 => "minecraft:oak_door",
        65 => "minecraft:ladder",
        66 => "minecraft:rail",
        67 => "minecraft:cobblestone_stairs",
        71 => "minecraft:iron_door",
        73 | 74 => "minecraft:redstone_ore",
        78 => "minecraft:snow",
        79 => "minecraft:ice",
        80 => "minecraft:snow_block",
        81 => "minecraft:cactus",
        82 => "minecraft:clay",
        83 => "minecraft:sugar_cane",
        85 => "minecraft:oak_fence",
        86 => "minecraft:pumpkin",
        87 => "minecraft:netherrack",
        88 => "minecraft:soul_sand",
        89 => "minecraft:glowstone",
        91 => "minecraft:jack_o_lantern",
        95 => "minecraft:invisible_bedrock",
        98 => "minecraft:stone_bricks",
        99 | 100 => "minecraft:mushroom_stem",
        103 => "minecraft:melon",
        106 => "minecraft:vine",
        107 => "minecraft:oak_fence_gate",
        108 => "minecraft:brick_stairs",
        109 => "minecraft:stone_brick_stairs",
        110 => "minecraft:mycelium",
        111 => "minecraft:lily_pad",
        112 => "minecraft:nether_bricks",
        121 => "minecraft:end_stone",
        129 => "minecraft:emerald_ore",
        133 => "minecraft:emerald_block",
        155 => "minecraft:quartz_block",
        159 | 172 => "minecraft:terracotta",
        161 => legacy_world_wood_name(data.saturating_add(4), "leaves"),
        162 => legacy_world_wood_name(data.saturating_add(4), "log"),
        169 => "minecraft:sea_lantern",
        170 => "minecraft:hay_block",
        171 => "minecraft:white_carpet",
        173 => "minecraft:coal_block",
        174 => "minecraft:packed_ice",
        175 => "minecraft:sunflower",
        _ => return format!("legacy:{id}"),
    };
    name.to_string()
}

fn legacy_world_wood_name(data: u8, suffix: &'static str) -> &'static str {
    match (data & 0x7, suffix) {
        (1, "planks") => "minecraft:spruce_planks",
        (2, "planks") => "minecraft:birch_planks",
        (3, "planks") => "minecraft:jungle_planks",
        (4, "planks") => "minecraft:acacia_planks",
        (5, "planks") => "minecraft:dark_oak_planks",
        (_, "planks") => "minecraft:oak_planks",
        (1, "log") => "minecraft:spruce_log",
        (2, "log") => "minecraft:birch_log",
        (3, "log") => "minecraft:jungle_log",
        (4, "log") => "minecraft:acacia_log",
        (5, "log") => "minecraft:dark_oak_log",
        (_, "log") => "minecraft:oak_log",
        (1, "leaves") => "minecraft:spruce_leaves",
        (2, "leaves") => "minecraft:birch_leaves",
        (3, "leaves") => "minecraft:jungle_leaves",
        (4, "leaves") => "minecraft:acacia_leaves",
        (5, "leaves") => "minecraft:dark_oak_leaves",
        _ => "minecraft:oak_leaves",
    }
}

fn legacy_world_wool_name(data: u8) -> &'static str {
    match data & 0x0f {
        1 => "minecraft:orange_wool",
        2 => "minecraft:magenta_wool",
        3 => "minecraft:light_blue_wool",
        4 => "minecraft:yellow_wool",
        5 => "minecraft:lime_wool",
        6 => "minecraft:pink_wool",
        7 => "minecraft:gray_wool",
        8 => "minecraft:light_gray_wool",
        9 => "minecraft:cyan_wool",
        10 => "minecraft:purple_wool",
        11 => "minecraft:blue_wool",
        12 => "minecraft:brown_wool",
        13 => "minecraft:green_wool",
        14 => "minecraft:red_wool",
        15 => "minecraft:black_wool",
        _ => "minecraft:white_wool",
    }
}

fn is_air_block_name(name: &str) -> bool {
    matches!(
        name,
        "air"
            | "cave_air"
            | "void_air"
            | "minecraft:air"
            | "minecraft:cave_air"
            | "minecraft:void_air"
            | "minecraft:structure_void"
            | "minecraft:light_block"
            | "minecraft:light"
    )
}

fn is_water_block_name(name: &str) -> bool {
    matches!(
        name,
        "water" | "flowing_water" | "minecraft:water" | "minecraft:flowing_water"
    )
}

pub fn terrain_surface_role(name: &str) -> TerrainSurfaceRole {
    if is_air_block_name(name) {
        return TerrainSurfaceRole::Air;
    }
    if is_water_block_name(name) {
        return TerrainSurfaceRole::Water;
    }
    if terrain_surface_overlay_alpha(name).is_some() {
        return TerrainSurfaceRole::Overlay;
    }
    TerrainSurfaceRole::Primary
}

pub fn terrain_surface_overlay_alpha(name: &str) -> Option<u8> {
    let name = name.strip_prefix("minecraft:").unwrap_or(name);
    if name.contains("carpet") {
        return None;
    }
    if matches!(
        name,
        "short_grass" | "tallgrass" | "tall_grass" | "fern" | "large_fern" | "vine"
    ) || name.contains("vine")
    {
        return Some(82);
    }
    if matches!(
        name,
        "deadbush"
            | "dead_bush"
            | "brown_mushroom"
            | "red_mushroom"
            | "poppy"
            | "dandelion"
            | "blue_orchid"
            | "allium"
            | "azure_bluet"
            | "oxeye_daisy"
            | "cornflower"
            | "lily_of_the_valley"
            | "wither_rose"
            | "torchflower"
    ) || name.contains("flower")
        || name.contains("sapling")
        || name.contains("bush")
        || name.contains("petals")
        || name.contains("tulip")
    {
        return Some(115);
    }
    if matches!(
        name,
        "tripWire"
            | "trip_wire"
            | "tripwire_hook"
            | "redstone_wire"
            | "rail"
            | "detector_rail"
            | "activator_rail"
            | "golden_rail"
    ) {
        return Some(130);
    }
    if matches!(
        name,
        "torch"
            | "redstone_torch"
            | "unlit_redstone_torch"
            | "soul_torch"
            | "copper_torch"
            | "lever"
    ) || name.contains("button")
        || name.contains("pressure_plate")
    {
        return Some(155);
    }
    None
}
