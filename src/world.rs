//! High-level lazy world access built on top of the storage layer.
//!
//! The methods in this module are intentionally split into blocking and async
//! forms. Blocking methods are the canonical implementation and are appropriate
//! for CLI tools, background worker threads, and tests. Async methods are thin
//! wrappers that offload the same work with `tokio::task::spawn_blocking`.

use crate::chunk::{
    BedrockDbKey, BlockPos, BlockState, Chunk, ChunkKey, ChunkPos, ChunkRecord, ChunkRecordTag,
    SubChunk, SubChunkDecodeMode, parse_subchunk_with_mode,
};
use crate::error::{BedrockWorldError, Result};
use crate::level_dat::{LevelDatDocument, read_level_dat_document, write_level_dat_document};
use crate::nbt::{NbtTag, parse_root_nbt};
use crate::parsed::{
    ItemStack, ParsedBiomeData, ParsedBiomeStorage, ParsedBlockEntity, ParsedChunkData,
    ParsedDbEntry, ParsedDbValue, ParsedEntity, ParsedGlobalData, ParsedMapData, ParsedVillageData,
    ParsedWorld, WorldParseOptions, WorldParseReport, collect_item_stacks,
    parse_block_entities_from_value, parse_chunk_records, parse_chunk_records_with_options,
    parse_data3d, parse_entities_from_value, parse_global_storage_entries, parse_legacy_data2d,
    parse_world_storage,
};
use crate::player::{PlayerData, PlayerId};
#[cfg(feature = "async")]
use crate::storage::backend::BedrockLevelDbStorage;
use crate::storage::{
    StorageBatch, StorageCancelFlag, StorageOp, StorageProgressSink, StorageReadOptions,
    StorageScanMode, StorageThreadingOptions, StorageVisitorControl, WorldStorage,
};
use bytes::Bytes;
use rayon::ThreadPoolBuilder;
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
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self { read_only: true }
    }
}

/// Lazy handle to a Minecraft Bedrock world folder.
///
/// A handle stores the world path and a storage backend. It does not scan or
/// parse the database until a query method is called.
pub struct BedrockWorld {
    path: PathBuf,
    options: OpenOptions,
    storage: Arc<dyn WorldStorage>,
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
pub enum RenderSurfaceSubchunkMode {
    Full,
    Needed,
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
                    .max(work_items.min(256).max(1))
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
pub struct RenderLoadStats {
    pub requested_chunks: usize,
    pub loaded_chunks: usize,
    pub subchunks_decoded: usize,
    pub worker_threads: usize,
    pub queue_wait_ms: u128,
    pub load_ms: u128,
}

#[derive(Debug, Clone)]
pub struct RenderChunkLoadOptions {
    pub surface: bool,
    pub surface_subchunks: RenderSurfaceSubchunkMode,
    pub fixed_y: Option<i32>,
    pub biome_y: Option<i32>,
    pub load_all_biomes: bool,
    pub block_entities: bool,
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
            surface: true,
            surface_subchunks: RenderSurfaceSubchunkMode::Needed,
            fixed_y: None,
            biome_y: Some(64),
            load_all_biomes: false,
            block_entities: false,
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
    pub biome_data: BTreeMap<i32, ParsedBiomeStorage>,
    pub subchunks: BTreeMap<i8, SubChunk>,
    pub block_entities: Vec<RenderBlockEntity>,
    pub version: crate::ChunkVersion,
}

#[derive(Debug, Clone)]
pub struct RenderRegionLoadOptions {
    pub surface: bool,
    pub surface_subchunks: RenderSurfaceSubchunkMode,
    pub fixed_y: Option<i32>,
    pub biome_y: Option<i32>,
    pub load_all_biomes: bool,
    pub block_entities: bool,
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
            surface: true,
            surface_subchunks: RenderSurfaceSubchunkMode::Needed,
            fixed_y: None,
            biome_y: Some(64),
            load_all_biomes: false,
            block_entities: false,
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
            surface: options.surface,
            surface_subchunks: options.surface_subchunks,
            fixed_y: options.fixed_y,
            biome_y: options.biome_y,
            load_all_biomes: options.load_all_biomes,
            block_entities: options.block_entities,
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

impl BedrockWorld {
    #[cfg(feature = "async")]
    pub async fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db_path = path.join("db");
        let read_only = options.read_only;
        let storage = tokio::task::spawn_blocking(move || {
            if read_only {
                BedrockLevelDbStorage::open_read_only(db_path)
            } else {
                BedrockLevelDbStorage::open(db_path)
            }
        })
        .await
        .map_err(|error| BedrockWorldError::Join(error.to_string()))??;
        Ok(Self {
            path,
            options,
            storage: Arc::new(storage),
        })
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
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
        if self.storage.get(b"~local_player")?.is_some() {
            players.push(PlayerId::Local);
        }
        self.storage.for_each_prefix(
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
        self.storage
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
        self.storage
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
        let outcome = self
            .storage
            .for_each_key(to_storage_read_options(&options), &mut |key| {
                check_cancelled(&options)?;
                entries_seen = entries_seen.saturating_add(1);
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                    && is_render_chunk_record_tag(chunk_key.tag)
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
        self.storage
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
        self.storage
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
        self.storage
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
        self.storage.put(key.as_ref(), &player.raw)
    }

    pub fn get_chunk_blocking(&self, pos: ChunkPos) -> Result<Chunk> {
        let mut records = Vec::new();
        let prefix = chunk_record_prefix(pos);
        self.storage.for_each_prefix(
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
        parse_world_storage(level_dat, self.storage.as_ref(), options)
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
        self.storage
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
            let Some(value) = self.storage.get(&key)? else {
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
        self.storage.for_each_prefix_key(
            &prefix,
            to_storage_read_options(options),
            &mut |key| {
                check_cancelled(options)?;
                if let BedrockDbKey::Chunk(chunk_key) = BedrockDbKey::decode(key)
                    && chunk_key.pos == pos
                    && is_render_chunk_record_tag(chunk_key.tag)
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
            let mut heights = [[None; 16]; 16];
            for local_z in 0..16_u8 {
                for local_x in 0..16_u8 {
                    let index = height_map_index(local_x, local_z);
                    heights[usize::from(local_z)][usize::from(local_x)] =
                        biome_data.height_map.get(index).copied();
                }
            }
            return Ok(Some(heights));
        }
        Ok(None)
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
        check_render_load_cancelled(&options)?;
        let height_map = self.get_height_map_blocking(pos)?;
        let mut biome_data = BTreeMap::new();
        if options.load_all_biomes {
            if let Some(storages) = self.get_biome_storages_blocking(pos)? {
                for storage in storages {
                    let key = storage.y.unwrap_or(i32::MIN);
                    biome_data.insert(key, storage);
                }
            }
        } else if let Some(biome_y) = options.biome_y
            && let Some(storage) = self.get_biome_storage_blocking(pos, biome_y)?
        {
            biome_data.insert(biome_storage_bucket_y(biome_y), storage);
        }

        let mut subchunk_ys = BTreeSet::new();
        if let Some(fixed_y) = options.fixed_y {
            subchunk_ys.insert(block_y_to_subchunk_y(fixed_y)?);
        }
        if options.surface {
            let (min_y, max_y) = pos.subchunk_index_range(crate::ChunkVersion::New);
            match options.surface_subchunks {
                RenderSurfaceSubchunkMode::Full => {
                    for y in min_y..=max_y {
                        subchunk_ys.insert(y);
                    }
                }
                RenderSurfaceSubchunkMode::Needed => {
                    insert_needed_surface_subchunks(
                        &mut subchunk_ys,
                        height_map.as_ref(),
                        min_y,
                        max_y,
                    );
                }
            }
        }

        let mut subchunks = BTreeMap::new();
        for y in subchunk_ys {
            check_render_load_cancelled(&options)?;
            if let Some(subchunk) = self.parse_subchunk_blocking(
                pos,
                y,
                WorldParseOptions {
                    subchunk_decode_mode: options.subchunk_decode,
                    ..WorldParseOptions::summary()
                },
            )? {
                subchunks.insert(y, subchunk);
            }
        }

        let block_entities = if options.block_entities {
            self.get_chunk_blocking(pos)?
                .get_block_entities()?
                .into_iter()
                .map(|entity| render_block_entity_from_nbt(entity.tag))
                .collect()
        } else {
            Vec::new()
        };

        Ok(RenderChunkData {
            pos,
            is_loaded: height_map.is_some()
                || !biome_data.is_empty()
                || !subchunks.is_empty()
                || !block_entities.is_empty(),
            height_map,
            biome_data,
            subchunks,
            block_entities,
            version: crate::ChunkVersion::New,
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

    fn load_render_chunks_with_stats_blocking(
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
            "loading render chunks (chunks={}, workers={}, surface={}, fixed_y={:?}, biome_y={:?}, block_entities={}, queue_depth={}, priority={:?})",
            positions.len(),
            worker_count,
            options.surface,
            options.fixed_y,
            options.biome_y,
            options.block_entities,
            options
                .pipeline
                .resolve_queue_depth(worker_count, positions.len()),
            options.priority
        );
        if worker_count == 1 {
            let mut chunks = Vec::with_capacity(positions.len());
            let mut completed = 0usize;
            for pos in positions {
                check_render_load_cancelled(&options)?;
                chunks.push(self.load_render_chunk_blocking(pos, options.clone())?);
                completed = completed.saturating_add(1);
                emit_render_load_progress(&options, completed);
            }
            let stats = render_load_stats(&chunks, worker_count, 0, started.elapsed().as_millis());
            log_render_load_complete(&stats);
            return Ok((chunks, stats));
        }

        let next_position = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let queue_depth = options
            .pipeline
            .resolve_queue_depth(worker_count, positions.len());
        let queue_wait_ms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (sender, receiver) =
            mpsc::sync_channel::<(usize, Result<RenderChunkData>)>(queue_depth);
        let pool = world_pool(worker_count)?;
        pool.scope(|scope| {
            for worker_index in 0..worker_count {
                let next_position = Arc::clone(&next_position);
                let sender = sender.clone();
                let positions = &positions;
                let options = options.clone();
                let queue_wait_ms = Arc::clone(&queue_wait_ms);
                scope.spawn(move |_| {
                    log::trace!("render chunk load worker {worker_index} started");
                    loop {
                        if check_render_load_cancelled(&options).is_err() {
                            return;
                        }
                        let index = next_position.fetch_add(1, Ordering::Relaxed);
                        let Some(pos) = positions.get(index).copied() else {
                            log::trace!("render chunk load worker {worker_index} finished");
                            return;
                        };
                        let send_started = Instant::now();
                        let result = sender
                            .send((index, self.load_render_chunk_blocking(pos, options.clone())));
                        queue_wait_ms.fetch_add(
                            u64::try_from(send_started.elapsed().as_millis()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                        if result.is_err() {
                            return;
                        }
                    }
                });
            }
            drop(sender);

            let mut results = Vec::with_capacity(positions.len());
            results.resize_with(positions.len(), || None);
            let mut completed = 0usize;
            for (index, result) in receiver {
                let slot = results.get_mut(index).ok_or_else(|| {
                    BedrockWorldError::Validation(
                        "render chunk worker returned an invalid index".to_string(),
                    )
                })?;
                *slot = Some(result);
                completed = completed.saturating_add(1);
                emit_render_load_progress(&options, completed);
            }
            let chunks = results
                .into_iter()
                .map(|result| {
                    result.ok_or_else(|| {
                        BedrockWorldError::Validation(
                            "render chunk worker did not return a result".to_string(),
                        )
                    })?
                })
                .collect::<Result<Vec<_>>>()?;
            let stats = render_load_stats(
                &chunks,
                worker_count,
                u128::from(queue_wait_ms.load(Ordering::Relaxed)),
                started.elapsed().as_millis(),
            );
            log_render_load_complete(&stats);
            Ok((chunks, stats))
        })
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
        parse_global_storage_entries(self.storage.as_ref(), WorldParseOptions::summary())
    }

    pub fn scan_entities_blocking(
        &self,
        options: WorldScanOptions,
    ) -> Result<(Vec<ParsedEntity>, WorldParseReport)> {
        let mut report = WorldParseReport::default();
        let mut entities = Vec::new();
        let mut entries_seen = 0usize;
        self.storage
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
        self.storage
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
        self.storage
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

    #[cfg(feature = "async")]
    #[must_use]
    fn blocking_clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            options: self.options.clone(),
            storage: self.storage.clone(),
        }
    }

    pub fn put_raw_record_blocking(&self, key: &ChunkKey, value: &[u8]) -> Result<()> {
        self.ensure_writable()?;
        self.storage.put(&key.encode(), value)
    }

    pub fn delete_raw_record_blocking(&self, key: &ChunkKey) -> Result<()> {
        self.ensure_writable()?;
        self.storage.delete(&key.encode())
    }

    #[must_use]
    pub fn transaction(&self) -> WorldTransaction {
        WorldTransaction {
            storage: self.storage.clone(),
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
pub struct WorldTransaction {
    storage: Arc<dyn WorldStorage>,
    batch: StorageBatch,
    read_only: bool,
}

impl WorldTransaction {
    pub fn put_raw_record(&mut self, key: &ChunkKey, value: impl Into<Bytes>) {
        self.batch.put(key.encode(), value.into());
    }

    pub fn delete_raw_record(&mut self, key: &ChunkKey) {
        self.batch.delete(key.encode());
    }

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

    pub fn commit(self) -> Result<()> {
        if self.read_only {
            return Err(BedrockWorldError::ReadOnly);
        }
        validate_batch(&self.batch)?;
        self.storage.write_batch(&self.batch)?;
        self.storage.flush()
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
    }
}

fn log_render_load_complete(stats: &RenderLoadStats) {
    log::debug!(
        "render chunk load complete (requested_chunks={}, loaded_chunks={}, missing_chunks={}, subchunks_decoded={}, worker_threads={}, queue_wait_ms={}, load_ms={})",
        stats.requested_chunks,
        stats.loaded_chunks,
        stats.requested_chunks.saturating_sub(stats.loaded_chunks),
        stats.subchunks_decoded,
        stats.worker_threads,
        stats.queue_wait_ms,
        stats.load_ms
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

const fn is_render_chunk_record_tag(tag: ChunkRecordTag) -> bool {
    matches!(
        tag,
        ChunkRecordTag::Data3D
            | ChunkRecordTag::Data2D
            | ChunkRecordTag::Data2DLegacy
            | ChunkRecordTag::SubChunkPrefix
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Dimension, MemoryStorage, NbtTag, block_storage_index};
    use indexmap::IndexMap;
    use std::sync::Arc;

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

        let writable_world =
            BedrockWorld::from_storage("memory", storage.clone(), OpenOptions { read_only: false });
        let mut transaction = writable_world.transaction();
        transaction.put_raw_record(&key, Bytes::from_static(b"\x02"));
        transaction.commit().expect("writable commit");

        assert_eq!(
            storage.get(&encoded).expect("get"),
            Some(Bytes::from_static(b"\x02"))
        );
    }

    #[test]
    fn biome_id_query_reads_legacy_data2d() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let storage = Arc::new(MemoryStorage::new());
        let mut data2d = vec![0_u8; 512];
        data2d.extend((0..256).map(|index| (index % 4) as u8));
        storage
            .put(
                &ChunkKey::new(pos, ChunkRecordTag::Data2D).encode(),
                &data2d,
            )
            .expect("put Data2D");
        let world = BedrockWorld::from_storage("memory", storage, OpenOptions::default());

        assert_eq!(
            world
                .get_biome_id_blocking(pos, 3, 2, 64)
                .expect("biome id"),
            Some(3)
        );
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
                    surface_subchunks: RenderSurfaceSubchunkMode::Needed,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("needed render chunk");
        let full = world
            .load_render_chunk_blocking(
                pos,
                RenderChunkLoadOptions {
                    surface_subchunks: RenderSurfaceSubchunkMode::Full,
                    ..RenderChunkLoadOptions::default()
                },
            )
            .expect("full render chunk");
        assert!(needed.subchunks.contains_key(&4));
        assert_eq!(needed.subchunks.get(&4), full.subchunks.get(&4));
        assert!(needed.subchunks.len() <= full.subchunks.len());
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
                    block_entities: true,
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

    fn test_data2d_bytes(height: i16, biome: u8) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(768);
        for _ in 0..256 {
            bytes.extend_from_slice(&height.to_le_bytes());
        }
        bytes.extend(std::iter::repeat_n(biome, 256));
        bytes
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
    let Some(height_map) = height_map else {
        return;
    };
    for row in height_map {
        for height in row.iter().flatten() {
            if let Ok(surface_y) = block_y_to_subchunk_y(i32::from(*height)) {
                let lower_y = surface_y
                    .saturating_sub(SURFACE_LOOKDOWN_SUBCHUNKS)
                    .max(min_subchunk_y);
                let upper_y = surface_y.clamp(min_subchunk_y, max_subchunk_y);
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
