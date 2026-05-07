//! Professional map queries used by map viewers and offline tools.

use crate::error::{BedrockWorldError, Result};
use crate::nbt::{NbtTag, serialize_root_nbt};
use crate::parsed::{
    ActorResolution, ParsedBlockEntity, ParsedChunkRecordValue, ParsedEntity,
    ParsedHardcodedSpawnArea, ParsedVillageData, RetentionMode, WorldParseCategories,
    WorldParseOptions,
};
use crate::world::{BedrockWorld, ChunkBounds, SurfaceColumnOptions, WorldStorageHandle};
use crate::{
    BlockPos, CancelFlag, ChunkPos, ChunkRecordTag, Dimension, RenderChunkRegion,
    SubChunkDecodeMode, SurfaceColumn,
};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::path::PathBuf;

const MT_N: usize = 624;
const MT_M: usize = 397;
const MT_MATRIX_A: u32 = 0x9908_b0df;
const MT_UPPER_MASK: u32 = 0x8000_0000;
const MT_LOWER_MASK: u32 = 0x7fff_ffff;
const WRITE_CONFIRM_TOKEN: &str = "CONFIRMED";

/// Inclusive chunk bounds used by professional map queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlimeChunkBounds {
    /// Bedrock dimension queried by these bounds.
    pub dimension: Dimension,
    /// Inclusive minimum chunk X coordinate.
    pub min_chunk_x: i32,
    /// Inclusive maximum chunk X coordinate.
    pub max_chunk_x: i32,
    /// Inclusive minimum chunk Z coordinate.
    pub min_chunk_z: i32,
    /// Inclusive maximum chunk Z coordinate.
    pub max_chunk_z: i32,
}

impl SlimeChunkBounds {
    /// Validates this value and returns a typed error on failure.
    pub fn validate(self) -> Result<()> {
        if self.min_chunk_x > self.max_chunk_x || self.min_chunk_z > self.max_chunk_z {
            return Err(BedrockWorldError::Validation(format!(
                "invalid chunk bounds: min=({}, {}) max=({}, {})",
                self.min_chunk_x, self.min_chunk_z, self.max_chunk_x, self.max_chunk_z
            )));
        }
        Ok(())
    }

    #[must_use]
    /// Returns the number of chunks covered by these inclusive bounds.
    pub const fn chunk_count(self) -> usize {
        let width = self.max_chunk_x.saturating_sub(self.min_chunk_x) as usize + 1;
        let height = self.max_chunk_z.saturating_sub(self.min_chunk_z) as usize + 1;
        width.saturating_mul(height)
    }

    #[must_use]
    /// Converts generic chunk bounds into slime-query bounds.
    pub fn from_chunk_bounds(bounds: ChunkBounds) -> Self {
        Self {
            dimension: bounds.dimension,
            min_chunk_x: bounds.min_chunk_x,
            max_chunk_x: bounds.max_chunk_x,
            min_chunk_z: bounds.min_chunk_z,
            max_chunk_z: bounds.max_chunk_z,
        }
    }

    #[must_use]
    /// Returns the midpoint chunk X/Z coordinates for these inclusive bounds.
    pub const fn center(self) -> (i32, i32) {
        (
            i32::midpoint(self.min_chunk_x, self.max_chunk_x),
            i32::midpoint(self.min_chunk_z, self.max_chunk_z),
        )
    }
}

impl From<RenderChunkRegion> for SlimeChunkBounds {
    fn from(region: RenderChunkRegion) -> Self {
        Self {
            dimension: region.dimension,
            min_chunk_x: region.min_chunk_x,
            max_chunk_x: region.max_chunk_x,
            min_chunk_z: region.min_chunk_z,
            max_chunk_z: region.max_chunk_z,
        }
    }
}

/// Supported square windows for slime-farm candidate queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlimeWindowSize(u8);

impl SlimeWindowSize {
    /// Creates a new value.
    pub fn new(size: u8) -> Result<Self> {
        if size == 0 || size.is_multiple_of(2) {
            return Err(BedrockWorldError::Validation(format!(
                "slime query window must be a positive odd size, got {size}"
            )));
        }
        Ok(Self(size))
    }

    #[must_use]
    /// Returns the value at the requested coordinates.
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Ranked slime chunk window candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlimeChunkWindow {
    /// Center chunk for this candidate window.
    pub center: ChunkPos,
    /// Inclusive minimum chunk X coordinate.
    pub min_chunk_x: i32,
    /// Inclusive maximum chunk X coordinate.
    pub max_chunk_x: i32,
    /// Inclusive minimum chunk Z coordinate.
    pub min_chunk_z: i32,
    /// Inclusive maximum chunk Z coordinate.
    pub max_chunk_z: i32,
    /// Number of slime chunks inside the window.
    pub slime_count: usize,
    /// Total number of chunks inside the window.
    pub total_count: usize,
}

/// One raw chunk record exposed through the query API.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkRecordDetail {
    /// Bedrock chunk record tag for this value.
    pub tag: ChunkRecordTag,
    /// Length of the original storage value in bytes.
    pub raw_value_len: usize,
    /// Consecutive Bedrock NBT roots decoded from the value.
    pub roots: Vec<NbtTag>,
    /// Whether the record can be written back as NBT by this API.
    pub writable_nbt: bool,
}

/// Detailed query result for one chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkDetail {
    /// Chunk position queried for this detail result.
    pub pos: ChunkPos,
    /// Records included in this result.
    pub records: Vec<ChunkRecordDetail>,
}

/// Block/tip information for one map coordinate.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockTip {
    /// World block position for the queried map coordinate.
    pub block: BlockPos,
    /// Chunk containing the queried block.
    pub chunk: ChunkPos,
    /// Local X coordinate within the chunk, in the range 0..16.
    pub local_x: u8,
    /// Local Z coordinate within the chunk, in the range 0..16.
    pub local_z: u8,
    /// Surface-column sample for the queried block, when available.
    pub surface: Option<SurfaceColumn>,
    /// Biome id associated with the sampled column.
    pub biome_id: Option<u32>,
    /// Height in pixels or blocks, depending on the surrounding type.
    pub height: Option<i16>,
    /// Whether the chunk is a Bedrock slime chunk.
    pub is_slime_chunk: bool,
}

/// Entity marker shown by map overlays.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityOverlay {
    /// Entity identifier decoded from NBT, when present.
    pub identifier: Option<String>,
    /// World position `[x, y, z]` decoded from the entity record.
    pub position: [f64; 3],
    /// Chunk containing the entity position.
    pub chunk: ChunkPos,
    /// Original or parsed Bedrock NBT payload.
    pub nbt: NbtTag,
}

/// Block entity marker shown by map overlays.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockEntityOverlay {
    /// Identifier value decoded from storage or NBT.
    pub id: Option<String>,
    /// World block position `[x, y, z]` decoded from the block entity.
    pub position: [i32; 3],
    /// Chunk containing the block entity position.
    pub chunk: ChunkPos,
    /// Original or parsed Bedrock NBT payload.
    pub nbt: NbtTag,
}

/// Hardcoded spawn area overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardcodedSpawnAreaOverlay {
    /// Parsed hardcoded spawn area.
    pub area: ParsedHardcodedSpawnArea,
    /// Chunk containing the hardcoded spawn area anchor.
    pub chunk: ChunkPos,
}

/// Village overlay. Bounds are best-effort because village NBT shapes vary by version.
#[derive(Debug, Clone, PartialEq)]
pub struct VillageOverlay {
    /// Decoded storage key for this record.
    pub key: crate::ParsedVillageKey,
    /// Inclusive chunk bounds for this value.
    pub bounds: Option<SlimeChunkBounds>,
    /// Number of NBT roots decoded from the value.
    pub root_count: usize,
    /// Length of the original raw value in bytes.
    pub raw_len: usize,
}

/// Reusable village overlay index for map viewers.
#[derive(Debug, Clone, PartialEq)]
pub struct VillageOverlayIndex {
    /// Whether village records are included.
    pub villages: Vec<VillageOverlay>,
}

impl VillageOverlayIndex {
    /// Builds a reusable village overlay index on the calling thread.
    pub fn build_blocking_with_control<S>(
        world: &BedrockWorld<S>,
        cancel: &CancelFlag,
    ) -> Result<Self>
    where
        S: WorldStorageHandle,
    {
        check_query_cancelled(Some(cancel))?;
        let mut villages = Vec::new();
        for village in world.scan_villages_lightweight_blocking(cancel)? {
            check_query_cancelled(Some(cancel))?;
            villages.push(village_overlay(village));
        }
        Ok(Self { villages })
    }

    #[must_use]
    /// Returns village overlays intersecting the requested bounds, capped by `max_items`.
    pub fn query(&self, bounds: SlimeChunkBounds, max_items: usize) -> Vec<VillageOverlay> {
        self.villages
            .iter()
            .filter(|overlay| {
                overlay
                    .bounds
                    .is_none_or(|village_bounds| bounds_intersect(bounds, village_bounds))
            })
            .take(max_items)
            .cloned()
            .collect()
    }
}

/// Overlay query result for a map region.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionOverlayQuery {
    /// Inclusive chunk bounds for this value.
    pub bounds: SlimeChunkBounds,
    /// Slime chunk positions in the queried region.
    pub slime_chunks: Vec<ChunkPos>,
    /// Hardcoded spawn area overlays in the queried region.
    pub hardcoded_spawn_areas: Vec<HardcodedSpawnAreaOverlay>,
    /// Parsed entity records included in this value.
    pub entities: Vec<EntityOverlay>,
    /// Whether block-entity records are loaded with render data.
    pub block_entities: Vec<BlockEntityOverlay>,
    /// Whether village records are included.
    pub villages: Vec<VillageOverlay>,
    /// Number of chunks scanned for this query.
    pub scanned_chunks: usize,
    /// Number of expected chunks missing from storage.
    pub missing_chunks: usize,
}

/// Query options with hard limits for interactive map use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionOverlayQueryOptions {
    /// Whether slime chunk overlays are included.
    pub include_slime: bool,
    /// Whether hardcoded spawn areas are included.
    pub include_hardcoded_spawn_areas: bool,
    /// Whether entity overlays are included.
    pub include_entities: bool,
    /// Whether block-entity overlays are included.
    pub include_block_entities: bool,
    /// Whether village overlays are included.
    pub include_villages: bool,
    /// Maximum chunks accepted for this query.
    pub max_chunks: usize,
    /// Maximum overlay items returned for each item kind.
    pub max_items_per_kind: usize,
}

impl Default for RegionOverlayQueryOptions {
    fn default() -> Self {
        Self {
            include_slime: true,
            include_hardcoded_spawn_areas: true,
            include_entities: true,
            include_block_entities: true,
            include_villages: true,
            max_chunks: 65_536,
            max_items_per_kind: 10_000,
        }
    }
}

/// Aggregate statistics for a selected chunk area.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SelectionStats {
    /// Inclusive chunk bounds for this value.
    pub bounds: Option<SlimeChunkBounds>,
    /// Number of chunks represented by these bounds.
    pub chunk_count: usize,
    /// Number of chunks with renderable data loaded.
    pub loaded_chunks: usize,
    /// Number of expected chunks missing from storage.
    pub missing_chunks: usize,
    /// Slime chunk positions in the queried region.
    pub slime_chunks: usize,
    /// Number of entity overlays found in the selection.
    pub entity_count: usize,
    /// Number of block entity overlays found in the selection.
    pub block_entity_count: usize,
    /// Number of hardcoded spawn area overlays found in the selection.
    pub hardcoded_spawn_area_count: usize,
    /// Number of village overlays found in the selection.
    pub village_count: usize,
}

/// Explicit write guard required by mutating query APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteGuard {
    world_path: PathBuf,
    confirmation_token: String,
    operation: String,
}

impl WriteGuard {
    #[must_use]
    /// Creates a confirmed write guard for a specific world path and operation.
    pub fn confirmed(world_path: impl Into<PathBuf>, operation: impl Into<String>) -> Self {
        Self {
            world_path: world_path.into(),
            confirmation_token: WRITE_CONFIRM_TOKEN.to_string(),
            operation: operation.into(),
        }
    }

    fn validate<S>(&self, world: &BedrockWorld<S>) -> Result<()>
    where
        S: WorldStorageHandle,
    {
        if self.confirmation_token != WRITE_CONFIRM_TOKEN || self.operation.trim().is_empty() {
            return Err(BedrockWorldError::Validation(
                "write guard is not confirmed".to_string(),
            ));
        }
        if self.world_path != world.path() {
            return Err(BedrockWorldError::Validation(format!(
                "write guard world path does not match: guard={} world={}",
                self.world_path.display(),
                world.path().display()
            )));
        }
        Ok(())
    }
}

#[must_use]
/// Is bedrock slime chunk.
pub fn is_bedrock_slime_chunk(chunk_x: i32, chunk_z: i32) -> bool {
    let seed = (chunk_x as u32).wrapping_mul(0x1f1f_1f1f) ^ (chunk_z as u32);
    mt19937_first_u32(seed).is_multiple_of(10)
}

#[must_use]
/// Is slime chunk.
pub fn is_slime_chunk(pos: ChunkPos) -> bool {
    pos.dimension == Dimension::Overworld && is_bedrock_slime_chunk(pos.x, pos.z)
}

/// Query slime chunk windows.
pub fn query_slime_chunk_windows(
    bounds: SlimeChunkBounds,
    window_size: SlimeWindowSize,
    max_results: usize,
) -> Result<Vec<SlimeChunkWindow>> {
    bounds.validate()?;
    if bounds.dimension != Dimension::Overworld || max_results == 0 {
        return Ok(Vec::new());
    }
    let width = bounds.max_chunk_x.saturating_sub(bounds.min_chunk_x) as usize + 1;
    let height = bounds.max_chunk_z.saturating_sub(bounds.min_chunk_z) as usize + 1;
    let size = usize::from(window_size.get());
    if width < size || height < size {
        return Ok(Vec::new());
    }
    let mut prefix = vec![0usize; (width + 1) * (height + 1)];
    for z in 0..height {
        let chunk_z = bounds
            .min_chunk_z
            .saturating_add(i32::try_from(z).unwrap_or(i32::MAX));
        for x in 0..width {
            let chunk_x = bounds
                .min_chunk_x
                .saturating_add(i32::try_from(x).unwrap_or(i32::MAX));
            let value = usize::from(is_bedrock_slime_chunk(chunk_x, chunk_z));
            let index = (z + 1) * (width + 1) + (x + 1);
            prefix[index] =
                value + prefix[z * (width + 1) + (x + 1)] + prefix[(z + 1) * (width + 1) + x]
                    - prefix[z * (width + 1) + x];
        }
    }
    let (center_x, center_z) = bounds.center();
    let mut windows = Vec::with_capacity((width - size + 1).saturating_mul(height - size + 1));
    for z in 0..=(height - size) {
        for x in 0..=(width - size) {
            let x2 = x + size;
            let z2 = z + size;
            let count = prefix[z2 * (width + 1) + x2] + prefix[z * (width + 1) + x]
                - prefix[z * (width + 1) + x2]
                - prefix[z2 * (width + 1) + x];
            let min_chunk_x = bounds.min_chunk_x + i32::try_from(x).unwrap_or(i32::MAX);
            let min_chunk_z = bounds.min_chunk_z + i32::try_from(z).unwrap_or(i32::MAX);
            let max_chunk_x = min_chunk_x + i32::from(window_size.get()) - 1;
            let max_chunk_z = min_chunk_z + i32::from(window_size.get()) - 1;
            windows.push(SlimeChunkWindow {
                center: ChunkPos {
                    x: i32::midpoint(min_chunk_x, max_chunk_x),
                    z: i32::midpoint(min_chunk_z, max_chunk_z),
                    dimension: bounds.dimension,
                },
                min_chunk_x,
                max_chunk_x,
                min_chunk_z,
                max_chunk_z,
                slime_count: count,
                total_count: size * size,
            });
        }
    }
    windows.sort_by_key(|window| {
        let dx = i64::from(window.center.x) - i64::from(center_x);
        let dz = i64::from(window.center.z) - i64::from(center_z);
        (
            Reverse(window.slime_count),
            dx.saturating_mul(dx).saturating_add(dz.saturating_mul(dz)),
            window.center.z,
            window.center.x,
        )
    });
    windows.truncate(max_results);
    Ok(windows)
}

/// Query block tip blocking.
pub fn query_block_tip_blocking<S>(
    world: &BedrockWorld<S>,
    block: BlockPos,
    dimension: Dimension,
) -> Result<BlockTip>
where
    S: WorldStorageHandle,
{
    let chunk = block.to_chunk_pos(dimension);
    let (local_x, _, local_z) = block.in_chunk_offset();
    let surface = world.get_surface_column_blocking(
        chunk,
        local_x,
        local_z,
        SurfaceColumnOptions::default(),
    )?;
    let height = world.get_height_at_blocking(chunk, local_x, local_z)?;
    let biome_y = surface.as_ref().map_or(block.y, |surface| surface.y);
    let biome_id = world.get_biome_id_blocking(chunk, local_x, local_z, biome_y)?;
    Ok(BlockTip {
        block,
        chunk,
        local_x,
        local_z,
        surface,
        biome_id,
        height,
        is_slime_chunk: is_slime_chunk(chunk),
    })
}

/// Query chunk detail blocking.
pub fn query_chunk_detail_blocking<S>(world: &BedrockWorld<S>, pos: ChunkPos) -> Result<ChunkDetail>
where
    S: WorldStorageHandle,
{
    let chunk = world.get_chunk_blocking(pos)?;
    let mut records = Vec::with_capacity(chunk.records.len());
    for record in chunk.records {
        let roots = parse_record_roots(record.key.tag, &record.value);
        records.push(ChunkRecordDetail {
            tag: record.key.tag,
            raw_value_len: record.value.len(),
            writable_nbt: record_tag_accepts_nbt_write(record.key.tag),
            roots,
        });
    }
    Ok(ChunkDetail { pos, records })
}

/// Query region overlays blocking.
pub fn query_region_overlays_blocking<S>(
    world: &BedrockWorld<S>,
    bounds: SlimeChunkBounds,
    options: RegionOverlayQueryOptions,
) -> Result<RegionOverlayQuery>
where
    S: WorldStorageHandle,
{
    query_region_overlays_blocking_inner(world, bounds, options, None)
}

/// Query region overlays blocking with control.
pub fn query_region_overlays_blocking_with_control<S>(
    world: &BedrockWorld<S>,
    bounds: SlimeChunkBounds,
    options: RegionOverlayQueryOptions,
    cancel: &CancelFlag,
) -> Result<RegionOverlayQuery>
where
    S: WorldStorageHandle,
{
    query_region_overlays_blocking_inner(world, bounds, options, Some(cancel))
}

fn query_region_overlays_blocking_inner<S>(
    world: &BedrockWorld<S>,
    bounds: SlimeChunkBounds,
    options: RegionOverlayQueryOptions,
    cancel: Option<&CancelFlag>,
) -> Result<RegionOverlayQuery>
where
    S: WorldStorageHandle,
{
    bounds.validate()?;
    if bounds.chunk_count() > options.max_chunks {
        return Err(BedrockWorldError::Validation(format!(
            "query covers {} chunks, limit is {}",
            bounds.chunk_count(),
            options.max_chunks
        )));
    }
    let mut result = RegionOverlayQuery {
        bounds,
        slime_chunks: Vec::new(),
        hardcoded_spawn_areas: Vec::new(),
        entities: Vec::new(),
        block_entities: Vec::new(),
        villages: Vec::new(),
        scanned_chunks: 0,
        missing_chunks: 0,
    };
    let chunk_parse_options = overlay_chunk_parse_options(options);
    let needs_chunk_records = overlay_options_need_chunk_records(options);
    for chunk_z in bounds.min_chunk_z..=bounds.max_chunk_z {
        check_query_cancelled(cancel)?;
        for chunk_x in bounds.min_chunk_x..=bounds.max_chunk_x {
            check_query_cancelled(cancel)?;
            let pos = ChunkPos {
                x: chunk_x,
                z: chunk_z,
                dimension: bounds.dimension,
            };
            if options.include_slime && is_slime_chunk(pos) {
                result.slime_chunks.push(pos);
            }
            if !needs_chunk_records {
                continue;
            }
            let parsed = world.parse_chunk_with_options_blocking(pos, chunk_parse_options)?;
            if parsed.records.is_empty() {
                result.missing_chunks = result.missing_chunks.saturating_add(1);
                continue;
            }
            result.scanned_chunks = result.scanned_chunks.saturating_add(1);
            for record in parsed.records {
                match record.value {
                    ParsedChunkRecordValue::HardcodedSpawnAreas(areas)
                        if options.include_hardcoded_spawn_areas =>
                    {
                        for area in areas {
                            if result.hardcoded_spawn_areas.len() >= options.max_items_per_kind {
                                break;
                            }
                            result
                                .hardcoded_spawn_areas
                                .push(HardcodedSpawnAreaOverlay { area, chunk: pos });
                        }
                    }
                    ParsedChunkRecordValue::Entities(entities) if options.include_entities => {
                        push_entities(
                            &mut result.entities,
                            entities,
                            pos,
                            options.max_items_per_kind,
                        );
                    }
                    ParsedChunkRecordValue::BlockEntities(block_entities)
                        if options.include_block_entities =>
                    {
                        push_block_entities(
                            &mut result.block_entities,
                            block_entities,
                            pos,
                            options.max_items_per_kind,
                        );
                    }
                    _ => {}
                }
            }
        }
    }
    if options.include_villages {
        check_query_cancelled(cancel)?;
        let village_cancel = cancel.cloned().unwrap_or_default();
        let index = VillageOverlayIndex::build_blocking_with_control(world, &village_cancel)?;
        result.villages = index.query(bounds, options.max_items_per_kind);
    }
    Ok(result)
}

fn overlay_options_need_chunk_records(options: RegionOverlayQueryOptions) -> bool {
    options.include_hardcoded_spawn_areas
        || options.include_entities
        || options.include_block_entities
}

fn overlay_chunk_parse_options(options: RegionOverlayQueryOptions) -> WorldParseOptions {
    WorldParseOptions {
        categories: WorldParseCategories {
            chunks: true,
            players: false,
            actors: options.include_entities,
            maps: false,
            villages: false,
            globals: false,
        },
        retention: RetentionMode::Structured,
        subchunk_decode_mode: SubChunkDecodeMode::CountsOnly,
        actor_resolution: if options.include_entities {
            ActorResolution::ResolveReferenced
        } else {
            ActorResolution::None
        },
    }
}

fn check_query_cancelled(cancel: Option<&CancelFlag>) -> Result<()> {
    if cancel.is_some_and(CancelFlag::is_cancelled) {
        return Err(BedrockWorldError::Cancelled {
            operation: "region overlay query",
        });
    }
    Ok(())
}

/// Query selection stats blocking.
pub fn query_selection_stats_blocking<S>(
    world: &BedrockWorld<S>,
    bounds: SlimeChunkBounds,
    options: RegionOverlayQueryOptions,
) -> Result<SelectionStats>
where
    S: WorldStorageHandle,
{
    let overlays = query_region_overlays_blocking(world, bounds, options)?;
    Ok(SelectionStats {
        bounds: Some(bounds),
        chunk_count: bounds.chunk_count(),
        loaded_chunks: overlays.scanned_chunks,
        missing_chunks: overlays.missing_chunks,
        slime_chunks: overlays.slime_chunks.len(),
        entity_count: overlays.entities.len(),
        block_entity_count: overlays.block_entities.len(),
        hardcoded_spawn_area_count: overlays.hardcoded_spawn_areas.len(),
        village_count: overlays.villages.len(),
    })
}

/// Delete chunks blocking.
pub fn delete_chunks_blocking<S>(
    world: &BedrockWorld<S>,
    bounds: SlimeChunkBounds,
    guard: &WriteGuard,
) -> Result<usize>
where
    S: WorldStorageHandle,
{
    bounds.validate()?;
    guard.validate(world)?;
    let mut deleted = 0usize;
    let mut transaction = world.transaction();
    for chunk_z in bounds.min_chunk_z..=bounds.max_chunk_z {
        for chunk_x in bounds.min_chunk_x..=bounds.max_chunk_x {
            let pos = ChunkPos {
                x: chunk_x,
                z: chunk_z,
                dimension: bounds.dimension,
            };
            for record in world.get_chunk_blocking(pos)?.records {
                transaction.delete_raw_record(&record.key);
                deleted = deleted.saturating_add(1);
            }
        }
    }
    transaction.commit()?;
    Ok(deleted)
}

/// Write chunk record nbt blocking.
pub fn write_chunk_record_nbt_blocking<S>(
    world: &BedrockWorld<S>,
    pos: ChunkPos,
    record_kind: ChunkRecordTag,
    tag: &NbtTag,
    guard: &WriteGuard,
) -> Result<()>
where
    S: WorldStorageHandle,
{
    guard.validate(world)?;
    if !record_tag_accepts_nbt_write(record_kind) {
        return Err(BedrockWorldError::Validation(format!(
            "chunk record {record_kind:?} does not support NBT writes"
        )));
    }
    let bytes = serialize_record_nbt(tag)?;
    world.put_raw_record_blocking(&crate::ChunkKey::new(pos, record_kind), &bytes)
}

fn push_entities(
    target: &mut Vec<EntityOverlay>,
    entities: Vec<ParsedEntity>,
    fallback_chunk: ChunkPos,
    limit: usize,
) {
    for entity in entities {
        if target.len() >= limit {
            break;
        }
        let Some(position) = entity.position else {
            continue;
        };
        target.push(EntityOverlay {
            identifier: entity.identifier,
            chunk: BlockPos {
                x: position[0].floor() as i32,
                y: position[1].floor() as i32,
                z: position[2].floor() as i32,
            }
            .to_chunk_pos(fallback_chunk.dimension),
            position,
            nbt: entity.nbt,
        });
    }
}

fn push_block_entities(
    target: &mut Vec<BlockEntityOverlay>,
    block_entities: Vec<ParsedBlockEntity>,
    fallback_chunk: ChunkPos,
    limit: usize,
) {
    for block_entity in block_entities {
        if target.len() >= limit {
            break;
        }
        let Some(position) = block_entity.position else {
            continue;
        };
        target.push(BlockEntityOverlay {
            id: block_entity.id,
            chunk: BlockPos {
                x: position[0],
                y: position[1],
                z: position[2],
            }
            .to_chunk_pos(fallback_chunk.dimension),
            position,
            nbt: block_entity.nbt,
        });
    }
}

fn parse_record_roots(tag: ChunkRecordTag, value: &[u8]) -> Vec<NbtTag> {
    match tag {
        ChunkRecordTag::BlockEntity | ChunkRecordTag::Entity | ChunkRecordTag::PendingTicks => {
            crate::nbt::parse_consecutive_root_nbt(value).unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn record_tag_accepts_nbt_write(tag: ChunkRecordTag) -> bool {
    matches!(
        tag,
        ChunkRecordTag::BlockEntity | ChunkRecordTag::Entity | ChunkRecordTag::PendingTicks
    )
}

fn serialize_record_nbt(tag: &NbtTag) -> Result<Vec<u8>> {
    match tag {
        NbtTag::List(values) => {
            let mut bytes = Vec::new();
            for value in values {
                bytes.extend(serialize_root_nbt(value)?);
            }
            Ok(bytes)
        }
        _ => serialize_root_nbt(tag),
    }
}

fn village_overlay(village: ParsedVillageData) -> VillageOverlay {
    let bounds = infer_village_bounds(&village.roots);
    VillageOverlay {
        key: village.key,
        bounds,
        root_count: village.roots.len(),
        raw_len: village.raw.len(),
    }
}

fn infer_village_bounds(roots: &[NbtTag]) -> Option<SlimeChunkBounds> {
    for root in roots {
        if let Some(bounds) = infer_bounds_from_tag(root) {
            return Some(bounds);
        }
    }
    None
}

fn infer_bounds_from_tag(tag: &NbtTag) -> Option<SlimeChunkBounds> {
    let NbtTag::Compound(map) = tag else {
        return None;
    };
    let min_x = nbt_i32_named(map, &["min_x", "MinX", "x0", "X0", "minBlockX"])?;
    let min_z = nbt_i32_named(map, &["min_z", "MinZ", "z0", "Z0", "minBlockZ"])?;
    let max_x = nbt_i32_named(map, &["max_x", "MaxX", "x1", "X1", "maxBlockX"])?;
    let max_z = nbt_i32_named(map, &["max_z", "MaxZ", "z1", "Z1", "maxBlockZ"])?;
    Some(SlimeChunkBounds {
        dimension: Dimension::Overworld,
        min_chunk_x: min_x.div_euclid(16),
        max_chunk_x: max_x.div_euclid(16),
        min_chunk_z: min_z.div_euclid(16),
        max_chunk_z: max_z.div_euclid(16),
    })
}

fn nbt_i32_named(map: &indexmap::IndexMap<String, NbtTag>, names: &[&str]) -> Option<i32> {
    for name in names {
        if let Some(value) = map.get(*name).and_then(nbt_i32) {
            return Some(value);
        }
    }
    None
}

fn nbt_i32(tag: &NbtTag) -> Option<i32> {
    match tag {
        NbtTag::Byte(value) => Some(i32::from(*value)),
        NbtTag::Short(value) => Some(i32::from(*value)),
        NbtTag::Int(value) => Some(*value),
        NbtTag::Long(value) => i32::try_from(*value).ok(),
        _ => None,
    }
}

fn bounds_intersect(left: SlimeChunkBounds, right: SlimeChunkBounds) -> bool {
    left.dimension == right.dimension
        && left.min_chunk_x <= right.max_chunk_x
        && left.max_chunk_x >= right.min_chunk_x
        && left.min_chunk_z <= right.max_chunk_z
        && left.max_chunk_z >= right.min_chunk_z
}

fn mt19937_first_u32(seed: u32) -> u32 {
    let mut mt = [0_u32; MT_N];
    mt[0] = seed;
    for i in 1..MT_N {
        mt[i] = 1_812_433_253_u32
            .wrapping_mul(mt[i - 1] ^ (mt[i - 1] >> 30))
            .wrapping_add(i as u32);
    }
    for i in 0..MT_N {
        let y = (mt[i] & MT_UPPER_MASK) | (mt[(i + 1) % MT_N] & MT_LOWER_MASK);
        mt[i] = mt[(i + MT_M) % MT_N] ^ (y >> 1) ^ if y & 1 == 0 { 0 } else { MT_MATRIX_A };
    }
    temper(mt[0])
}

const fn temper(mut value: u32) -> u32 {
    value ^= value >> 11;
    value ^= (value << 7) & 0x9d2c_5680;
    value ^= (value << 15) & 0xefc6_0000;
    value ^= value >> 18;
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStorage, OpenOptions};
    use std::sync::Arc;

    #[test]
    fn bedrock_slime_vectors_match_known_pe_results() {
        assert!(is_bedrock_slime_chunk(-1, 0));
        assert!(is_bedrock_slime_chunk(109, 3));
        assert!(!is_bedrock_slime_chunk(0, 0));
        assert!(!is_bedrock_slime_chunk(110, 3));
    }

    #[test]
    fn slime_window_query_matches_naive_count() {
        let bounds = SlimeChunkBounds {
            dimension: Dimension::Overworld,
            min_chunk_x: -4,
            max_chunk_x: 4,
            min_chunk_z: -4,
            max_chunk_z: 4,
        };
        let windows =
            query_slime_chunk_windows(bounds, SlimeWindowSize::new(3).unwrap(), 100).unwrap();
        for window in windows {
            let mut count = 0;
            for z in window.min_chunk_z..=window.max_chunk_z {
                for x in window.min_chunk_x..=window.max_chunk_x {
                    count += usize::from(is_bedrock_slime_chunk(x, z));
                }
            }
            assert_eq!(window.slime_count, count);
        }
    }

    #[test]
    fn slime_window_query_prefix_sum_keeps_stable_sorting_for_negative_bounds() {
        let bounds = SlimeChunkBounds {
            dimension: Dimension::Overworld,
            min_chunk_x: -12,
            max_chunk_x: 7,
            min_chunk_z: -9,
            max_chunk_z: 8,
        };
        let windows =
            query_slime_chunk_windows(bounds, SlimeWindowSize::new(5).unwrap(), 24).unwrap();
        let mut expected = Vec::new();
        let center = bounds.center();
        for min_z in bounds.min_chunk_z..=bounds.max_chunk_z - 4 {
            for min_x in bounds.min_chunk_x..=bounds.max_chunk_x - 4 {
                let max_x = min_x + 4;
                let max_z = min_z + 4;
                let mut count = 0usize;
                for z in min_z..=max_z {
                    for x in min_x..=max_x {
                        count += usize::from(is_bedrock_slime_chunk(x, z));
                    }
                }
                expected.push(SlimeChunkWindow {
                    center: ChunkPos {
                        x: i32::midpoint(min_x, max_x),
                        z: i32::midpoint(min_z, max_z),
                        dimension: Dimension::Overworld,
                    },
                    min_chunk_x: min_x,
                    max_chunk_x: max_x,
                    min_chunk_z: min_z,
                    max_chunk_z: max_z,
                    slime_count: count,
                    total_count: 25,
                });
            }
        }
        expected.sort_by_key(|window| {
            let dx = i64::from(window.center.x) - i64::from(center.0);
            let dz = i64::from(window.center.z) - i64::from(center.1);
            (
                Reverse(window.slime_count),
                dx.saturating_mul(dx).saturating_add(dz.saturating_mul(dz)),
                window.center.z,
                window.center.x,
            )
        });
        expected.truncate(24);

        assert_eq!(windows, expected);
    }

    #[test]
    fn overlay_query_respects_cancel_before_scanning_chunks() {
        let storage = Arc::new(MemoryStorage::default()) as Arc<dyn crate::WorldStorage>;
        let world = BedrockWorld::from_storage(
            std::path::PathBuf::from("cancelled"),
            storage,
            OpenOptions::default(),
        );
        let cancel = CancelFlag::new();
        cancel.cancel();
        let bounds = SlimeChunkBounds {
            dimension: Dimension::Overworld,
            min_chunk_x: -128,
            max_chunk_x: 128,
            min_chunk_z: -128,
            max_chunk_z: 128,
        };
        let error = query_region_overlays_blocking_with_control(
            &world,
            bounds,
            RegionOverlayQueryOptions {
                max_chunks: 100_000,
                ..RegionOverlayQueryOptions::default()
            },
            &cancel,
        )
        .expect_err("cancelled query should fail");

        assert_eq!(error.kind(), crate::BedrockWorldErrorKind::Cancelled);
    }

    #[test]
    fn invalid_slime_window_size_is_rejected() {
        assert!(SlimeWindowSize::new(0).is_err());
        assert!(SlimeWindowSize::new(4).is_err());
        assert!(SlimeWindowSize::new(5).is_ok());
    }

    #[test]
    fn read_only_write_guard_still_rejects_mutation() {
        let world = BedrockWorld::from_storage(
            "memory",
            Arc::new(MemoryStorage::new()),
            OpenOptions::default(),
        );
        let guard = WriteGuard::confirmed("memory", "test write");
        let error = write_chunk_record_nbt_blocking(
            &world,
            ChunkPos {
                x: 0,
                z: 0,
                dimension: Dimension::Overworld,
            },
            ChunkRecordTag::BlockEntity,
            &NbtTag::Compound(indexmap::IndexMap::new()),
            &guard,
        )
        .expect_err("read-only world rejects writes");
        assert_eq!(error.kind(), crate::BedrockWorldErrorKind::ReadOnly);
    }
}
