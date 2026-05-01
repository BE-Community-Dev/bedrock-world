//! Structured parsers layered above raw Bedrock LevelDB records.
//!
//! The parser family in this module is designed for inspection and tooling:
//! callers can choose summary-only scans, structured parsed entries, or raw
//! retention for offline debugging. Parse failures are accumulated in
//! [`WorldParseReport`] where possible so a single unknown record does not stop
//! a full-world scan.

use crate::chunk::{
    BedrockDbKey, ChunkPos, ChunkRecord, ChunkRecordTag, ChunkVersion, LegacyTerrain,
    ParsedVillageKey, SubChunk, SubChunkDecodeMode, SubChunkFormat, parse_subchunk_with_mode,
};
use crate::error::Result as WorldResult;
use crate::level_dat::LevelDatDocument;
use crate::nbt::{NbtTag, parse_consecutive_root_nbt, parse_root_nbt};
use crate::storage::{StorageReadOptions, StorageVisitorControl, WorldStorage};
use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const MAX_BIOME_PALETTE_LEN: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
/// Parsed view of a world scan.
pub struct ParsedWorld {
    /// Parsed `level.dat` document read before scanning storage.
    pub level_dat: LevelDatDocument,
    /// Parsed database entries retained according to [`RetentionMode`].
    pub entries: Vec<ParsedDbEntry>,
    /// Aggregate counters, warnings, and parse errors.
    pub report: WorldParseReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Options controlling scan breadth and retained output.
pub struct WorldParseOptions {
    /// Database record categories to parse.
    pub categories: WorldParseCategories,
    /// Raw/structured retention strategy.
    pub retention: RetentionMode,
    /// Subchunk decode strategy.
    pub subchunk_decode_mode: SubChunkDecodeMode,
    /// Actor lookup strategy for `digp` records.
    pub actor_resolution: ActorResolution,
}

impl WorldParseOptions {
    #[must_use]
    pub const fn summary() -> Self {
        Self {
            categories: WorldParseCategories::all(),
            retention: RetentionMode::Summary,
            subchunk_decode_mode: SubChunkDecodeMode::CountsOnly,
            actor_resolution: ActorResolution::ResolveReferenced,
        }
    }

    #[must_use]
    pub const fn structured() -> Self {
        Self {
            categories: WorldParseCategories::all(),
            retention: RetentionMode::Structured,
            subchunk_decode_mode: SubChunkDecodeMode::CountsOnly,
            actor_resolution: ActorResolution::ResolveReferenced,
        }
    }

    #[must_use]
    pub const fn full_raw() -> Self {
        Self {
            categories: WorldParseCategories::all(),
            retention: RetentionMode::FullRaw,
            subchunk_decode_mode: SubChunkDecodeMode::FullIndices,
            actor_resolution: ActorResolution::ResolveAll,
        }
    }

    #[must_use]
    pub const fn full() -> Self {
        Self::full_raw()
    }
}

impl Default for WorldParseOptions {
    fn default() -> Self {
        Self::summary()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Category toggles for world-level parsing.
pub struct WorldParseCategories {
    /// Parse chunk records.
    pub chunks: bool,
    /// Parse player records.
    pub players: bool,
    /// Parse actor and actor digest records.
    pub actors: bool,
    /// Parse map records.
    pub maps: bool,
    /// Parse village records.
    pub villages: bool,
    /// Parse known global records.
    pub globals: bool,
}

impl WorldParseCategories {
    #[must_use]
    pub const fn all() -> Self {
        Self {
            chunks: true,
            players: true,
            actors: true,
            maps: true,
            villages: true,
            globals: true,
        }
    }

    #[must_use]
    pub const fn keys_only() -> Self {
        Self {
            chunks: false,
            players: false,
            actors: false,
            maps: false,
            villages: false,
            globals: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Controls how much raw data is kept in parse output.
pub enum RetentionMode {
    /// Keep counts and high-level summaries only.
    Summary,
    /// Keep structured parsed entries without raw values.
    Structured,
    /// Keep structured entries and raw values.
    FullRaw,
}

impl RetentionMode {
    #[must_use]
    pub const fn retains_entries(self) -> bool {
        matches!(self, Self::Structured | Self::FullRaw)
    }

    #[must_use]
    pub const fn retains_raw(self) -> bool {
        matches!(self, Self::FullRaw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Actor resolution strategy for `digp` actor digest records.
pub enum ActorResolution {
    /// Do not follow actor references.
    None,
    /// Keep actor digest ids without resolving actorprefix values.
    DigestOnly,
    /// Resolve actors referenced by digest records.
    ResolveReferenced,
    /// Resolve all actorprefix records encountered by a scan.
    ResolveAll,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
/// Aggregate counters and non-fatal diagnostics produced by parsers.
pub struct WorldParseReport {
    pub entry_count: usize,
    pub chunk_count: usize,
    pub subchunk_count: usize,
    pub legacy_subchunk_count: usize,
    pub legacy_terrain_count: usize,
    pub subchunk_storage_count: usize,
    pub palette_state_count: usize,
    pub entity_count: usize,
    pub block_entity_count: usize,
    pub item_count: usize,
    pub player_count: usize,
    pub other_nbt_root_count: usize,
    pub raw_entry_count: usize,
    pub actor_digest_count: usize,
    pub actor_digest_hit_count: usize,
    pub actor_digest_missing_count: usize,
    pub biome_record_count: usize,
    pub biome_layer_count: usize,
    pub hardcoded_spawn_area_count: usize,
    pub village_record_count: usize,
    pub map_record_count: usize,
    pub global_record_count: usize,
    pub key_kinds: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
    pub parse_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedDbEntry {
    pub key: BedrockDbKey,
    pub raw_key: Bytes,
    pub raw_value_len: usize,
    pub value: ParsedDbValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedChunkData {
    pub pos: ChunkPos,
    pub records: Vec<ParsedChunkRecord>,
    pub report: WorldParseReport,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedDbValue {
    Chunk(ParsedChunkRecord),
    Player(ParsedPlayer),
    ActorEntities(Vec<ParsedEntity>),
    ActorDigest(ParsedActorDigest),
    MapData(ParsedMapData),
    VillageData(ParsedVillageData),
    GlobalData(ParsedGlobalData),
    NbtRoots(Vec<NbtTag>),
    Raw(Bytes),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedChunkRecord {
    pub key: crate::ChunkKey,
    pub value: ParsedChunkRecordValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedChunkRecordValue {
    SubChunk(SubChunk),
    LegacyTerrain(LegacyTerrain),
    Entities(Vec<ParsedEntity>),
    BlockEntities(Vec<ParsedBlockEntity>),
    PendingTicks(Vec<NbtTag>),
    Version(u8),
    FinalizedState(i32),
    BiomeData(ParsedBiomeData),
    HardcodedSpawnAreas(Vec<ParsedHardcodedSpawnArea>),
    Raw(Bytes),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedActorDigest {
    pub pos: crate::ChunkPos,
    pub actor_ids: Vec<i64>,
    pub entities: Vec<ParsedEntity>,
    pub missing_actor_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBiomeData {
    pub version: ChunkVersion,
    pub height_map: Vec<i16>,
    pub storages: Vec<ParsedBiomeStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBiomeStorage {
    pub y: Option<i32>,
    pub palette: Vec<u32>,
    pub indices: Option<Vec<u16>>,
    pub counts: Vec<u16>,
}

impl ParsedBiomeStorage {
    #[must_use]
    pub fn palette_index_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u16> {
        if local_x >= 16 || local_y >= 16 || local_z >= 16 {
            return None;
        }
        let index = if self.y.is_some() {
            crate::block_storage_index(local_x, local_y, local_z)
        } else {
            usize::from(local_z) * 16 + usize::from(local_x)
        };
        self.indices.as_ref()?.get(index).copied()
    }

    #[must_use]
    pub fn biome_id_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u32> {
        let palette_index = usize::from(self.palette_index_at(local_x, local_y, local_z)?);
        self.palette.get(palette_index).copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedHardcodedSpawnArea {
    pub kind: HardcodedSpawnAreaKind,
    pub min: [i32; 3],
    pub max: [i32; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardcodedSpawnAreaKind {
    NetherFortress,
    SwampHut,
    OceanMonument,
    PillagerOutpost,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedMapData {
    pub id: String,
    pub roots: Vec<NbtTag>,
    pub raw: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedVillageData {
    pub key: ParsedVillageKey,
    pub roots: Vec<NbtTag>,
    pub raw: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedGlobalData {
    pub name: String,
    pub roots: Vec<NbtTag>,
    pub raw: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPlayer {
    pub key: BedrockDbKey,
    pub unique_id: Option<i64>,
    pub position: Option<[f64; 3]>,
    pub dimension_id: Option<i32>,
    pub items: Vec<ItemStack>,
    pub nbt: NbtTag,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedEntity {
    pub identifier: Option<String>,
    pub definitions: Vec<String>,
    pub unique_id: Option<i64>,
    pub position: Option<[f64; 3]>,
    pub rotation: Option<[f32; 2]>,
    pub motion: Option<[f32; 3]>,
    pub items: Vec<ItemStack>,
    pub nbt: NbtTag,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedBlockEntity {
    pub id: Option<String>,
    pub position: Option<[i32; 3]>,
    pub is_movable: Option<bool>,
    pub custom_name: Option<String>,
    pub items: Vec<ItemStack>,
    pub nbt: NbtTag,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemStack {
    pub name: Option<String>,
    pub count: Option<i32>,
    pub damage: Option<i32>,
    pub was_picked_up: Option<bool>,
    pub has_block: bool,
    pub has_tag: bool,
    pub nbt: NbtTag,
}

pub fn parse_world_storage(
    level_dat: LevelDatDocument,
    storage: &dyn WorldStorage,
    options: WorldParseOptions,
) -> WorldResult<ParsedWorld> {
    let actor_records = load_actor_records(storage, options)?;

    let mut report = WorldParseReport::default();
    let mut chunk_positions = BTreeSet::new();
    let mut parsed_entries = Vec::new();

    storage.for_each_entry(StorageReadOptions::default(), &mut |raw_key, raw_value| {
        report.entry_count += 1;
        let key = BedrockDbKey::decode(raw_key);
        *report.key_kinds.entry(key.summary_kind()).or_default() += 1;
        if let BedrockDbKey::Chunk(chunk_key) = &key {
            chunk_positions.insert(format!(
                "{}:{}:{}",
                chunk_key.pos.x,
                chunk_key.pos.z,
                chunk_key.pos.dimension.id()
            ));
        }
        if options.retention.retains_entries() && should_parse_key(&key, options.categories) {
            let value = parse_entry_value(&key, raw_value, &actor_records, &mut report, options);
            parsed_entries.push(ParsedDbEntry {
                key,
                raw_key: Bytes::copy_from_slice(raw_key),
                raw_value_len: raw_value.len(),
                value,
            });
        }
        Ok(StorageVisitorControl::Continue)
    })?;

    report.chunk_count = chunk_positions.len();
    Ok(ParsedWorld {
        level_dat,
        entries: parsed_entries,
        report,
    })
}

#[must_use]
pub fn parse_chunk_records(pos: ChunkPos, records: Vec<ChunkRecord>) -> ParsedChunkData {
    parse_chunk_records_with_options(pos, records, WorldParseOptions::full())
}

#[must_use]
pub fn parse_chunk_records_with_options(
    pos: ChunkPos,
    records: Vec<ChunkRecord>,
    options: WorldParseOptions,
) -> ParsedChunkData {
    let mut report = WorldParseReport::default();
    let parsed_records = records
        .into_iter()
        .map(|record| {
            *report
                .key_kinds
                .entry(format!("Chunk::{:?}", record.key.tag))
                .or_default() += 1;
            ParsedChunkRecord {
                key: record.key.clone(),
                value: parse_chunk_record_value(&record.key, &record.value, &mut report, options),
            }
        })
        .collect::<Vec<_>>();
    report.entry_count = parsed_records.len();
    report.chunk_count = usize::from(!parsed_records.is_empty());
    ParsedChunkData {
        pos,
        records: parsed_records,
        report,
    }
}

pub fn parse_global_storage_entries(
    storage: &dyn WorldStorage,
    options: WorldParseOptions,
) -> WorldResult<Vec<ParsedDbEntry>> {
    let actor_records = HashMap::new();
    let mut report = WorldParseReport::default();
    let mut entries = Vec::new();
    storage.for_each_entry(StorageReadOptions::default(), &mut |raw_key, raw_value| {
        let key = BedrockDbKey::decode(raw_key);
        if matches!(
            key,
            BedrockDbKey::Chunk(_)
                | BedrockDbKey::ActorPrefix { .. }
                | BedrockDbKey::ActorDigest { .. }
        ) {
            return Ok(StorageVisitorControl::Continue);
        }
        let value = parse_entry_value(&key, raw_value, &actor_records, &mut report, options);
        entries.push(ParsedDbEntry {
            key,
            raw_key: Bytes::copy_from_slice(raw_key),
            raw_value_len: raw_value.len(),
            value,
        });
        Ok(StorageVisitorControl::Continue)
    })?;
    Ok(entries)
}

fn load_actor_records(
    storage: &dyn WorldStorage,
    options: WorldParseOptions,
) -> WorldResult<HashMap<i64, Bytes>> {
    match options.actor_resolution {
        ActorResolution::None | ActorResolution::DigestOnly => Ok(HashMap::new()),
        ActorResolution::ResolveAll => {
            let mut actor_records = HashMap::new();
            storage.for_each_entry(StorageReadOptions::default(), &mut |key, value| {
                if let BedrockDbKey::ActorPrefix { actor_id } = BedrockDbKey::decode(key) {
                    actor_records.insert(actor_id, value.clone());
                }
                Ok(StorageVisitorControl::Continue)
            })?;
            Ok(actor_records)
        }
        ActorResolution::ResolveReferenced => {
            let mut actor_ids = BTreeSet::new();
            storage.for_each_entry(StorageReadOptions::default(), &mut |key, value| {
                if matches!(BedrockDbKey::decode(key), BedrockDbKey::ActorDigest { .. }) {
                    for actor_id_bytes in value.chunks_exact(8) {
                        let mut actor_id_array = [0_u8; 8];
                        actor_id_array.copy_from_slice(actor_id_bytes);
                        actor_ids.insert(i64::from_le_bytes(actor_id_array));
                    }
                }
                Ok(StorageVisitorControl::Continue)
            })?;
            let mut actor_records = HashMap::new();
            for actor_id in actor_ids {
                if let Some(value) = storage.get(&actor_prefix_key(actor_id))? {
                    actor_records.insert(actor_id, value);
                }
            }
            Ok(actor_records)
        }
    }
}

fn actor_prefix_key(actor_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity("actorprefix".len() + 8);
    key.extend_from_slice(b"actorprefix");
    key.extend_from_slice(&actor_id.to_le_bytes());
    key
}

fn should_parse_key(key: &BedrockDbKey, categories: WorldParseCategories) -> bool {
    match key {
        BedrockDbKey::Chunk(_) => categories.chunks,
        BedrockDbKey::LocalPlayer | BedrockDbKey::RemotePlayer(_) => categories.players,
        BedrockDbKey::ActorPrefix { .. } | BedrockDbKey::ActorDigest { .. } => categories.actors,
        BedrockDbKey::Map(_) => categories.maps,
        BedrockDbKey::Village(_) => categories.villages,
        BedrockDbKey::PlainString(name) if should_try_nbt_plain_key(name) => categories.globals,
        _ => false,
    }
}

fn parse_entry_value(
    key: &BedrockDbKey,
    value: &Bytes,
    actor_records: &HashMap<i64, Bytes>,
    report: &mut WorldParseReport,
    options: WorldParseOptions,
) -> ParsedDbValue {
    match key {
        BedrockDbKey::Chunk(chunk_key) => ParsedDbValue::Chunk(ParsedChunkRecord {
            key: chunk_key.clone(),
            value: parse_chunk_record_value(chunk_key, value, report, options),
        }),
        BedrockDbKey::LocalPlayer | BedrockDbKey::RemotePlayer(_) => {
            parse_player_value(key.clone(), value, report)
        }
        BedrockDbKey::ActorPrefix { .. } => parse_actor_value(value, report),
        BedrockDbKey::ActorDigest { pos } => {
            parse_actor_digest_value(*pos, value, actor_records, report, options)
        }
        BedrockDbKey::Map(id) => parse_map_value(id, value, report),
        BedrockDbKey::Village(village) => parse_village_value(village, value, report),
        BedrockDbKey::PlainString(name) if should_try_nbt_plain_key(name) => {
            parse_global_value(name, value, report)
        }
        BedrockDbKey::GameFlatWorldLayers
        | BedrockDbKey::Portals
        | BedrockDbKey::SchedulerWt
        | BedrockDbKey::StructureTemplate(_)
        | BedrockDbKey::TickingArea(_)
        | BedrockDbKey::PlainString(_)
        | BedrockDbKey::Unknown(_) => {
            report.raw_entry_count += 1;
            raw_db_value(value, options)
        }
    }
}

fn parse_chunk_record_value(
    chunk_key: &crate::ChunkKey,
    value: &Bytes,
    report: &mut WorldParseReport,
    options: WorldParseOptions,
) -> ParsedChunkRecordValue {
    match chunk_key.tag {
        ChunkRecordTag::SubChunkPrefix => {
            match parse_subchunk_with_mode(
                chunk_key.subchunk_y.unwrap_or_default(),
                value.clone(),
                options.subchunk_decode_mode,
            ) {
                Ok(subchunk) => {
                    report.subchunk_count += 1;
                    match &subchunk.format {
                        SubChunkFormat::Paletted { storages, .. } => {
                            report.subchunk_storage_count += storages.len();
                            report.palette_state_count += storages
                                .iter()
                                .map(|storage| storage.states.len())
                                .sum::<usize>();
                        }
                        SubChunkFormat::LegacySubChunk(_) => {
                            report.legacy_subchunk_count += 1;
                            report.subchunk_storage_count += 1;
                        }
                        SubChunkFormat::LegacyTerrain
                        | SubChunkFormat::FixedArrayV1
                        | SubChunkFormat::Raw { .. } => {}
                    }
                    ParsedChunkRecordValue::SubChunk(subchunk)
                }
                Err(error) => {
                    report.warnings.push(format!(
                        "subchunk {:?} kept raw: {error}",
                        chunk_key.subchunk_y
                    ));
                    report.raw_entry_count += 1;
                    ParsedChunkRecordValue::Raw(value.clone())
                }
            }
        }
        ChunkRecordTag::BlockEntity => parse_block_entities(value, report),
        ChunkRecordTag::Entity => parse_entities_chunk_record(value, report),
        ChunkRecordTag::PendingTicks => parse_pending_ticks(value, report),
        ChunkRecordTag::Version | ChunkRecordTag::VersionOld | ChunkRecordTag::LegacyVersion => {
            value.first().copied().map_or_else(
                || ParsedChunkRecordValue::Raw(value.clone()),
                ParsedChunkRecordValue::Version,
            )
        }
        ChunkRecordTag::FinalizedState => read_i32(value).map_or_else(
            || ParsedChunkRecordValue::Raw(value.clone()),
            ParsedChunkRecordValue::FinalizedState,
        ),
        ChunkRecordTag::Data3D => parse_biome_data(value, ChunkVersion::New, report),
        ChunkRecordTag::Data2D | ChunkRecordTag::Data2DLegacy => {
            parse_biome_data(value, ChunkVersion::Old, report)
        }
        ChunkRecordTag::HardcodedSpawners => parse_hardcoded_spawn_areas(value, report),
        ChunkRecordTag::LegacyTerrain => parse_legacy_terrain(value, report),
        ChunkRecordTag::BlockExtraData
        | ChunkRecordTag::BiomeState
        | ChunkRecordTag::ConversionData
        | ChunkRecordTag::BorderBlocks
        | ChunkRecordTag::RandomTicks
        | ChunkRecordTag::Checksums
        | ChunkRecordTag::GenerationSeed
        | ChunkRecordTag::MetaDataHash
        | ChunkRecordTag::GeneratedPreCavesAndCliffsBlending
        | ChunkRecordTag::BlendingBiomeHeight
        | ChunkRecordTag::BlendingData
        | ChunkRecordTag::ActorDigestVersion
        | ChunkRecordTag::Unknown(_) => {
            report.raw_entry_count += 1;
            raw_chunk_value(value, options)
        }
    }
}

fn parse_legacy_terrain(value: &Bytes, report: &mut WorldParseReport) -> ParsedChunkRecordValue {
    match LegacyTerrain::parse(value.clone()) {
        Ok(terrain) => {
            report.legacy_terrain_count += 1;
            ParsedChunkRecordValue::LegacyTerrain(terrain)
        }
        Err(error) => {
            report
                .warnings
                .push(format!("LegacyTerrain kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

fn parse_actor_digest_value(
    pos: crate::ChunkPos,
    value: &Bytes,
    actor_records: &HashMap<i64, Bytes>,
    report: &mut WorldParseReport,
    options: WorldParseOptions,
) -> ParsedDbValue {
    report.actor_digest_count += 1;
    if !value.len().is_multiple_of(8) {
        report
            .warnings
            .push(format!("actor digest for {pos:?} kept raw: invalid length"));
        report.raw_entry_count += 1;
        return raw_db_value(value, options);
    }
    let mut actor_ids = Vec::with_capacity(value.len() / 8);
    let mut entities = Vec::new();
    let mut missing_actor_count = 0;
    for actor_id_bytes in value.chunks_exact(8) {
        let mut actor_id_array = [0_u8; 8];
        actor_id_array.copy_from_slice(actor_id_bytes);
        let actor_id = i64::from_le_bytes(actor_id_array);
        actor_ids.push(actor_id);
        let Some(actor_value) = actor_records.get(&actor_id) else {
            missing_actor_count += 1;
            continue;
        };
        report.actor_digest_hit_count += 1;
        match parse_actor_value(actor_value, report) {
            ParsedDbValue::ActorEntities(mut parsed_entities) => {
                entities.append(&mut parsed_entities);
            }
            ParsedDbValue::Raw(_)
            | ParsedDbValue::Chunk(_)
            | ParsedDbValue::Player(_)
            | ParsedDbValue::ActorDigest(_)
            | ParsedDbValue::MapData(_)
            | ParsedDbValue::VillageData(_)
            | ParsedDbValue::GlobalData(_)
            | ParsedDbValue::NbtRoots(_) => {}
        }
    }
    report.actor_digest_missing_count += missing_actor_count;
    ParsedDbValue::ActorDigest(ParsedActorDigest {
        pos,
        actor_ids,
        entities,
        missing_actor_count,
    })
}

fn raw_db_value(value: &Bytes, options: WorldParseOptions) -> ParsedDbValue {
    if options.retention.retains_raw() {
        ParsedDbValue::Raw(value.clone())
    } else {
        ParsedDbValue::Raw(Bytes::new())
    }
}

fn raw_chunk_value(value: &Bytes, options: WorldParseOptions) -> ParsedChunkRecordValue {
    if options.retention.retains_raw() {
        ParsedChunkRecordValue::Raw(value.clone())
    } else {
        ParsedChunkRecordValue::Raw(Bytes::new())
    }
}

fn parse_map_value(id: &str, value: &Bytes, report: &mut WorldParseReport) -> ParsedDbValue {
    report.map_record_count += 1;
    let roots = parse_consecutive_root_nbt(value).unwrap_or_else(|error| {
        report.warnings.push(format!("map_{id} kept raw: {error}"));
        Vec::new()
    });
    ParsedDbValue::MapData(ParsedMapData {
        id: id.to_string(),
        roots,
        raw: value.clone(),
    })
}

fn parse_village_value(
    key: &ParsedVillageKey,
    value: &Bytes,
    report: &mut WorldParseReport,
) -> ParsedDbValue {
    report.village_record_count += 1;
    let roots = parse_consecutive_root_nbt(value).unwrap_or_else(|error| {
        report
            .warnings
            .push(format!("{} kept raw: {error}", key.raw));
        Vec::new()
    });
    ParsedDbValue::VillageData(ParsedVillageData {
        key: key.clone(),
        roots,
        raw: value.clone(),
    })
}

fn parse_global_value(name: &str, value: &Bytes, report: &mut WorldParseReport) -> ParsedDbValue {
    report.global_record_count += 1;
    match parse_consecutive_root_nbt(value) {
        Ok(tags) => {
            report.other_nbt_root_count += tags.len();
            ParsedDbValue::GlobalData(ParsedGlobalData {
                name: name.to_string(),
                roots: tags,
                raw: value.clone(),
            })
        }
        Err(error) => {
            report.warnings.push(format!("{name} kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedDbValue::Raw(value.clone())
        }
    }
}

fn parse_player_value(
    key: BedrockDbKey,
    value: &Bytes,
    report: &mut WorldParseReport,
) -> ParsedDbValue {
    match parse_root_nbt(value) {
        Ok(nbt) => {
            let items = collect_item_stacks(&nbt);
            report.player_count += 1;
            report.item_count += items.len();
            let root = compound(&nbt);
            ParsedDbValue::Player(ParsedPlayer {
                key,
                unique_id: root.and_then(|root| long_field(root, "UniqueID")),
                position: root.and_then(|root| vec3_f64_field(root, "Pos")),
                dimension_id: root.and_then(|root| int_field(root, "DimensionId")),
                items,
                nbt,
            })
        }
        Err(error) => {
            report
                .parse_errors
                .push(format!("player NBT parse failed: {error}"));
            report.raw_entry_count += 1;
            ParsedDbValue::Raw(value.clone())
        }
    }
}

pub(crate) fn parse_actor_value(value: &Bytes, report: &mut WorldParseReport) -> ParsedDbValue {
    match parse_consecutive_root_nbt(value) {
        Ok(tags) => {
            let entities = tags
                .into_iter()
                .map(|tag| parse_entity_from_nbt(tag, report))
                .collect::<Vec<_>>();
            report.entity_count += entities.len();
            ParsedDbValue::ActorEntities(entities)
        }
        Err(error) => {
            report
                .warnings
                .push(format!("actorprefix kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedDbValue::Raw(value.clone())
        }
    }
}

fn parse_biome_data(
    value: &Bytes,
    version: ChunkVersion,
    report: &mut WorldParseReport,
) -> ParsedChunkRecordValue {
    let result = match version {
        ChunkVersion::Old => parse_legacy_data2d(value),
        ChunkVersion::New => parse_data3d(value),
    };
    match result {
        Ok(data) => {
            report.biome_record_count += 1;
            report.biome_layer_count += data.storages.len();
            ParsedChunkRecordValue::BiomeData(data)
        }
        Err(error) => {
            report
                .warnings
                .push(format!("biome data kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

pub(crate) fn parse_legacy_data2d(value: &[u8]) -> Result<ParsedBiomeData, String> {
    if value.len() < 768 {
        return Err(format!("Data2D is too short: {}", value.len()));
    }
    let height_map = read_height_map(&value[..512])?;
    let indices = value[512..768]
        .iter()
        .map(|value| u16::from(*value))
        .collect::<Vec<_>>();
    let palette = (0..=255).collect::<Vec<_>>();
    let mut counts = vec![0_u16; palette.len()];
    for index in &indices {
        if let Some(count) = counts.get_mut(usize::from(*index)) {
            *count = count.saturating_add(1);
        }
    }
    Ok(ParsedBiomeData {
        version: ChunkVersion::Old,
        height_map,
        storages: vec![ParsedBiomeStorage {
            y: None,
            palette,
            indices: Some(indices),
            counts,
        }],
    })
}

pub(crate) fn parse_data3d(value: &[u8]) -> Result<ParsedBiomeData, String> {
    if value.len() < 512 {
        return Err(format!("Data3D is too short: {}", value.len()));
    }
    let height_map = read_height_map(&value[..512])?;
    let mut offset = 512;
    let mut storages = Vec::new();
    let mut y = -64;
    while offset < value.len() {
        let (storage, consumed) = parse_subchunk_biomes(&value[offset..], y)?;
        if consumed == 0 {
            return Err("Data3D biome parser did not advance".to_string());
        }
        offset += consumed;
        y += 16;
        storages.push(storage);
    }
    Ok(ParsedBiomeData {
        version: ChunkVersion::New,
        height_map,
        storages,
    })
}

fn parse_subchunk_biomes(
    value: &[u8],
    start_y: i32,
) -> Result<(ParsedBiomeStorage, usize), String> {
    let Some(header) = value.first().copied() else {
        return Err("missing biome storage header".to_string());
    };
    if header == 0xff {
        return Ok((
            ParsedBiomeStorage {
                y: Some(start_y),
                palette: vec![u32::MAX],
                indices: None,
                counts: vec![4096],
            },
            1,
        ));
    }
    let bits_per_biome = header >> 1;
    let mut offset = 1;
    let indices = if bits_per_biome == 0 {
        vec![0_u16; 4096]
    } else {
        let word_count = packed_word_count(bits_per_biome);
        let words_byte_len = word_count
            .checked_mul(4)
            .ok_or_else(|| "biome palette word count overflowed".to_string())?;
        let words = value
            .get(offset..offset + words_byte_len)
            .ok_or_else(|| "biome palette words are truncated".to_string())?;
        offset += words_byte_len;
        unpack_indices(words, bits_per_biome)?
    };
    let palette_len = if bits_per_biome == 0 {
        1
    } else {
        let len = read_i32_le(value, offset)?;
        offset += 4;
        usize::try_from(len).map_err(|_| format!("invalid biome palette length: {len}"))?
    };
    if palette_len > MAX_BIOME_PALETTE_LEN {
        return Err(format!(
            "biome palette length {palette_len} exceeds maximum {MAX_BIOME_PALETTE_LEN}"
        ));
    }
    let mut palette = Vec::with_capacity(palette_len);
    for _ in 0..palette_len {
        let id = read_i32_le(value, offset)?;
        offset += 4;
        palette.push(u32::try_from(id).unwrap_or(u32::MAX));
    }
    let mut counts = vec![0_u16; palette.len()];
    for index in &indices {
        if let Some(count) = counts.get_mut(usize::from(*index)) {
            *count = count.saturating_add(1);
        }
    }
    Ok((
        ParsedBiomeStorage {
            y: Some(start_y),
            palette,
            indices: Some(indices),
            counts,
        },
        offset,
    ))
}

fn read_height_map(value: &[u8]) -> Result<Vec<i16>, String> {
    if value.len() != 512 {
        return Err(format!("height map must be 512 bytes, got {}", value.len()));
    }
    Ok(value
        .chunks_exact(2)
        .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
        .collect())
}

fn packed_word_count(bits_per_value: u8) -> usize {
    if bits_per_value == 0 {
        return 0;
    }
    let values_per_word = usize::from(32 / bits_per_value);
    4096_usize.div_ceil(values_per_word)
}

fn unpack_indices(words_bytes: &[u8], bits_per_value: u8) -> Result<Vec<u16>, String> {
    if bits_per_value == 0 {
        return Ok(vec![0; 4096]);
    }
    if !matches!(bits_per_value, 1 | 2 | 3 | 4 | 5 | 6 | 8 | 16) {
        return Err(format!(
            "unsupported biome bits-per-value: {bits_per_value}"
        ));
    }
    let values_per_word = usize::from(32 / bits_per_value);
    let mask = (1_u32 << bits_per_value) - 1;
    let mut indices = Vec::with_capacity(4096);
    for word_bytes in words_bytes.chunks_exact(4) {
        let word = u32::from_le_bytes([word_bytes[0], word_bytes[1], word_bytes[2], word_bytes[3]]);
        for item_index in 0..values_per_word {
            if indices.len() == 4096 {
                break;
            }
            indices.push(((word >> (item_index * usize::from(bits_per_value))) & mask) as u16);
        }
    }
    if indices.len() != 4096 {
        return Err(format!("decoded {} biome indices", indices.len()));
    }
    Ok(indices)
}

fn read_i32_le(value: &[u8], offset: usize) -> Result<i32, String> {
    let bytes = value
        .get(offset..offset + 4)
        .ok_or_else(|| "i32 field is truncated".to_string())?;
    Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn parse_hardcoded_spawn_areas(
    value: &Bytes,
    report: &mut WorldParseReport,
) -> ParsedChunkRecordValue {
    match read_hardcoded_spawn_areas(value) {
        Ok(areas) => {
            report.hardcoded_spawn_area_count += areas.len();
            ParsedChunkRecordValue::HardcodedSpawnAreas(areas)
        }
        Err(error) => {
            report
                .warnings
                .push(format!("hardcoded spawn areas kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

fn read_hardcoded_spawn_areas(value: &[u8]) -> Result<Vec<ParsedHardcodedSpawnArea>, String> {
    let count = usize::try_from(read_i32_le(value, 0)?)
        .map_err(|_| "hardcoded spawn area count cannot be negative".to_string())?;
    let expected_len = 4 + count * 25;
    if value.len() != expected_len {
        return Err(format!(
            "expected {expected_len} bytes, got {}",
            value.len()
        ));
    }
    let mut areas = Vec::with_capacity(count);
    for index in 0..count {
        let offset = 4 + index * 25;
        areas.push(ParsedHardcodedSpawnArea {
            kind: match value[offset + 24] {
                1 => HardcodedSpawnAreaKind::NetherFortress,
                2 => HardcodedSpawnAreaKind::SwampHut,
                3 => HardcodedSpawnAreaKind::OceanMonument,
                5 => HardcodedSpawnAreaKind::PillagerOutpost,
                value => HardcodedSpawnAreaKind::Unknown(value),
            },
            min: [
                read_i32_le(value, offset)?,
                read_i32_le(value, offset + 4)?,
                read_i32_le(value, offset + 8)?,
            ],
            max: [
                read_i32_le(value, offset + 12)?,
                read_i32_le(value, offset + 16)?,
                read_i32_le(value, offset + 20)?,
            ],
        });
    }
    Ok(areas)
}

pub(crate) fn parse_block_entities(
    value: &Bytes,
    report: &mut WorldParseReport,
) -> ParsedChunkRecordValue {
    match parse_consecutive_root_nbt(value) {
        Ok(tags) => {
            let block_entities = tags
                .into_iter()
                .map(|tag| parse_block_entity_from_nbt(tag, report))
                .collect::<Vec<_>>();
            report.block_entity_count += block_entities.len();
            ParsedChunkRecordValue::BlockEntities(block_entities)
        }
        Err(error) => {
            report
                .warnings
                .push(format!("block entities kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

fn parse_entities_chunk_record(
    value: &Bytes,
    report: &mut WorldParseReport,
) -> ParsedChunkRecordValue {
    match parse_consecutive_root_nbt(value) {
        Ok(tags) => {
            let entities = tags
                .into_iter()
                .map(|tag| parse_entity_from_nbt(tag, report))
                .collect::<Vec<_>>();
            report.entity_count += entities.len();
            ParsedChunkRecordValue::Entities(entities)
        }
        Err(error) => {
            report.warnings.push(format!("entities kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

pub(crate) fn parse_entities_from_value(
    value: &Bytes,
    report: &mut WorldParseReport,
) -> Vec<ParsedEntity> {
    match parse_actor_value(value, report) {
        ParsedDbValue::ActorEntities(entities) => entities,
        _ => Vec::new(),
    }
}

pub(crate) fn parse_block_entities_from_value(
    value: &Bytes,
    report: &mut WorldParseReport,
) -> Vec<ParsedBlockEntity> {
    match parse_block_entities(value, report) {
        ParsedChunkRecordValue::BlockEntities(block_entities) => block_entities,
        _ => Vec::new(),
    }
}

fn parse_pending_ticks(value: &Bytes, report: &mut WorldParseReport) -> ParsedChunkRecordValue {
    match parse_consecutive_root_nbt(value) {
        Ok(tags) => ParsedChunkRecordValue::PendingTicks(tags),
        Err(error) => {
            report
                .warnings
                .push(format!("pending ticks kept raw: {error}"));
            report.raw_entry_count += 1;
            ParsedChunkRecordValue::Raw(value.clone())
        }
    }
}

fn parse_entity_from_nbt(nbt: NbtTag, report: &mut WorldParseReport) -> ParsedEntity {
    let items = collect_item_stacks(&nbt);
    report.item_count += items.len();
    let root = compound(&nbt);
    ParsedEntity {
        identifier: root.and_then(entity_identifier),
        definitions: root.map_or_else(Vec::new, entity_definitions),
        unique_id: root.and_then(|root| long_field(root, "UniqueID")),
        position: root.and_then(|root| vec3_f64_field(root, "Pos")),
        rotation: root.and_then(|root| vec2_f32_field(root, "Rotation")),
        motion: root.and_then(|root| vec3_f32_field(root, "Motion")),
        items,
        nbt,
    }
}

fn parse_block_entity_from_nbt(nbt: NbtTag, report: &mut WorldParseReport) -> ParsedBlockEntity {
    let items = collect_item_stacks(&nbt);
    report.item_count += items.len();
    let root = compound(&nbt);
    ParsedBlockEntity {
        id: root
            .and_then(|root| string_field(root, "id"))
            .map(ToString::to_string),
        position: root.and_then(|root| {
            Some([
                int_field(root, "x")?,
                int_field(root, "y")?,
                int_field(root, "z")?,
            ])
        }),
        is_movable: root.and_then(|root| bool_field(root, "isMovable")),
        custom_name: root
            .and_then(|root| string_field(root, "CustomName"))
            .map(ToString::to_string),
        items,
        nbt,
    }
}

pub(crate) fn collect_item_stacks(tag: &NbtTag) -> Vec<ItemStack> {
    let mut items = Vec::new();
    collect_item_stacks_inner(tag, &mut items);
    items
}

fn collect_item_stacks_inner(tag: &NbtTag, items: &mut Vec<ItemStack>) {
    match tag {
        NbtTag::Compound(root) => {
            if looks_like_item_stack(root) {
                items.push(ItemStack {
                    name: string_field(root, "Name")
                        .or_else(|| string_field(root, "name"))
                        .map(ToString::to_string),
                    count: int_field(root, "Count"),
                    damage: int_field(root, "Damage").or_else(|| int_field(root, "Aux")),
                    was_picked_up: bool_field(root, "WasPickedUp"),
                    has_block: root.contains_key("Block"),
                    has_tag: root.contains_key("tag"),
                    nbt: tag.clone(),
                });
            }
            for value in root.values() {
                collect_item_stacks_inner(value, items);
            }
        }
        NbtTag::List(values) => {
            for value in values {
                collect_item_stacks_inner(value, items);
            }
        }
        _ => {}
    }
}

fn looks_like_item_stack(root: &IndexMap<String, NbtTag>) -> bool {
    (root.contains_key("Name") || root.contains_key("name")) && root.contains_key("Count")
}

fn should_try_nbt_plain_key(name: &str) -> bool {
    matches!(
        name,
        "AutonomousEntities"
            | "BiomeData"
            | "LevelChunkMetaDataDictionary"
            | "Overworld"
            | "WorldClocks"
            | "mobevents"
            | "scoreboard"
    )
}

fn entity_identifier(root: &IndexMap<String, NbtTag>) -> Option<String> {
    string_field(root, "identifier")
        .or_else(|| string_field(root, "Identifier"))
        .or_else(|| string_field(root, "id"))
        .map(ToString::to_string)
}

fn entity_definitions(root: &IndexMap<String, NbtTag>) -> Vec<String> {
    match root.get("definitions").or_else(|| root.get("Definitions")) {
        Some(NbtTag::List(values)) => values
            .iter()
            .filter_map(|value| match value {
                NbtTag::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn compound(tag: &NbtTag) -> Option<&IndexMap<String, NbtTag>> {
    match tag {
        NbtTag::Compound(root) => Some(root),
        _ => None,
    }
}

fn string_field<'a>(root: &'a IndexMap<String, NbtTag>, key: &str) -> Option<&'a str> {
    match root.get(key) {
        Some(NbtTag::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn bool_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<bool> {
    match root.get(key) {
        Some(NbtTag::Byte(value)) => Some(*value != 0),
        Some(NbtTag::Short(value)) => Some(*value != 0),
        Some(NbtTag::Int(value)) => Some(*value != 0),
        _ => None,
    }
}

fn int_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<i32> {
    match root.get(key) {
        Some(NbtTag::Byte(value)) => Some(i32::from(*value)),
        Some(NbtTag::Short(value)) => Some(i32::from(*value)),
        Some(NbtTag::Int(value)) => Some(*value),
        Some(NbtTag::Long(value)) => i32::try_from(*value).ok(),
        _ => None,
    }
}

fn long_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<i64> {
    match root.get(key) {
        Some(NbtTag::Byte(value)) => Some(i64::from(*value)),
        Some(NbtTag::Short(value)) => Some(i64::from(*value)),
        Some(NbtTag::Int(value)) => Some(i64::from(*value)),
        Some(NbtTag::Long(value)) => Some(*value),
        _ => None,
    }
}

fn f64_value(tag: &NbtTag) -> Option<f64> {
    match tag {
        NbtTag::Float(value) => Some(f64::from(*value)),
        NbtTag::Double(value) => Some(*value),
        NbtTag::Int(value) => Some(f64::from(*value)),
        NbtTag::Long(value) => Some(*value as f64),
        _ => None,
    }
}

fn f32_value(tag: &NbtTag) -> Option<f32> {
    match tag {
        NbtTag::Float(value) => Some(*value),
        NbtTag::Double(value) => Some(*value as f32),
        NbtTag::Int(value) => Some(*value as f32),
        _ => None,
    }
}

fn vec3_f64_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<[f64; 3]> {
    let Some(NbtTag::List(values)) = root.get(key) else {
        return None;
    };
    Some([
        f64_value(values.first()?)?,
        f64_value(values.get(1)?)?,
        f64_value(values.get(2)?)?,
    ])
}

fn vec3_f32_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<[f32; 3]> {
    let Some(NbtTag::List(values)) = root.get(key) else {
        return None;
    };
    Some([
        f32_value(values.first()?)?,
        f32_value(values.get(1)?)?,
        f32_value(values.get(2)?)?,
    ])
}

fn vec2_f32_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<[f32; 2]> {
    let Some(NbtTag::List(values)) = root.get(key) else {
        return None;
    };
    Some([f32_value(values.first()?)?, f32_value(values.get(1)?)?])
}

fn read_i32(value: &[u8]) -> Option<i32> {
    let bytes: [u8; 4] = value.get(..4)?.try_into().ok()?;
    Some(i32::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbt::serialize_root_nbt;
    use crate::storage::{MemoryStorage, WorldStorage};

    #[test]
    fn item_stack_extracts_common_fields() {
        let item = NbtTag::Compound(IndexMap::from([
            (
                "Name".to_string(),
                NbtTag::String("minecraft:stone".to_string()),
            ),
            ("Count".to_string(), NbtTag::Byte(5)),
            ("Damage".to_string(), NbtTag::Short(1)),
            ("WasPickedUp".to_string(), NbtTag::Byte(1)),
        ]));

        let items = collect_item_stacks(&item);

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name.as_deref(), Some("minecraft:stone"));
        assert_eq!(items[0].count, Some(5));
        assert_eq!(items[0].damage, Some(1));
        assert_eq!(items[0].was_picked_up, Some(true));
    }

    #[test]
    fn entity_extracts_identifier_position_and_items() {
        let entity = NbtTag::Compound(IndexMap::from([
            (
                "identifier".to_string(),
                NbtTag::String("minecraft:pig".to_string()),
            ),
            (
                "Pos".to_string(),
                NbtTag::List(vec![
                    NbtTag::Float(1.0),
                    NbtTag::Float(2.0),
                    NbtTag::Float(3.0),
                ]),
            ),
            (
                "Inventory".to_string(),
                NbtTag::List(vec![NbtTag::Compound(IndexMap::from([
                    (
                        "Name".to_string(),
                        NbtTag::String("minecraft:dirt".to_string()),
                    ),
                    ("Count".to_string(), NbtTag::Byte(1)),
                ]))]),
            ),
        ]));
        let bytes = Bytes::from(serialize_root_nbt(&entity).expect("serialize"));
        let mut report = WorldParseReport::default();

        let value = parse_actor_value(&bytes, &mut report);

        let ParsedDbValue::ActorEntities(entities) = value else {
            panic!("expected entity value");
        };
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].identifier.as_deref(), Some("minecraft:pig"));
        assert_eq!(entities[0].position, Some([1.0, 2.0, 3.0]));
        assert_eq!(entities[0].items.len(), 1);
    }

    #[test]
    fn block_entity_extracts_container_items() {
        let block_entity = NbtTag::Compound(IndexMap::from([
            ("id".to_string(), NbtTag::String("Chest".to_string())),
            ("x".to_string(), NbtTag::Int(1)),
            ("y".to_string(), NbtTag::Int(2)),
            ("z".to_string(), NbtTag::Int(3)),
            ("isMovable".to_string(), NbtTag::Byte(1)),
            (
                "Items".to_string(),
                NbtTag::List(vec![NbtTag::Compound(IndexMap::from([
                    (
                        "Name".to_string(),
                        NbtTag::String("minecraft:apple".to_string()),
                    ),
                    ("Count".to_string(), NbtTag::Byte(2)),
                ]))]),
            ),
        ]));
        let bytes = Bytes::from(serialize_root_nbt(&block_entity).expect("serialize"));
        let mut report = WorldParseReport::default();

        let value = parse_block_entities(&bytes, &mut report);

        let ParsedChunkRecordValue::BlockEntities(block_entities) = value else {
            panic!("expected block entities");
        };
        assert_eq!(block_entities.len(), 1);
        assert_eq!(block_entities[0].id.as_deref(), Some("Chest"));
        assert_eq!(block_entities[0].position, Some([1, 2, 3]));
        assert_eq!(block_entities[0].items.len(), 1);
    }

    #[test]
    fn biome_lookup_uses_xz_plane_storage_order() {
        let mut indices = vec![0_u16; 4096];
        indices[crate::block_storage_index(1, 2, 3)] = 2;
        let storage = ParsedBiomeStorage {
            y: Some(0),
            palette: vec![10, 20, 30],
            indices: Some(indices),
            counts: vec![4095, 0, 1],
        };

        assert_eq!(storage.biome_id_at(1, 2, 3), Some(30));
        assert_eq!(storage.biome_id_at(1, 3, 3), Some(10));
    }

    #[test]
    fn chunk_record_parser_preserves_legacy_terrain_structure() {
        let records = vec![ChunkRecord {
            key: crate::ChunkKey::new(
                ChunkPos {
                    x: 0,
                    z: 0,
                    dimension: crate::Dimension::Overworld,
                },
                ChunkRecordTag::LegacyTerrain,
            ),
            value: Bytes::from(vec![0; crate::LEGACY_TERRAIN_VALUE_LEN]),
        }];

        let parsed = parse_chunk_records(
            ChunkPos {
                x: 0,
                z: 0,
                dimension: crate::Dimension::Overworld,
            },
            records,
        );

        assert_eq!(parsed.report.legacy_terrain_count, 1);
        assert!(matches!(
            parsed.records[0].value,
            ParsedChunkRecordValue::LegacyTerrain(_)
        ));
    }

    #[test]
    fn chunk_record_parser_counts_legacy_subchunks() {
        let mut value = vec![0; crate::LEGACY_SUBCHUNK_MIN_VALUE_LEN];
        value[0] = 2;
        let records = vec![ChunkRecord {
            key: crate::ChunkKey::subchunk(
                ChunkPos {
                    x: 0,
                    z: 0,
                    dimension: crate::Dimension::Overworld,
                },
                0,
            ),
            value: Bytes::from(value),
        }];

        let parsed = parse_chunk_records(
            ChunkPos {
                x: 0,
                z: 0,
                dimension: crate::Dimension::Overworld,
            },
            records,
        );

        assert_eq!(parsed.report.subchunk_count, 1);
        assert_eq!(parsed.report.legacy_subchunk_count, 1);
        assert!(matches!(
            parsed.records[0].value,
            ParsedChunkRecordValue::SubChunk(SubChunk {
                format: SubChunkFormat::LegacySubChunk(_),
                ..
            })
        ));
    }

    #[test]
    fn summary_parse_does_not_retain_raw_entries() {
        let storage = MemoryStorage::new();
        let chunk_key = crate::ChunkKey::new(
            ChunkPos {
                x: 0,
                z: 0,
                dimension: crate::Dimension::Overworld,
            },
            ChunkRecordTag::Version,
        );
        storage
            .put(&chunk_key.encode(), &[1])
            .expect("insert chunk version");
        storage
            .put(
                b"~local_player",
                &serialize_root_nbt(&NbtTag::Compound(IndexMap::new())).expect("serialize"),
            )
            .expect("insert player");

        let parsed = parse_world_storage(
            LevelDatDocument {
                header: crate::LevelDatHeader {
                    version: 10,
                    declared_len: 0,
                    actual_payload_len: 0,
                },
                root: NbtTag::Compound(IndexMap::new()),
                warnings: Vec::new(),
            },
            &storage,
            WorldParseOptions::summary(),
        )
        .expect("parse summary");

        assert_eq!(parsed.report.entry_count, 2);
        assert_eq!(parsed.report.chunk_count, 1);
        assert!(parsed.entries.is_empty());
    }
}
