//! Bedrock chunk keys, coordinates, and subchunk payload parsing.
//!
//! This module decodes LevelDB key shapes used by Minecraft Bedrock worlds and
//! exposes conservative parsers for modern paletted subchunks plus older
//! LevelDB-era terrain arrays. Unsupported payloads are preserved as raw bytes
//! where possible so inspection tools can keep scanning mixed-version worlds.

use crate::error::{BedrockWorldError, Result};
use crate::nbt::{NbtTag, parse_consecutive_root_nbt, parse_root_nbt_with_consumed};
use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MAX_SUBCHUNK_PALETTE_LEN: usize = 4096;
/// Number of block ID entries in an old 16x128x16 `LegacyTerrain` value.
pub const LEGACY_TERRAIN_BLOCK_COUNT: usize = 16 * 128 * 16;
/// Exact byte length of an old `LevelDB` `LegacyTerrain` value.
pub const LEGACY_TERRAIN_VALUE_LEN: usize = 83_200;
/// Number of block entries in a 16x16x16 legacy subchunk.
pub const LEGACY_SUBCHUNK_BLOCK_COUNT: usize = 16 * 16 * 16;
/// Minimum byte length of a legacy subchunk without light arrays.
pub const LEGACY_SUBCHUNK_MIN_VALUE_LEN: usize =
    1 + LEGACY_SUBCHUNK_BLOCK_COUNT + LEGACY_SUBCHUNK_BLOCK_COUNT / 2;
/// Byte length of a legacy subchunk with sky and block light arrays.
pub const LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN: usize =
    LEGACY_SUBCHUNK_MIN_VALUE_LEN + LEGACY_SUBCHUNK_BLOCK_COUNT;

const LEGACY_TERRAIN_BLOCK_DATA_OFFSET: usize = LEGACY_TERRAIN_BLOCK_COUNT;
const LEGACY_TERRAIN_SKY_LIGHT_OFFSET: usize =
    LEGACY_TERRAIN_BLOCK_DATA_OFFSET + LEGACY_TERRAIN_BLOCK_COUNT / 2;
const LEGACY_TERRAIN_BLOCK_LIGHT_OFFSET: usize =
    LEGACY_TERRAIN_SKY_LIGHT_OFFSET + LEGACY_TERRAIN_BLOCK_COUNT / 2;
const LEGACY_TERRAIN_HEIGHTMAP_OFFSET: usize =
    LEGACY_TERRAIN_BLOCK_LIGHT_OFFSET + LEGACY_TERRAIN_BLOCK_COUNT / 2;
const LEGACY_TERRAIN_BIOME_OFFSET: usize = LEGACY_TERRAIN_HEIGHTMAP_OFFSET + 16 * 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Bedrock dimension identifier.
pub enum Dimension {
    /// The overworld dimension, encoded as `0`.
    Overworld,
    /// The Nether dimension, encoded as `1`.
    Nether,
    /// The End dimension, encoded as `2`.
    End,
    /// A dimension id not recognized by this crate.
    Unknown(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Vertical build-height generation used for chunk bounds.
pub enum ChunkVersion {
    /// Pre-Caves-and-Cliffs vertical range.
    Old,
    /// Modern extended vertical range.
    New,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Absolute block position within a world.
pub struct BlockPos {
    /// Absolute X block coordinate.
    pub x: i32,
    /// Absolute Y block coordinate.
    pub y: i32,
    /// Absolute Z block coordinate.
    pub z: i32,
}

impl Dimension {
    #[must_use]
    pub const fn id(self) -> i32 {
        match self {
            Self::Overworld => 0,
            Self::Nether => 1,
            Self::End => 2,
            Self::Unknown(value) => value,
        }
    }

    #[must_use]
    pub const fn from_id(id: i32) -> Self {
        match id {
            0 => Self::Overworld,
            1 => Self::Nether,
            2 => Self::End,
            value => Self::Unknown(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Chunk position and dimension.
pub struct ChunkPos {
    /// Chunk X coordinate.
    pub x: i32,
    /// Chunk Z coordinate.
    pub z: i32,
    /// Dimension containing this chunk.
    pub dimension: Dimension,
}

impl ChunkPos {
    #[must_use]
    pub const fn y_range(self, version: ChunkVersion) -> (i32, i32) {
        match self.dimension {
            Dimension::Nether => (0, 127),
            Dimension::End => (0, 255),
            Dimension::Overworld => match version {
                ChunkVersion::Old => (0, 255),
                ChunkVersion::New => (-64, 319),
            },
            Dimension::Unknown(_) => (0, -1),
        }
    }

    #[must_use]
    pub const fn subchunk_index_range(self, version: ChunkVersion) -> (i8, i8) {
        match self.dimension {
            Dimension::Nether => (0, 7),
            Dimension::End => (0, 15),
            Dimension::Overworld => match version {
                ChunkVersion::Old => (0, 15),
                ChunkVersion::New => (-4, 19),
            },
            Dimension::Unknown(_) => (0, -1),
        }
    }

    #[must_use]
    pub const fn min_block_pos(self, version: ChunkVersion) -> BlockPos {
        let (min_y, _) = self.y_range(version);
        BlockPos {
            x: self.x * 16,
            y: min_y,
            z: self.z * 16,
        }
    }

    #[must_use]
    pub const fn max_block_pos(self, version: ChunkVersion) -> BlockPos {
        let (_, max_y) = self.y_range(version);
        BlockPos {
            x: self.x * 16 + 15,
            y: max_y,
            z: self.z * 16 + 15,
        }
    }
}

impl BlockPos {
    #[must_use]
    pub const fn to_chunk_pos(self, dimension: Dimension) -> ChunkPos {
        let x = if self.x < 0 { self.x - 15 } else { self.x } / 16;
        let z = if self.z < 0 { self.z - 15 } else { self.z } / 16;
        ChunkPos { x, z, dimension }
    }

    #[must_use]
    pub const fn in_chunk_offset(self) -> (u8, i32, u8) {
        let mut x = self.x % 16;
        let mut z = self.z % 16;
        if x < 0 {
            x += 16;
        }
        if z < 0 {
            z += 16;
        }
        (x as u8, self.y, z as u8)
    }
}

#[must_use]
/// Returns the Bedrock X-major storage index for local 16x16x16 coordinates.
pub fn block_storage_index(local_x: u8, local_y: u8, local_z: u8) -> usize {
    usize::from(local_x) * 256 + usize::from(local_z) * 16 + usize::from(local_y)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChunkRecordTag {
    Data3D,
    Data2D,
    Data2DLegacy,
    SubChunkPrefix,
    LegacyTerrain,
    BlockEntity,
    Entity,
    PendingTicks,
    BlockExtraData,
    BiomeState,
    FinalizedState,
    ConversionData,
    BorderBlocks,
    HardcodedSpawners,
    RandomTicks,
    Checksums,
    GenerationSeed,
    MetaDataHash,
    GeneratedPreCavesAndCliffsBlending,
    BlendingBiomeHeight,
    BlendingData,
    ActorDigestVersion,
    Version,
    VersionOld,
    LegacyVersion,
    Unknown(u8),
}

impl ChunkRecordTag {
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Data3D => 0x2b,
            Self::Version => 0x2c,
            Self::Data2D => 0x2d,
            Self::Data2DLegacy => 0x2e,
            Self::SubChunkPrefix => 0x2f,
            Self::LegacyTerrain => 0x30,
            Self::BlockEntity => 0x31,
            Self::Entity => 0x32,
            Self::PendingTicks => 0x33,
            Self::BlockExtraData => 0x34,
            Self::BiomeState => 0x35,
            Self::FinalizedState => 0x36,
            Self::ConversionData => 0x37,
            Self::BorderBlocks => 0x38,
            Self::HardcodedSpawners => 0x39,
            Self::RandomTicks => 0x3a,
            Self::Checksums => 0x3b,
            Self::GenerationSeed => 0x3c,
            Self::GeneratedPreCavesAndCliffsBlending => 0x3d,
            Self::BlendingBiomeHeight => 0x3e,
            Self::MetaDataHash => 0x3f,
            Self::BlendingData => 0x40,
            Self::ActorDigestVersion => 0x41,
            Self::VersionOld => 0x76,
            Self::LegacyVersion => 0x77,
            Self::Unknown(value) => value,
        }
    }

    #[must_use]
    pub const fn from_byte(value: u8) -> Self {
        match value {
            0x2b => Self::Data3D,
            0x2c => Self::Version,
            0x2d => Self::Data2D,
            0x2e => Self::Data2DLegacy,
            0x2f => Self::SubChunkPrefix,
            0x30 => Self::LegacyTerrain,
            0x31 => Self::BlockEntity,
            0x32 => Self::Entity,
            0x33 => Self::PendingTicks,
            0x34 => Self::BlockExtraData,
            0x35 => Self::BiomeState,
            0x36 => Self::FinalizedState,
            0x37 => Self::ConversionData,
            0x38 => Self::BorderBlocks,
            0x39 => Self::HardcodedSpawners,
            0x3a => Self::RandomTicks,
            0x3b => Self::Checksums,
            0x3c => Self::GenerationSeed,
            0x3d => Self::GeneratedPreCavesAndCliffsBlending,
            0x3e => Self::BlendingBiomeHeight,
            0x3f => Self::MetaDataHash,
            0x40 => Self::BlendingData,
            0x41 => Self::ActorDigestVersion,
            0x76 => Self::VersionOld,
            0x77 => Self::LegacyVersion,
            other => Self::Unknown(other),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BedrockDbKey {
    Chunk(ChunkKey),
    LocalPlayer,
    RemotePlayer(String),
    ActorPrefix { actor_id: i64 },
    ActorDigest { pos: ChunkPos },
    Map(String),
    Village(ParsedVillageKey),
    Portals,
    SchedulerWt,
    StructureTemplate(String),
    TickingArea(String),
    GameFlatWorldLayers,
    PlainString(String),
    Unknown(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VillageRecordKind {
    Info,
    Dwellers,
    Players,
    Poi,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedVillageKey {
    pub raw: String,
    pub dimension: Option<Dimension>,
    pub uuid: String,
    pub kind: VillageRecordKind,
}

impl BedrockDbKey {
    #[must_use]
    pub fn decode(key: &[u8]) -> Self {
        if key == b"~local_player" {
            return Self::LocalPlayer;
        }
        if let Some(remote_player) = key.strip_prefix(b"player_") {
            return Self::RemotePlayer(String::from_utf8_lossy(remote_player).into_owned());
        }
        if let Some(actor_id) = parse_i64_suffix(key, b"actorprefix") {
            return Self::ActorPrefix { actor_id };
        }
        if let Some(pos) = parse_chunk_pos_suffix(key, b"digp") {
            return Self::ActorDigest { pos };
        }
        if key == b"portals" {
            return Self::Portals;
        }
        if key == b"schedulerWT" {
            return Self::SchedulerWt;
        }
        if let Some(map_id) = ascii_suffix(key, b"map_") {
            return Self::Map(map_id);
        }
        if let Some(village) = parse_village_key(key) {
            return Self::Village(village);
        }
        if let Some(name) = ascii_suffix(key, b"structuretemplate") {
            return Self::StructureTemplate(name);
        }
        if let Some(name) = ascii_suffix(key, b"tickingarea") {
            return Self::TickingArea(name);
        }
        if key == b"game_flatworldlayers" {
            return Self::GameFlatWorldLayers;
        }
        if key.iter().all(u8::is_ascii_graphic) {
            return Self::PlainString(String::from_utf8_lossy(key).into_owned());
        }
        if let Ok(chunk_key) = ChunkKey::decode(key) {
            if matches!(chunk_key.tag, ChunkRecordTag::Unknown(_)) {
                return Self::Unknown(Bytes::copy_from_slice(key));
            }
            return Self::Chunk(chunk_key);
        }
        Self::Unknown(Bytes::copy_from_slice(key))
    }

    #[must_use]
    pub fn summary_kind(&self) -> String {
        match self {
            Self::Chunk(key) => format!("Chunk::{:?}", key.tag),
            Self::LocalPlayer => "LocalPlayer".to_string(),
            Self::RemotePlayer(_) => "RemotePlayer".to_string(),
            Self::ActorPrefix { .. } => "ActorPrefix".to_string(),
            Self::ActorDigest { .. } => "ActorDigest".to_string(),
            Self::Map(_) => "Map".to_string(),
            Self::Village(village) => format!("Village::{:?}", village.kind),
            Self::Portals => "Portals".to_string(),
            Self::SchedulerWt => "SchedulerWt".to_string(),
            Self::StructureTemplate(_) => "StructureTemplate".to_string(),
            Self::TickingArea(_) => "TickingArea".to_string(),
            Self::GameFlatWorldLayers => "GameFlatWorldLayers".to_string(),
            Self::PlainString(value) => format!("PlainString::{value}"),
            Self::Unknown(_) => "Unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkKey {
    pub pos: ChunkPos,
    pub tag: ChunkRecordTag,
    pub subchunk_y: Option<i8>,
}

impl ChunkKey {
    #[must_use]
    pub const fn new(pos: ChunkPos, tag: ChunkRecordTag) -> Self {
        Self {
            pos,
            tag,
            subchunk_y: None,
        }
    }

    #[must_use]
    pub const fn subchunk(pos: ChunkPos, y: i8) -> Self {
        Self {
            pos,
            tag: ChunkRecordTag::SubChunkPrefix,
            subchunk_y: Some(y),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(if self.pos.dimension == Dimension::Overworld {
            10
        } else {
            14
        });
        bytes.extend_from_slice(&self.pos.x.to_le_bytes());
        bytes.extend_from_slice(&self.pos.z.to_le_bytes());
        if self.pos.dimension != Dimension::Overworld {
            bytes.extend_from_slice(&self.pos.dimension.id().to_le_bytes());
        }
        bytes.push(self.tag.byte());
        if let Some(y) = self.subchunk_y {
            bytes.push(y.to_ne_bytes()[0]);
        }
        Bytes::from(bytes)
    }

    pub fn decode(key: &[u8]) -> Result<Self> {
        match key.len() {
            9 | 10 | 13 | 14 => {}
            len => {
                return Err(BedrockWorldError::InvalidKey(format!(
                    "unsupported chunk key length: {len}"
                )));
            }
        }

        let x = read_i32(key, 0)?;
        let z = read_i32(key, 4)?;
        let (dimension, tag_index) = if key.len() >= 13 {
            (Dimension::from_id(read_i32(key, 8)?), 12)
        } else {
            (Dimension::Overworld, 8)
        };
        let tag = ChunkRecordTag::from_byte(
            *key.get(tag_index)
                .ok_or_else(|| BedrockWorldError::InvalidKey("missing record tag".to_string()))?,
        );
        let subchunk_y = if matches!(key.len(), 10 | 14) {
            Some(i8::from_ne_bytes([key[tag_index + 1]]))
        } else {
            None
        };
        Ok(Self {
            pos: ChunkPos { x, z, dimension },
            tag,
            subchunk_y,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRecord {
    pub key: ChunkKey,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockState {
    pub name: String,
    pub states: BTreeMap<String, NbtTag>,
    pub version: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockPalette {
    pub states: Vec<BlockState>,
    pub indices: Option<Vec<u16>>,
    pub counts: Vec<u16>,
}

impl BlockPalette {
    #[must_use]
    pub fn palette_index_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u16> {
        if local_x >= 16 || local_y >= 16 || local_z >= 16 {
            return None;
        }
        self.indices
            .as_ref()?
            .get(block_storage_index(local_x, local_y, local_z))
            .copied()
    }

    #[must_use]
    pub fn block_state_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<&BlockState> {
        let palette_index = usize::from(self.palette_index_at(local_x, local_y, local_z)?);
        self.states.get(palette_index)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubChunkDecodeMode {
    CountsOnly,
    #[default]
    FullIndices,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubChunkFormat {
    LegacySubChunk(LegacySubChunk),
    LegacyTerrain,
    FixedArrayV1,
    Paletted {
        version: u8,
        storages: Vec<BlockPalette>,
    },
    Raw {
        version: Option<u8>,
        bytes: Bytes,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubChunk {
    pub y: i8,
    pub format: SubChunkFormat,
}

impl SubChunk {
    #[must_use]
    pub fn block_state_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<&BlockState> {
        match &self.format {
            SubChunkFormat::Paletted { storages, .. } => storages
                .first()
                .and_then(|storage| storage.block_state_at(local_x, local_y, local_z)),
            _ => None,
        }
    }

    #[must_use]
    pub fn legacy_block_id_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        match &self.format {
            SubChunkFormat::LegacySubChunk(subchunk) => {
                subchunk.block_id_at(local_x, local_y, local_z)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn legacy_block_data_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        match &self.format {
            SubChunkFormat::LegacySubChunk(subchunk) => {
                subchunk.block_data_at(local_x, local_y, local_z)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyTerrain {
    bytes: Bytes,
}

impl LegacyTerrain {
    pub fn parse(bytes: Bytes) -> Result<Self> {
        if bytes.len() != LEGACY_TERRAIN_VALUE_LEN {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "LegacyTerrain value must be {LEGACY_TERRAIN_VALUE_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Self { bytes })
    }

    #[must_use]
    pub fn raw(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub fn block_ids(&self) -> &[u8] {
        &self.bytes[..LEGACY_TERRAIN_BLOCK_COUNT]
    }

    #[must_use]
    pub fn block_data(&self) -> &[u8] {
        &self.bytes[LEGACY_TERRAIN_BLOCK_DATA_OFFSET..LEGACY_TERRAIN_SKY_LIGHT_OFFSET]
    }

    #[must_use]
    pub fn sky_light(&self) -> &[u8] {
        &self.bytes[LEGACY_TERRAIN_SKY_LIGHT_OFFSET..LEGACY_TERRAIN_BLOCK_LIGHT_OFFSET]
    }

    #[must_use]
    pub fn block_light(&self) -> &[u8] {
        &self.bytes[LEGACY_TERRAIN_BLOCK_LIGHT_OFFSET..LEGACY_TERRAIN_HEIGHTMAP_OFFSET]
    }

    #[must_use]
    pub fn heightmap(&self) -> &[u8] {
        &self.bytes[LEGACY_TERRAIN_HEIGHTMAP_OFFSET..LEGACY_TERRAIN_BIOME_OFFSET]
    }

    #[must_use]
    pub fn biomes(&self) -> &[u8] {
        &self.bytes[LEGACY_TERRAIN_BIOME_OFFSET..LEGACY_TERRAIN_VALUE_LEN]
    }

    #[must_use]
    pub fn block_index(local_x: u8, local_y: u8, local_z: u8) -> Option<usize> {
        if local_x < 16 && local_y < 128 && local_z < 16 {
            Some((usize::from(local_y) * 16 + usize::from(local_z)) * 16 + usize::from(local_x))
        } else {
            None
        }
    }

    #[must_use]
    pub fn column_index(local_x: u8, local_z: u8) -> Option<usize> {
        if local_x < 16 && local_z < 16 {
            Some(usize::from(local_z) * 16 + usize::from(local_x))
        } else {
            None
        }
    }

    #[must_use]
    pub fn block_id_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| self.block_ids().get(index).copied())
    }

    #[must_use]
    pub fn block_data_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.block_data(), index))
    }

    #[must_use]
    pub fn sky_light_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.sky_light(), index))
    }

    #[must_use]
    pub fn block_light_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.block_light(), index))
    }

    #[must_use]
    pub fn height_at(&self, local_x: u8, local_z: u8) -> Option<u8> {
        Self::column_index(local_x, local_z).and_then(|index| self.heightmap().get(index).copied())
    }

    #[must_use]
    pub fn biome_color_at(&self, local_x: u8, local_z: u8) -> Option<u32> {
        let offset = Self::column_index(local_x, local_z)?.checked_mul(4)?;
        let bytes: [u8; 4] = self.biomes().get(offset..offset + 4)?.try_into().ok()?;
        Some(u32::from_le_bytes(bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacySubChunk {
    version: u8,
    bytes: Bytes,
}

impl LegacySubChunk {
    pub fn parse(bytes: Bytes) -> Result<Self> {
        let Some(version) = bytes.first().copied() else {
            return Err(BedrockWorldError::UnsupportedChunkFormat(
                "legacy subchunk value is empty".to_string(),
            ));
        };
        if !matches!(version, 0 | 2..=7) {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "version {version} is not a legacy subchunk payload"
            )));
        }
        if !matches!(
            bytes.len(),
            LEGACY_SUBCHUNK_MIN_VALUE_LEN | LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN
        ) {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "legacy subchunk value has invalid length {}",
                bytes.len()
            )));
        }
        Ok(Self { version, bytes })
    }

    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    #[must_use]
    pub fn raw(&self) -> &Bytes {
        &self.bytes
    }

    #[must_use]
    pub fn block_ids(&self) -> &[u8] {
        let start = 1;
        let end = start + LEGACY_SUBCHUNK_BLOCK_COUNT;
        &self.bytes[start..end]
    }

    #[must_use]
    pub fn block_data(&self) -> &[u8] {
        let start = 1 + LEGACY_SUBCHUNK_BLOCK_COUNT;
        let end = start + LEGACY_SUBCHUNK_BLOCK_COUNT / 2;
        &self.bytes[start..end]
    }

    #[must_use]
    pub fn sky_light(&self) -> Option<&[u8]> {
        if self.bytes.len() != LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN {
            return None;
        }
        let start = 1 + LEGACY_SUBCHUNK_BLOCK_COUNT + LEGACY_SUBCHUNK_BLOCK_COUNT / 2;
        let end = start + LEGACY_SUBCHUNK_BLOCK_COUNT / 2;
        Some(&self.bytes[start..end])
    }

    #[must_use]
    pub fn block_light(&self) -> Option<&[u8]> {
        if self.bytes.len() != LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN {
            return None;
        }
        let start = 1 + LEGACY_SUBCHUNK_BLOCK_COUNT + LEGACY_SUBCHUNK_BLOCK_COUNT;
        Some(&self.bytes[start..])
    }

    #[must_use]
    pub fn block_index(local_x: u8, local_y: u8, local_z: u8) -> Option<usize> {
        if local_x < 16 && local_y < 16 && local_z < 16 {
            Some((usize::from(local_y) * 16 + usize::from(local_z)) * 16 + usize::from(local_x))
        } else {
            None
        }
    }

    #[must_use]
    pub fn block_id_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| self.block_ids().get(index).copied())
    }

    #[must_use]
    pub fn block_data_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.block_data(), index))
    }

    #[must_use]
    pub fn sky_light_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.sky_light()?, index))
    }

    #[must_use]
    pub fn block_light_at(&self, local_x: u8, local_y: u8, local_z: u8) -> Option<u8> {
        Self::block_index(local_x, local_y, local_z)
            .and_then(|index| nibble_at(self.block_light()?, index))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntityData {
    pub tag: NbtTag,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub pos: ChunkPos,
    pub version: Option<u8>,
    pub records: Vec<ChunkRecord>,
}

impl Chunk {
    pub fn get_subchunk(&self, y: i8) -> Result<Option<SubChunk>> {
        let Some(record) = self.records.iter().find(|record| {
            record.key.tag == ChunkRecordTag::SubChunkPrefix && record.key.subchunk_y == Some(y)
        }) else {
            return Ok(None);
        };
        parse_subchunk(y, record.value.clone()).map(Some)
    }

    pub fn legacy_terrain(&self) -> Result<Option<LegacyTerrain>> {
        let Some(record) = self
            .records
            .iter()
            .find(|record| record.key.tag == ChunkRecordTag::LegacyTerrain)
        else {
            return Ok(None);
        };
        LegacyTerrain::parse(record.value.clone()).map(Some)
    }

    pub fn get_block(&self, x: u8, y: i16, z: u8) -> Result<BlockState> {
        if x >= 16 || z >= 16 {
            return Err(BedrockWorldError::Validation(format!(
                "local block coordinates must use x/z in 0..15, got x={x}, z={z}"
            )));
        }

        let subchunk_y = i8::try_from(i32::from(y).div_euclid(16)).map_err(|_| {
            BedrockWorldError::Validation(format!(
                "block y={y} cannot be represented as a Bedrock subchunk index"
            ))
        })?;
        let local_y = u8::try_from(i32::from(y).rem_euclid(16)).map_err(|_| {
            BedrockWorldError::Validation(format!("block y={y} has invalid local subchunk offset"))
        })?;
        let Some(subchunk) = self.get_subchunk(subchunk_y)? else {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "chunk {:?} has no subchunk at y={subchunk_y}",
                self.pos
            )));
        };
        if let Some(state) = subchunk.block_state_at(x, local_y, z) {
            return Ok(state.clone());
        }
        if let Some(id) = subchunk.legacy_block_id_at(x, local_y, z) {
            let mut states = BTreeMap::new();
            if let Some(data) = subchunk.legacy_block_data_at(x, local_y, z) {
                states.insert("data".to_string(), NbtTag::Byte(data as i8));
            }
            return Ok(BlockState {
                name: format!("legacy:{id}"),
                states,
                version: None,
            });
        }
        Err(BedrockWorldError::UnsupportedChunkFormat(format!(
            "chunk {:?} does not expose a block state at local ({x}, {y}, {z})",
            self.pos
        )))
    }

    pub fn set_block(&mut self, _x: u8, _y: i16, _z: u8, _block: BlockState) -> Result<()> {
        Err(BedrockWorldError::UnsupportedChunkFormat(
            "structured block editing is not enabled for this chunk format".to_string(),
        ))
    }

    pub fn get_entities(&self) -> Result<Vec<EntityData>> {
        let mut entities = Vec::new();
        for record in self
            .records
            .iter()
            .filter(|record| record.key.tag == ChunkRecordTag::Entity)
        {
            entities.extend(parse_consecutive_nbt(record.value.as_ref())?);
        }
        Ok(entities)
    }

    pub fn get_block_entities(&self) -> Result<Vec<EntityData>> {
        let mut entities = Vec::new();
        for record in self
            .records
            .iter()
            .filter(|record| record.key.tag == ChunkRecordTag::BlockEntity)
        {
            entities.extend(parse_consecutive_nbt(record.value.as_ref())?);
        }
        Ok(entities)
    }
}

pub fn parse_subchunk(y: i8, bytes: Bytes) -> Result<SubChunk> {
    parse_subchunk_with_mode(y, bytes, SubChunkDecodeMode::FullIndices)
}

pub fn parse_subchunk_with_mode(y: i8, bytes: Bytes, mode: SubChunkDecodeMode) -> Result<SubChunk> {
    let version = bytes.first().copied();
    let format = match version {
        Some(0 | 2..=7) => LegacySubChunk::parse(bytes.clone()).map_or_else(
            |_| SubChunkFormat::Raw { version, bytes },
            SubChunkFormat::LegacySubChunk,
        ),
        Some(version @ 1) => parse_palette_storages(&bytes, 1, 1, mode).map_or_else(
            |_| SubChunkFormat::Raw {
                version: Some(version),
                bytes,
            },
            |storages| SubChunkFormat::Paletted { version, storages },
        ),
        Some(version @ 8..=u8::MAX) => parse_paletted_subchunk(version, &bytes, mode)
            .unwrap_or_else(|_| SubChunkFormat::Raw {
                version: Some(version),
                bytes,
            }),
        _ => SubChunkFormat::Raw { version, bytes },
    };
    Ok(SubChunk { y, format })
}

fn parse_consecutive_nbt(bytes: &[u8]) -> Result<Vec<EntityData>> {
    parse_consecutive_root_nbt(bytes)
        .map(|tags| tags.into_iter().map(|tag| EntityData { tag }).collect())
}

fn parse_paletted_subchunk(
    version: u8,
    bytes: &[u8],
    mode: SubChunkDecodeMode,
) -> Result<SubChunkFormat> {
    let Some(storage_count) = bytes.get(1).copied() else {
        return Err(BedrockWorldError::UnsupportedChunkFormat(
            "paletted subchunk is missing storage count".to_string(),
        ));
    };
    let offsets: &[usize] = if version == 9 { &[3, 2] } else { &[2] };
    for offset in offsets {
        if let Ok(storages) = parse_palette_storages(bytes, *offset, storage_count, mode) {
            return Ok(SubChunkFormat::Paletted { version, storages });
        }
    }
    Err(BedrockWorldError::UnsupportedChunkFormat(
        "unsupported paletted subchunk layout".to_string(),
    ))
}

fn parse_palette_storages(
    bytes: &[u8],
    mut offset: usize,
    storage_count: u8,
    mode: SubChunkDecodeMode,
) -> Result<Vec<BlockPalette>> {
    let mut storages = Vec::with_capacity(usize::from(storage_count));
    for _ in 0..storage_count {
        let header = *bytes.get(offset).ok_or_else(|| {
            BedrockWorldError::UnsupportedChunkFormat(
                "palette storage header is missing".to_string(),
            )
        })?;
        offset += 1;

        let bits_per_block = header >> 1;
        if !matches!(bits_per_block, 0 | 1 | 2 | 3 | 4 | 5 | 6 | 8 | 16) {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "unsupported bits-per-block value: {bits_per_block}"
            )));
        }

        let word_count = packed_word_count(bits_per_block);
        let words_byte_len = word_count.checked_mul(4).ok_or_else(|| {
            BedrockWorldError::UnsupportedChunkFormat("palette word count overflowed".to_string())
        })?;
        let words_bytes = bytes.get(offset..offset + words_byte_len).ok_or_else(|| {
            BedrockWorldError::UnsupportedChunkFormat(
                "palette block indices are truncated".to_string(),
            )
        })?;
        offset += words_byte_len;

        let palette_len = read_i32_at(bytes, offset)?;
        offset += 4;
        if palette_len < 0 {
            return Err(BedrockWorldError::UnsupportedChunkFormat(
                "palette length cannot be negative".to_string(),
            ));
        }
        let palette_len = usize::try_from(palette_len).map_err(|_| {
            BedrockWorldError::UnsupportedChunkFormat("palette length overflowed".to_string())
        })?;
        if palette_len > MAX_SUBCHUNK_PALETTE_LEN {
            return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                "palette length {palette_len} exceeds maximum {MAX_SUBCHUNK_PALETTE_LEN}"
            )));
        }
        let mut states = Vec::with_capacity(palette_len);
        for _ in 0..palette_len {
            let (tag, consumed) = parse_root_nbt_with_consumed(&bytes[offset..])?;
            offset += consumed;
            states.push(block_state_from_nbt(&tag));
        }

        let indices = unpack_palette_indices(words_bytes, bits_per_block, palette_len)?;
        let mut counts = vec![0_u16; palette_len];
        for index in &indices {
            if let Some(count) = counts.get_mut(usize::from(*index)) {
                *count = count.saturating_add(1);
            }
        }
        let indices = match mode {
            SubChunkDecodeMode::CountsOnly => None,
            SubChunkDecodeMode::FullIndices => Some(indices),
        };
        storages.push(BlockPalette {
            states,
            indices,
            counts,
        });
    }
    Ok(storages)
}

fn packed_word_count(bits_per_block: u8) -> usize {
    if bits_per_block == 0 {
        return 0;
    }
    let values_per_word = usize::from(32 / bits_per_block);
    4096_usize.div_ceil(values_per_word)
}

fn unpack_palette_indices(
    words_bytes: &[u8],
    bits_per_block: u8,
    palette_len: usize,
) -> Result<Vec<u16>> {
    if bits_per_block == 0 {
        return Ok(vec![0; 4096]);
    }
    let values_per_word = usize::from(32 / bits_per_block);
    let mask = (1_u32 << bits_per_block) - 1;
    let mut indices = Vec::with_capacity(4096);
    for word_bytes in words_bytes.chunks_exact(4) {
        let word = u32::from_le_bytes(
            word_bytes
                .try_into()
                .map_err(|_| BedrockWorldError::CorruptWorld("bad palette word".to_string()))?,
        );
        for item_index in 0..values_per_word {
            if indices.len() == 4096 {
                break;
            }
            let value = ((word >> (item_index * usize::from(bits_per_block))) & mask) as u16;
            if palette_len > 0 && usize::from(value) >= palette_len {
                return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
                    "palette index {value} exceeds palette length {palette_len}"
                )));
            }
            indices.push(value);
        }
    }
    if indices.len() != 4096 {
        return Err(BedrockWorldError::UnsupportedChunkFormat(format!(
            "palette produced {} block indices instead of 4096",
            indices.len()
        )));
    }
    Ok(indices)
}

fn block_state_from_nbt(tag: &NbtTag) -> BlockState {
    let NbtTag::Compound(root) = tag else {
        return BlockState {
            name: "<invalid>".to_string(),
            states: BTreeMap::new(),
            version: None,
        };
    };
    let name = string_field(root, "name")
        .or_else(|| string_field(root, "Name"))
        .unwrap_or("<unknown>")
        .to_string();
    let states = match root.get("states").or_else(|| root.get("States")) {
        Some(NbtTag::Compound(values)) => values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        _ => BTreeMap::new(),
    };
    let version = int_field(root, "version").or_else(|| int_field(root, "Version"));
    BlockState {
        name,
        states,
        version,
    }
}

fn string_field<'a>(root: &'a IndexMap<String, NbtTag>, key: &str) -> Option<&'a str> {
    match root.get(key) {
        Some(NbtTag::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn int_field(root: &IndexMap<String, NbtTag>, key: &str) -> Option<i32> {
    match root.get(key) {
        Some(NbtTag::Byte(value)) => Some(i32::from(*value)),
        Some(NbtTag::Short(value)) => Some(i32::from(*value)),
        Some(NbtTag::Int(value)) => Some(*value),
        _ => None,
    }
}

fn read_i32_at(bytes: &[u8], offset: usize) -> Result<i32> {
    let slice: [u8; 4] = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| {
            BedrockWorldError::UnsupportedChunkFormat("i32 field is truncated".to_string())
        })?
        .try_into()
        .map_err(|_| BedrockWorldError::UnsupportedChunkFormat("bad i32 field".to_string()))?;
    Ok(i32::from_le_bytes(slice))
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| BedrockWorldError::InvalidKey("chunk key is truncated".to_string()))?;
    let slice: [u8; 4] = slice
        .try_into()
        .map_err(|_| BedrockWorldError::InvalidKey("invalid i32 field".to_string()))?;
    Ok(i32::from_le_bytes(slice))
}

fn parse_i64_suffix(key: &[u8], prefix: &[u8]) -> Option<i64> {
    let suffix = key.strip_prefix(prefix)?;
    let bytes: [u8; 8] = suffix.try_into().ok()?;
    Some(i64::from_le_bytes(bytes))
}

fn parse_chunk_pos_suffix(key: &[u8], prefix: &[u8]) -> Option<ChunkPos> {
    let suffix = key.strip_prefix(prefix)?;
    match suffix.len() {
        8 => Some(ChunkPos {
            x: read_i32_optional(suffix, 0)?,
            z: read_i32_optional(suffix, 4)?,
            dimension: Dimension::Overworld,
        }),
        12 => Some(ChunkPos {
            x: read_i32_optional(suffix, 0)?,
            z: read_i32_optional(suffix, 4)?,
            dimension: Dimension::from_id(read_i32_optional(suffix, 8)?),
        }),
        _ => None,
    }
}

fn read_i32_optional(bytes: &[u8], offset: usize) -> Option<i32> {
    let slice: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;
    Some(i32::from_le_bytes(slice))
}

fn nibble_at(bytes: &[u8], index: usize) -> Option<u8> {
    let byte = *bytes.get(index / 2)?;
    Some(if index.is_multiple_of(2) {
        byte & 0x0f
    } else {
        byte >> 4
    })
}

fn ascii_suffix(key: &[u8], prefix: &[u8]) -> Option<String> {
    let suffix = key.strip_prefix(prefix)?;
    if suffix.iter().all(u8::is_ascii_graphic) {
        return Some(String::from_utf8_lossy(suffix).into_owned());
    }
    None
}

fn parse_village_key(key: &[u8]) -> Option<ParsedVillageKey> {
    let raw = std::str::from_utf8(key).ok()?;
    let parts = raw.split('_').collect::<Vec<_>>();
    if !matches!(parts.as_slice(), ["VILLAGE", ..]) || !matches!(parts.len(), 3 | 4) {
        return None;
    }
    let dimension = if parts.len() == 4 {
        Some(match parts[1] {
            "Overworld" => Dimension::Overworld,
            "Nether" => Dimension::Nether,
            "TheEnd" => Dimension::End,
            _ => return None,
        })
    } else {
        None
    };
    let uuid = parts[parts.len() - 2];
    if uuid.len() != 36 {
        return None;
    }
    let kind = match parts[parts.len() - 1] {
        "INFO" => VillageRecordKind::Info,
        "DWELLERS" => VillageRecordKind::Dwellers,
        "PLAYERS" => VillageRecordKind::Players,
        "POI" => VillageRecordKind::Poi,
        _ => VillageRecordKind::Unknown,
    };
    Some(ParsedVillageKey {
        raw: raw.to_string(),
        dimension,
        uuid: uuid.to_string(),
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbt::serialize_root_nbt;

    #[test]
    fn chunk_key_roundtrips_overworld_and_subchunk() {
        let pos = ChunkPos {
            x: -3,
            z: 7,
            dimension: Dimension::Overworld,
        };
        let key = ChunkKey::subchunk(pos, -4);
        let encoded = key.encode();

        assert_eq!(encoded.len(), 10);
        assert_eq!(ChunkKey::decode(&encoded).expect("decode"), key);
    }

    #[test]
    fn chunk_key_roundtrips_dimension_key() {
        let pos = ChunkPos {
            x: 1,
            z: 2,
            dimension: Dimension::Nether,
        };
        let key = ChunkKey::new(pos, ChunkRecordTag::Version);
        let encoded = key.encode();

        assert_eq!(encoded.len(), 13);
        assert_eq!(ChunkKey::decode(&encoded).expect("decode"), key);
    }

    #[test]
    fn bedrock_db_key_decodes_actor_and_digp_keys() {
        let mut actor_key = b"actorprefix".to_vec();
        actor_key.extend_from_slice(&42_i64.to_le_bytes());
        assert_eq!(
            BedrockDbKey::decode(&actor_key),
            BedrockDbKey::ActorPrefix { actor_id: 42 }
        );

        let mut digp_key = b"digp".to_vec();
        digp_key.extend_from_slice(&1_i32.to_le_bytes());
        digp_key.extend_from_slice(&(-2_i32).to_le_bytes());
        assert_eq!(
            BedrockDbKey::decode(&digp_key),
            BedrockDbKey::ActorDigest {
                pos: ChunkPos {
                    x: 1,
                    z: -2,
                    dimension: Dimension::Overworld
                }
            }
        );
    }

    #[test]
    fn chunk_record_tags_align_with_bedrock_level_reference() {
        let expected = [
            (0x2b, ChunkRecordTag::Data3D),
            (0x2c, ChunkRecordTag::Version),
            (0x2d, ChunkRecordTag::Data2D),
            (0x2e, ChunkRecordTag::Data2DLegacy),
            (0x2f, ChunkRecordTag::SubChunkPrefix),
            (0x30, ChunkRecordTag::LegacyTerrain),
            (0x31, ChunkRecordTag::BlockEntity),
            (0x32, ChunkRecordTag::Entity),
            (0x33, ChunkRecordTag::PendingTicks),
            (0x34, ChunkRecordTag::BlockExtraData),
            (0x35, ChunkRecordTag::BiomeState),
            (0x36, ChunkRecordTag::FinalizedState),
            (0x37, ChunkRecordTag::ConversionData),
            (0x38, ChunkRecordTag::BorderBlocks),
            (0x39, ChunkRecordTag::HardcodedSpawners),
            (0x3a, ChunkRecordTag::RandomTicks),
            (0x3b, ChunkRecordTag::Checksums),
            (0x3c, ChunkRecordTag::GenerationSeed),
            (0x3d, ChunkRecordTag::GeneratedPreCavesAndCliffsBlending),
            (0x3e, ChunkRecordTag::BlendingBiomeHeight),
            (0x3f, ChunkRecordTag::MetaDataHash),
            (0x40, ChunkRecordTag::BlendingData),
            (0x41, ChunkRecordTag::ActorDigestVersion),
            (0x76, ChunkRecordTag::VersionOld),
        ];
        for (byte, tag) in expected {
            assert_eq!(ChunkRecordTag::from_byte(byte), tag);
            assert_eq!(tag.byte(), byte);
        }
    }

    #[test]
    fn bedrock_db_key_decodes_specific_ascii_keys_before_plain_keys() {
        assert_eq!(
            BedrockDbKey::decode(b"map_42"),
            BedrockDbKey::Map("42".to_string())
        );
        assert!(matches!(
            BedrockDbKey::decode(b"VILLAGE_12345678-1234-1234-1234-123456789abc_INFO"),
            BedrockDbKey::Village(_)
        ));
        assert!(matches!(
            BedrockDbKey::decode(b"LevelChunkMetaDataDictionary"),
            BedrockDbKey::PlainString(_)
        ));
    }

    #[test]
    fn chunk_pos_matches_bedrock_level_height_ranges() {
        let overworld = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        assert_eq!(overworld.y_range(ChunkVersion::Old), (0, 255));
        assert_eq!(overworld.y_range(ChunkVersion::New), (-64, 319));
        assert_eq!(overworld.subchunk_index_range(ChunkVersion::New), (-4, 19));
        assert_eq!(
            BlockPos {
                x: -1,
                y: 64,
                z: -1
            }
            .to_chunk_pos(Dimension::Overworld),
            ChunkPos {
                x: -1,
                z: -1,
                dimension: Dimension::Overworld
            }
        );
    }

    #[test]
    fn legacy_terrain_exposes_old_leveldb_arrays() {
        let mut bytes = vec![0; LEGACY_TERRAIN_VALUE_LEN];
        let block_index = LegacyTerrain::block_index(1, 2, 3).expect("block index");
        let column_index = LegacyTerrain::column_index(1, 3).expect("column index");
        bytes[block_index] = 42;
        bytes[LEGACY_TERRAIN_BLOCK_DATA_OFFSET + block_index / 2] = 0xba;
        bytes[LEGACY_TERRAIN_SKY_LIGHT_OFFSET + block_index / 2] = 0xc7;
        bytes[LEGACY_TERRAIN_BLOCK_LIGHT_OFFSET + block_index / 2] = 0xd5;
        bytes[LEGACY_TERRAIN_HEIGHTMAP_OFFSET + column_index] = 99;
        bytes[LEGACY_TERRAIN_BIOME_OFFSET + column_index * 4
            ..LEGACY_TERRAIN_BIOME_OFFSET + column_index * 4 + 4]
            .copy_from_slice(&0x00ab_cdef_u32.to_le_bytes());

        let terrain = LegacyTerrain::parse(Bytes::from(bytes)).expect("legacy terrain");

        assert_eq!(terrain.block_id_at(1, 2, 3), Some(42));
        assert_eq!(terrain.block_data_at(1, 2, 3), Some(0x0b));
        assert_eq!(terrain.sky_light_at(1, 2, 3), Some(0x0c));
        assert_eq!(terrain.block_light_at(1, 2, 3), Some(0x0d));
        assert_eq!(terrain.height_at(1, 3), Some(99));
        assert_eq!(terrain.biome_color_at(1, 3), Some(0x00ab_cdef));
        assert!(LegacyTerrain::parse(Bytes::from_static(b"short")).is_err());
    }

    #[test]
    fn legacy_subchunk_decodes_block_ids_metadata_and_light() {
        let mut bytes = vec![0; LEGACY_SUBCHUNK_WITH_LIGHT_VALUE_LEN];
        bytes[0] = 2;
        let index = LegacySubChunk::block_index(4, 5, 6).expect("block index");
        bytes[1 + index] = 7;
        bytes[1 + LEGACY_SUBCHUNK_BLOCK_COUNT + index / 2] = 0x0c;
        bytes[1 + LEGACY_SUBCHUNK_BLOCK_COUNT + LEGACY_SUBCHUNK_BLOCK_COUNT / 2 + index / 2] = 0x0e;
        bytes[1 + LEGACY_SUBCHUNK_BLOCK_COUNT + LEGACY_SUBCHUNK_BLOCK_COUNT + index / 2] = 0x0a;

        let subchunk = parse_subchunk(0, Bytes::from(bytes)).expect("parse legacy subchunk");

        let SubChunkFormat::LegacySubChunk(legacy) = &subchunk.format else {
            panic!("expected legacy subchunk");
        };
        assert_eq!(legacy.version(), 2);
        assert_eq!(legacy.block_id_at(4, 5, 6), Some(7));
        assert_eq!(legacy.block_data_at(4, 5, 6), Some(0x0c));
        assert_eq!(legacy.sky_light_at(4, 5, 6), Some(0x0e));
        assert_eq!(legacy.block_light_at(4, 5, 6), Some(0x0a));
        assert_eq!(subchunk.legacy_block_id_at(4, 5, 6), Some(7));
    }

    #[test]
    fn paletted_subchunk_v1_uses_single_storage_without_count_byte() {
        let mut bytes = build_paletted_subchunk(8, None, 4, 4);
        bytes.remove(1);
        bytes[0] = 1;

        let subchunk = parse_subchunk(0, Bytes::from(bytes)).expect("parse v1 palette");

        let SubChunkFormat::Paletted { version, storages } = subchunk.format else {
            panic!("expected v1 paletted subchunk");
        };
        assert_eq!(version, 1);
        assert_eq!(storages.len(), 1);
        assert_eq!(storages[0].indices.as_ref().expect("indices").len(), 4096);
    }

    #[test]
    fn paletted_subchunk_decodes_supported_bits_per_block() {
        for bits_per_block in [0, 1, 2, 3, 4, 5, 6, 8, 16] {
            let bytes = build_paletted_subchunk(8, None, bits_per_block, 4);

            let subchunk = parse_subchunk(0, Bytes::from(bytes)).expect("parse");

            let SubChunkFormat::Paletted { storages, .. } = subchunk.format else {
                panic!("expected paletted subchunk for {bits_per_block} bits");
            };
            assert_eq!(storages.len(), 1);
            assert_eq!(storages[0].indices.as_ref().expect("indices").len(), 4096);
            assert_eq!(storages[0].counts.iter().sum::<u16>(), 4096);
        }
    }

    #[test]
    fn paletted_subchunk_counts_only_drops_indices_but_keeps_counts() {
        let bytes = build_paletted_subchunk(8, None, 4, 4);

        let subchunk =
            parse_subchunk_with_mode(0, Bytes::from(bytes), SubChunkDecodeMode::CountsOnly)
                .expect("parse");

        let SubChunkFormat::Paletted { storages, .. } = subchunk.format else {
            panic!("expected paletted subchunk");
        };
        assert!(storages[0].indices.is_none());
        assert_eq!(storages[0].counts.iter().sum::<u16>(), 4096);
    }

    #[test]
    fn paletted_subchunk_v9_accepts_embedded_y_byte() {
        let bytes = build_paletted_subchunk(9, Some(-4), 4, 4);

        let subchunk = parse_subchunk(-4, Bytes::from(bytes)).expect("parse");

        let SubChunkFormat::Paletted { storages, .. } = subchunk.format else {
            panic!("expected paletted v9 subchunk");
        };
        assert_eq!(storages[0].states.len(), 4);
    }

    #[test]
    fn block_state_lookup_uses_xz_plane_storage_order() {
        let bytes = build_paletted_subchunk(8, None, 4, 8);
        let subchunk = parse_subchunk(0, Bytes::from(bytes)).expect("parse");

        assert_eq!(block_storage_index(1, 2, 3), 306);
        let state = subchunk
            .block_state_at(1, 2, 3)
            .expect("block state at x=1 y=2 z=3");

        assert_eq!(
            state.name,
            format!("minecraft:block_{}", block_storage_index(1, 2, 3) % 8)
        );
    }

    #[test]
    fn chunk_get_block_reads_decoded_paletted_subchunk() {
        let pos = ChunkPos {
            x: 0,
            z: 0,
            dimension: Dimension::Overworld,
        };
        let key = ChunkKey::subchunk(pos, 0);
        let chunk = Chunk {
            pos,
            version: Some(8),
            records: vec![ChunkRecord {
                key,
                value: Bytes::from(build_paletted_subchunk(8, None, 4, 8)),
            }],
        };

        let state = chunk.get_block(1, 2, 3).expect("block state");

        assert_eq!(state.name, "minecraft:block_2");
    }

    fn build_paletted_subchunk(
        version: u8,
        embedded_y: Option<i8>,
        bits_per_block: u8,
        palette_len: usize,
    ) -> Vec<u8> {
        let palette_len = if bits_per_block == 0 { 1 } else { palette_len };
        let mut bytes = vec![version, 1];
        if let Some(y) = embedded_y {
            bytes.push(y as u8);
        }
        bytes.push(bits_per_block << 1);
        let values_per_word = if bits_per_block == 0 {
            4096
        } else {
            usize::from(32 / bits_per_block)
        };
        let mut words = vec![0_u32; packed_word_count(bits_per_block)];
        if bits_per_block != 0 {
            for block_index in 0..4096 {
                let value = u32::try_from(block_index % palette_len).expect("palette index");
                let word_index = block_index / values_per_word;
                let bit_offset = (block_index % values_per_word) * usize::from(bits_per_block);
                words[word_index] |= value << bit_offset;
            }
        }
        for word in words {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(
            &i32::try_from(palette_len)
                .expect("palette length")
                .to_le_bytes(),
        );
        for index in 0..palette_len {
            let tag = NbtTag::Compound(IndexMap::from([
                (
                    "name".to_string(),
                    NbtTag::String(format!("minecraft:block_{index}")),
                ),
                ("states".to_string(), NbtTag::Compound(IndexMap::new())),
                ("version".to_string(), NbtTag::Int(1)),
            ]));
            bytes.extend_from_slice(&serialize_root_nbt(&tag).expect("serialize palette"));
        }
        bytes
    }
}
