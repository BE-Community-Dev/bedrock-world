use bedrock_world::{
    BedrockLevelDbStorage, BedrockWorld, ChunkPos, Dimension, OpenOptions, WorldScanOptions,
    WorldThreadingOptions, read_level_dat_document,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

fn fixture_world_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sample-bedrock-world")
}

fn main() {
    let world_path = fixture_world_path();
    if !world_path.join("level.dat").exists() || !world_path.join("db").exists() {
        eprintln!(
            "large fixture is missing at {}; benchmark skipped",
            world_path.display()
        );
        return;
    }

    let start = Instant::now();
    let level_dat = read_level_dat_document(&world_path.join("level.dat")).expect("read level.dat");
    println!(
        "large_fixture.level_dat elapsed_ms={} version={} payload_len={}",
        start.elapsed().as_millis(),
        level_dat.header.version,
        level_dat.header.actual_payload_len
    );

    let start = Instant::now();
    let storage = Arc::new(BedrockLevelDbStorage::open(world_path.join("db")).expect("open db"));
    println!(
        "large_fixture.db.open_lazy elapsed_ms={}",
        start.elapsed().as_millis()
    );

    let world = BedrockWorld::from_storage(world_path, storage, OpenOptions::default());

    let options = WorldScanOptions {
        threading: WorldThreadingOptions::Single,
        ..WorldScanOptions::default()
    };
    let start = Instant::now();
    let key_kinds = world
        .classify_keys_blocking(options)
        .expect("classify keys");
    let elapsed = start.elapsed();
    let total_entries = key_kinds.values().copied().sum::<usize>();
    println!(
        "large_fixture.classify_keys.single elapsed_ms={} entries={} entries_per_sec={:.2}",
        elapsed.as_millis(),
        total_entries,
        u32::try_from(total_entries)
            .map(f64::from)
            .unwrap_or(f64::INFINITY)
            / elapsed.as_secs_f64()
    );

    let start = Instant::now();
    let players = world.list_players_blocking().expect("list players");
    println!(
        "large_fixture.players elapsed_ms={} count={}",
        start.elapsed().as_millis(),
        players.len()
    );

    let pos = ChunkPos {
        x: 347,
        z: 207,
        dimension: Dimension::Overworld,
    };
    let start = Instant::now();
    let chunk = world.parse_chunk_blocking(pos).expect("parse chunk");
    println!(
        "large_fixture.sample_chunk elapsed_ms={} records={} subchunks={} block_entities={} parse_errors={}",
        start.elapsed().as_millis(),
        chunk.report.entry_count,
        chunk.report.subchunk_count,
        chunk.report.block_entity_count,
        chunk.report.parse_errors.len()
    );
}
