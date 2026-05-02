# bedrock-world

[English](README.md) | [简体中文](README.zh-CN.md)

`bedrock-world` 是基于 `bedrock-leveldb` 的 Minecraft Bedrock 世界库。它提供
快速 `level.dat` 访问、小端 NBT、Bedrock DB key 分类、玩家读取、包含旧 LevelDB
terrain record 的 chunk/subchunk 解析、实体和方块实体解析、物品提取、biome summary，
以及部分 map/village/global record 分类。

本 crate 不生成地图图片，也不复制 `bedrock-dev/bedrock-level` 的图片/颜色生成代码；
该项目只作为解析行为参考。

## 推荐 API

- `read_level_dat(path)` 和 `write_level_dat_atomic(path, document)` 是启动器快路径，
  不会打开 LevelDB。
- `BedrockWorld::open(path, OpenOptions)` 创建 lazy 世界句柄，不会解析整张地图。
- UI 和工具应优先使用分类 API：
  `classify_keys_blocking`、`list_players_blocking`、
  `list_chunk_positions_blocking`、`parse_chunk_blocking`、
  `parse_subchunk_blocking`、`scan_entities_blocking`、
  `scan_block_entities_blocking`、`scan_items_blocking`、`scan_maps_blocking`、
  `scan_villages_blocking`、`scan_globals_blocking`。
- async wrapper 使用 `tokio::task::spawn_blocking`，磁盘 I/O 和解析不会阻塞前台 async runtime。
- `WorldScanOptions` 控制线程、取消和进度回调。
- `WorldPipelineOptions` 进一步控制有界 pipeline 的队列深度、chunk batch 大小、
  subchunk decode worker 预算和进度间隔。字段为 0 时使用自动策略。
- 渲染专用 API 现在有独立快路径：
  `list_render_chunk_positions_blocking`、
  `list_render_chunk_positions_in_region_blocking`、
  `load_render_chunk_blocking`、`load_render_chunks_blocking` 和
  `load_render_region_blocking`。这些接口只读取渲染 chunk 所需记录，并支持有界并行。
- `parse_world_blocking(WorldParseOptions)` 是显式高级/离线接口，不是启动器默认路径。
- 公开 fallible API 返回 `bedrock_world::Result<T>`。应用侧应匹配
  `BedrockWorldError::kind()` 这样的稳定分类，而不是解析面向人的错误字符串。

更完整的 API 和测试说明见 [`docs/API.md`](docs/API.md) 与
[`docs/TESTING.md`](docs/TESTING.md)。

```rust
use bedrock_world::{
    read_level_dat, BedrockWorld, OpenOptions, WorldScanOptions, WorldThreadingOptions,
};

async fn inspect_world() -> bedrock_world::Result<()> {
    let level = read_level_dat("path/to/minecraftWorld/level.dat")?;
    println!("level.dat version={}", level.header.version);

    let world = BedrockWorld::open("path/to/minecraftWorld", OpenOptions::default()).await?;
    let players = world.list_players().await?;
    println!("players={}", players.len());

    let key_counts = world
        .classify_keys(WorldScanOptions {
            threading: WorldThreadingOptions::Auto,
            ..WorldScanOptions::default()
        })
        .await?;
    println!("key categories={}", key_counts.len());

    Ok(())
}
```

## 解析策略

| 策略 | Raw entries | Raw values | Subchunk indices | Actor resolution | 适用场景 |
| --- | ---: | ---: | ---: | --- | --- |
| `WorldParseOptions::summary()` | 否 | 否 | 只保留 counts | 引用到的 actor | UI summary、大型扫描 |
| `WorldParseOptions::structured()` | 选定 parsed entries | 否 | 只保留 counts | 引用到的 actor | 检查工具 |
| `WorldParseOptions::full_raw()` / `full()` | 是 | 是 | 完整 4096 indices | 全部 actor | 调试/离线分析 |

`WorldParseCategories` 控制是否解析 chunks、players、entities、block entities、items、
maps、villages、globals 和 key counts。summary 模式保留统计和结构化摘要，但避免 raw value 驻留。

## 性能模型

- 启动器只需要 `level.dat` 时应使用 `read_level_dat` 和 `write_level_dat_atomic`；
  这条路径只访问 `level.dat` 文件。
- `BedrockWorld::open` 是 lazy 的，DB 访问委托给 `bedrock-leveldb`。
- key 分类使用 key-only scan，不保留 value。
- 视口渲染应先使用 `list_render_chunk_positions_in_region_blocking` 或 async wrapper。
  它会用 key-only prefix scan 探测当前区域，跳过没有渲染记录的 chunk。
- chunk 解析使用 prefix scan 和 LevelDB native block cache；样本 chunk 读取不再做全 table 扫描。
- 默认世界扫描使用自动有界并行 table scan。确定性调试可使用 `WorldThreadingOptions::Single`。
- `RenderChunkLoadOptions::threading` 和 `RenderRegionLoadOptions::threading`
  控制 render chunk/region 并行加载。当外层渲染器已经有 worker pool 时，建议使用
  `Single` 避免嵌套过度并行。
- `RenderChunkLoadOptions::priority` 可以设置
  `RenderChunkPriority::DistanceFrom { chunk_x, chunk_z }`，优先加载当前视口中心附近。
  `RenderRegionData::stats` 会记录请求/命中 chunk、decoded subchunk、worker 数、
  queue wait 和总 load 时间。
- 长扫描可通过 `CancelFlag` 取消，并通过 `ProgressSink` 汇报进度。

### 迁移：全量 chunk scan 到视口渲染索引

旧版地图查看器通常等待全世界 chunk scan 完成后再渲染：

```rust
let all_chunks = world
    .list_chunk_positions_blocking(WorldScanOptions::default())?;
let visible = all_chunks
    .into_iter()
    .filter(|pos| viewport.contains(*pos))
    .collect::<Vec<_>>();
```

现在优先查询当前视口对应的渲染区域：

```rust
let visible = world.list_render_chunk_positions_in_region_blocking(
    bedrock_world::RenderChunkRegion {
        dimension,
        min_chunk_x,
        min_chunk_z,
        max_chunk_x,
        max_chunk_z,
    },
    WorldScanOptions {
        threading: WorldThreadingOptions::Auto,
        cancel: Some(cancel),
        progress: Some(progress),
        ..WorldScanOptions::default()
    },
)?;
```

随后只加载这些 chunk：

```rust
let chunks = world.load_render_chunks_blocking(
    visible,
    bedrock_world::RenderChunkLoadOptions {
        threading: WorldThreadingOptions::Fixed(4),
        priority: bedrock_world::RenderChunkPriority::DistanceFrom {
            chunk_x: viewport_center_x,
            chunk_z: viewport_center_z,
        },
        ..bedrock_world::RenderChunkLoadOptions::default()
    },
)?;
```

## Fixture 结果

`tests/fixtures/sample-bedrock-world` 是本地可选的大型 Bedrock 世界 fixture，
包含 native `.ldb` table 和 WAL 数据。它被 Git 忽略，因为真实世界体积很大，
并且可能包含玩家数据。缺少该目录时，fixture test 和大型 benchmark 会打印 skip
信息并正常通过。

```text
cargo test -p bedrock-world -- --nocapture

unit tests: 36 passed; 0 failed; finished in 0.03s
fixture test: 1 passed; 0 failed; finished in 15.85s

db.entries.count=4571643
db.entries.key_bytes=63624175
db.entries.value_bytes=8398184492
db.chunk.positions.count=237534
db.unknown_keys.first=[]
parsed.sample_chunk.pos=ChunkPos { x: 451, z: -457, dimension: End }
parsed.sample_chunk.records=10
parsed.sample_chunk.subchunks=5
parsed.sample_chunk.subchunk_storages=4
parsed.sample_chunk.palette_states=10
parsed.sample_chunk.block_entities=0
parsed.sample_chunk.biomes.records=1 storages=25
parsed.sample_chunk.errors=[]
players.count=290
```

## Benchmark 结果

最近一次本地运行：

```text
cargo bench -p bedrock-world

large_fixture.level_dat elapsed_ms=0 version=10 payload_len=2889
large_fixture.db.open_lazy elapsed_ms=1
large_fixture.classify_keys.single elapsed_ms=14097 entries=4571643 entries_per_sec=324287.69
large_fixture.players elapsed_ms=54 count=290
large_fixture.sample_chunk elapsed_ms=48 records=17 subchunks=9 block_entities=0 parse_errors=0

bedrock_world/level_dat/parse_synthetic            [357.45 ns 364.34 ns 374.54 ns]
bedrock_world/level_dat/read_fixture               [53.089 us 53.509 us 53.942 us]
bedrock_world/db/open_lazy                         [765.74 us 776.76 us 794.67 us]
bedrock_world/world/list_players                   [37.395 ms 37.947 ms 39.316 ms]
bedrock_world/subchunk/decode_palette_full_indices [35.651 us 35.934 us 36.196 us]
bedrock_world/subchunk/decode_palette_counts_only  [36.772 us 37.286 us 38.240 us]
bedrock_world/chunk/parse_fixture_chunk            [37.881 ms 38.514 ms 40.018 ms]
```

由于未安装 `gnuplot`，Criterion 使用 Plotters backend。Criterion measurement time
设置为 4 秒，这样较慢的 fixture benchmark 不会触发 short-sampling 警告。大型 fixture
harness 与 Criterion 分离，因为多百万 entry 扫描不应该在 microbenchmark 中反复运行。

## 完整度

| 范围 | 状态 |
| --- | --- |
| `level.dat` header、warning、原子写入 | 已实现 |
| Bedrock 小端 NBT 和连续 root | 已实现 |
| DB key 分类 | 已覆盖 chunk、player、actor、digp、map、village 和常见 global key |
| 旧 `LegacyTerrain` record | 已实现 83,200 字节 LevelDB 时代 terrain value 解析 |
| 旧 subchunk block array | 已实现 v0 和 v2-v7 pre-paletted `SubChunkPrefix` value |
| Subchunk v1/v8/v9 palette 解析 | 已实现 counts-only 和 full-indices 模式 |
| Data2D/Data3D biome summary | 已实现 |
| `digp -> actorprefix` actor 解析 | 已实现，可配置 |
| 玩家、实体、方块实体、物品栈 | 已提取通用字段 |
| 未知版本专属数据 | 按 retention mode raw 保留或计数 |
| 所有 chunk 版本的完整结构化编辑 | 未实现 |
| 地图图片生成 | 未实现 |

早于 LevelDB 的 Bedrock 世界使用 `chunks.dat` / `entities.dat` 之类文件，不是数据库世界；
这类文件导入目前不属于本 crate 的范围。

## 使用建议

- 不要在 UI 中调用 `parse_world_blocking(WorldParseOptions::full_raw())`；它是离线调试路径。
- 启动器元数据使用 `read_level_dat`，安全修改使用 `write_level_dat_atomic`。
- 只需要某一类数据时使用对应分类 API，避免不必要地解析实体、区块和 global records。
- 渲染器应先构造视口 `RenderChunkRegion`，再使用 render-index API。完整
  `list_chunk_positions_blocking` 保留给元数据、搜索和离线导出场景。
- 当前 GitHub 初始版本设置了 `publish = false`，因为 `bedrock-leveldb` 仍通过固定 Git
  revision 使用。正式发布 crates.io 前应先发布 `bedrock-leveldb`，再把本依赖切换为
  crates.io version。
