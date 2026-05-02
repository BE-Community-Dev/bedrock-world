//! Storage abstraction used by [`crate::BedrockWorld`].
//!
//! The trait in this module keeps world parsing independent from a concrete
//! LevelDB implementation. [`MemoryStorage`] is useful for tests and synthetic
//! tools, while [`BedrockLevelDbStorage`](crate::BedrockLevelDbStorage) adapts
//! the `bedrock-leveldb` crate.

use crate::error::{BedrockWorldError, Result};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageEntry {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

#[derive(Debug, Clone)]
pub struct StorageReadOptions {
    pub threading: StorageThreadingOptions,
    pub scan_mode: StorageScanMode,
    pub pipeline: StoragePipelineOptions,
    pub cancel: Option<StorageCancelFlag>,
    pub progress: Option<StorageProgressSink>,
}

impl Default for StorageReadOptions {
    fn default() -> Self {
        Self {
            threading: StorageThreadingOptions::Auto,
            scan_mode: StorageScanMode::ParallelTables,
            pipeline: StoragePipelineOptions::default(),
            cancel: None,
            progress: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoragePipelineOptions {
    pub queue_depth: usize,
    pub table_batch_size: usize,
    pub progress_interval: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageThreadingOptions {
    #[default]
    Auto,
    Fixed(usize),
    Single,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageScanMode {
    #[default]
    Sequential,
    ParallelTables,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageVisitorControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageScanOutcome {
    pub visited: usize,
    pub bytes_read: usize,
    pub stopped: bool,
    pub tables_scanned: usize,
    pub worker_threads: usize,
    pub queue_wait_ms: u128,
    pub cancel_checks: usize,
}

impl StorageScanOutcome {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            visited: 0,
            bytes_read: 0,
            stopped: false,
            tables_scanned: 0,
            worker_threads: 0,
            queue_wait_ms: 0,
            cancel_checks: 0,
        }
    }

    pub fn record(&mut self, value_len: usize) {
        self.visited = self.visited.saturating_add(1);
        self.bytes_read = self.bytes_read.saturating_add(value_len);
    }

    pub fn merge(&mut self, other: Self) {
        self.visited = self.visited.saturating_add(other.visited);
        self.bytes_read = self.bytes_read.saturating_add(other.bytes_read);
        self.stopped |= other.stopped;
        self.tables_scanned = self.tables_scanned.saturating_add(other.tables_scanned);
        self.worker_threads = self.worker_threads.max(other.worker_threads);
        self.queue_wait_ms = self.queue_wait_ms.saturating_add(other.queue_wait_ms);
        self.cancel_checks = self.cancel_checks.saturating_add(other.cancel_checks);
    }
}

#[derive(Debug, Clone, Default)]
pub struct StorageCancelFlag(Arc<AtomicBool>);

impl StorageCancelFlag {
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
pub struct StorageProgressSink {
    inner: Arc<dyn Fn(StorageScanProgress) + Send + Sync>,
}

impl std::fmt::Debug for StorageProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageProgressSink")
            .finish_non_exhaustive()
    }
}

impl StorageProgressSink {
    #[must_use]
    pub fn new(callback: impl Fn(StorageScanProgress) + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(callback),
        }
    }

    pub fn emit(&self, progress: StorageScanProgress) {
        (self.inner)(progress);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageScanProgress {
    pub entries_seen: usize,
    pub bytes_read: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageBatch {
    ops: Vec<StorageOp>,
}

impl StorageBatch {
    #[must_use]
    pub const fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.ops.push(StorageOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Bytes>) {
        self.ops.push(StorageOp::Delete { key: key.into() });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    #[must_use]
    pub fn ops(&self) -> &[StorageOp] {
        &self.ops
    }
}

pub trait WorldStorage: Send + Sync {
    /// Looks up a raw value by exact key.
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    /// Writes a raw key/value pair.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Deletes a raw key.
    fn delete(&self, key: &[u8]) -> Result<()>;
    /// Visits keys without forcing value materialization when the backend can
    /// support key-only scans.
    fn for_each_key(
        &self,
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome>;
    /// Visits key/value records whose key starts with `prefix`.
    fn for_each_prefix(
        &self,
        prefix: &[u8],
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8], &Bytes) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome>;
    /// Visits keys whose key starts with `prefix` without requiring value
    /// materialization when the backend can support key-only scans.
    fn for_each_prefix_key(
        &self,
        prefix: &[u8],
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome> {
        self.for_each_prefix(prefix, options, &mut |key, _value| visitor(key))
    }
    /// Visits all key/value records.
    fn for_each_entry(
        &self,
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8], &Bytes) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome> {
        self.for_each_prefix(b"", options, visitor)
    }
    /// Applies a batch of raw operations atomically when supported by the backend.
    fn write_batch(&self, batch: &StorageBatch) -> Result<()>;
    /// Flushes pending writes to durable storage when supported by the backend.
    fn flush(&self) -> Result<()>;
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    values: Arc<RwLock<BTreeMap<Vec<u8>, Bytes>>>,
}

impl MemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl WorldStorage for MemoryStorage {
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let values = self.values.read().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        Ok(values.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut values = self.values.write().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        values.insert(key.to_vec(), Bytes::copy_from_slice(value));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut values = self.values.write().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        values.remove(key);
        Ok(())
    }

    fn for_each_key(
        &self,
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome> {
        let values = self.values.read().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        let mut outcome = StorageScanOutcome::empty();
        for (key, value) in values.iter() {
            check_cancelled(&options)?;
            outcome.record(value.len());
            if visitor(key)? == StorageVisitorControl::Stop {
                outcome.stopped = true;
                return Ok(outcome);
            }
            emit_progress(&options, outcome);
        }
        Ok(outcome)
    }

    fn for_each_prefix(
        &self,
        prefix: &[u8],
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8], &Bytes) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome> {
        let values = self.values.read().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        let mut outcome = StorageScanOutcome::empty();
        for (key, value) in values
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
        {
            check_cancelled(&options)?;
            outcome.record(value.len());
            if visitor(key, value)? == StorageVisitorControl::Stop {
                outcome.stopped = true;
                return Ok(outcome);
            }
            emit_progress(&options, outcome);
        }
        Ok(outcome)
    }

    fn for_each_prefix_key(
        &self,
        prefix: &[u8],
        options: StorageReadOptions,
        visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
    ) -> Result<StorageScanOutcome> {
        let values = self.values.read().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        let mut outcome = StorageScanOutcome::empty();
        for (key, value) in values
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
        {
            check_cancelled(&options)?;
            outcome.record(value.len());
            if visitor(key)? == StorageVisitorControl::Stop {
                outcome.stopped = true;
                return Ok(outcome);
            }
            emit_progress(&options, outcome);
        }
        Ok(outcome)
    }

    fn write_batch(&self, batch: &StorageBatch) -> Result<()> {
        let mut values = self.values.write().map_err(|_| {
            BedrockWorldError::ConcurrentWrite("memory storage poisoned".to_string())
        })?;
        for op in batch.ops() {
            match op {
                StorageOp::Put { key, value } => {
                    values.insert(key.to_vec(), value.clone());
                }
                StorageOp::Delete { key } => {
                    values.remove(key.as_ref());
                }
            }
        }
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub mod backend {
    use super::*;

    #[cfg(feature = "backend-bedrock-leveldb")]
    pub struct BedrockLevelDbStorage {
        db: bedrock_leveldb::Db,
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    impl BedrockLevelDbStorage {
        /// Opens a `LevelDB` directory for read/write access.
        pub fn open(path: impl AsRef<Path>) -> Result<Self> {
            Self::open_inner(path, false)
        }

        /// Opens a `LevelDB` directory without allowing backend writes.
        pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
            Self::open_inner(path, true)
        }

        fn open_inner(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
            let path = path.as_ref().to_path_buf();
            if !path.exists() {
                return Err(BedrockWorldError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("LevelDB path not found: {}", path.display()),
                )));
            }
            let options = bedrock_leveldb::OpenOptions {
                read_only,
                create_if_missing: false,
                error_if_exists: false,
                paranoid_checks: true,
                compression_policy: bedrock_leveldb::CompressionPolicy::Zlib,
                cache_size: 64 * 1024 * 1024,
                write_buffer_size: 4 * 1024 * 1024,
            };
            let db = bedrock_leveldb::Db::open(path, options).map_err(map_leveldb_error)?;
            Ok(Self { db })
        }
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    impl WorldStorage for BedrockLevelDbStorage {
        fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.db.get(key).map_err(map_leveldb_error)
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
            self.db
                .put(
                    Bytes::copy_from_slice(key),
                    Bytes::copy_from_slice(value),
                    bedrock_leveldb::WriteOptions::default(),
                )
                .map_err(map_leveldb_error)
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.db
                .delete(
                    Bytes::copy_from_slice(key),
                    bedrock_leveldb::WriteOptions::default(),
                )
                .map_err(map_leveldb_error)
        }

        fn for_each_key(
            &self,
            options: StorageReadOptions,
            visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
        ) -> Result<StorageScanOutcome> {
            let read_options = to_leveldb_read_options(options);
            let mut visitor_error = None;
            let scan_result = self
                .db
                .for_each_key(read_options, |key| match visitor(key) {
                    Ok(StorageVisitorControl::Continue) => {
                        Ok(bedrock_leveldb::VisitorControl::Continue)
                    }
                    Ok(StorageVisitorControl::Stop) => Ok(bedrock_leveldb::VisitorControl::Stop),
                    Err(error) => {
                        visitor_error = Some(error);
                        Ok(bedrock_leveldb::VisitorControl::Stop)
                    }
                });
            match (scan_result, visitor_error) {
                (_, Some(error)) => Err(error),
                (Ok(outcome), None) => Ok(to_storage_outcome(outcome)),
                (Err(error), None) => Err(map_leveldb_error(error)),
            }
        }

        fn for_each_prefix(
            &self,
            prefix: &[u8],
            options: StorageReadOptions,
            visitor: &mut (dyn FnMut(&[u8], &Bytes) -> Result<StorageVisitorControl> + Send),
        ) -> Result<StorageScanOutcome> {
            let read_options = to_leveldb_read_options(options);
            let mut visitor_error = None;
            let scan_result = self.db.for_each_prefix(prefix, read_options, |key, value| {
                match visitor(key, value) {
                    Ok(StorageVisitorControl::Continue) => {
                        Ok(bedrock_leveldb::VisitorControl::Continue)
                    }
                    Ok(StorageVisitorControl::Stop) => Ok(bedrock_leveldb::VisitorControl::Stop),
                    Err(error) => {
                        visitor_error = Some(error);
                        Ok(bedrock_leveldb::VisitorControl::Stop)
                    }
                }
            });
            match (scan_result, visitor_error) {
                (_, Some(error)) => Err(error),
                (Ok(outcome), None) => Ok(to_storage_outcome(outcome)),
                (Err(error), None) => Err(map_leveldb_error(error)),
            }
        }

        fn for_each_prefix_key(
            &self,
            prefix: &[u8],
            options: StorageReadOptions,
            visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
        ) -> Result<StorageScanOutcome> {
            let read_options = to_leveldb_read_options(options);
            let mut visitor_error = None;
            let scan_result =
                self.db
                    .for_each_prefix_key(prefix, read_options, |key| match visitor(key) {
                        Ok(StorageVisitorControl::Continue) => {
                            Ok(bedrock_leveldb::VisitorControl::Continue)
                        }
                        Ok(StorageVisitorControl::Stop) => {
                            Ok(bedrock_leveldb::VisitorControl::Stop)
                        }
                        Err(error) => {
                            visitor_error = Some(error);
                            Ok(bedrock_leveldb::VisitorControl::Stop)
                        }
                    });
            match (scan_result, visitor_error) {
                (_, Some(error)) => Err(error),
                (Ok(outcome), None) => Ok(to_storage_outcome(outcome)),
                (Err(error), None) => Err(map_leveldb_error(error)),
            }
        }

        fn write_batch(&self, batch: &StorageBatch) -> Result<()> {
            let mut db_batch = bedrock_leveldb::WriteBatch::new();
            for op in batch.ops() {
                match op {
                    StorageOp::Put { key, value } => db_batch.put(key.clone(), value.clone()),
                    StorageOp::Delete { key } => db_batch.delete(key.clone()),
                }
            }
            self.db
                .write(db_batch, bedrock_leveldb::WriteOptions::default())
                .map_err(map_leveldb_error)
        }

        fn flush(&self) -> Result<()> {
            self.db.flush().map_err(map_leveldb_error)
        }
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    fn map_leveldb_error(error: bedrock_leveldb::LevelDbError) -> BedrockWorldError {
        match error.kind() {
            bedrock_leveldb::ErrorKind::Cancelled => BedrockWorldError::Cancelled {
                operation: "LevelDB scan",
            },
            bedrock_leveldb::ErrorKind::ReadOnly => BedrockWorldError::ReadOnly,
            _ => BedrockWorldError::LevelDb(error.to_string()),
        }
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    fn to_leveldb_read_options(options: StorageReadOptions) -> bedrock_leveldb::ReadOptions {
        bedrock_leveldb::ReadOptions {
            checksum: bedrock_leveldb::ChecksumMode::Inherit,
            cache_policy: bedrock_leveldb::CachePolicy::Use,
            threading: match options.threading {
                StorageThreadingOptions::Auto => bedrock_leveldb::ThreadingOptions::Auto,
                StorageThreadingOptions::Fixed(threads) => {
                    bedrock_leveldb::ThreadingOptions::Fixed(threads)
                }
                StorageThreadingOptions::Single => bedrock_leveldb::ThreadingOptions::Single,
            },
            scan_mode: match options.scan_mode {
                StorageScanMode::Sequential => bedrock_leveldb::ScanMode::Sequential,
                StorageScanMode::ParallelTables => bedrock_leveldb::ScanMode::ParallelTables,
            },
            pipeline: bedrock_leveldb::ScanPipelineOptions {
                queue_depth: options.pipeline.queue_depth,
                table_batch_size: options.pipeline.table_batch_size,
                progress_interval: options.pipeline.progress_interval,
            },
            cancel: options
                .cancel
                .map(|cancel| bedrock_leveldb::ScanCancelFlag::from_shared(cancel.0)),
            progress: options.progress.map(|progress| {
                bedrock_leveldb::ScanProgressSink::new(move |db_progress| {
                    progress.emit(StorageScanProgress {
                        entries_seen: db_progress.visited,
                        bytes_read: db_progress.bytes_read,
                    });
                })
            }),
        }
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    const fn to_storage_outcome(outcome: bedrock_leveldb::ScanOutcome) -> StorageScanOutcome {
        StorageScanOutcome {
            visited: outcome.visited,
            bytes_read: outcome.bytes_read,
            stopped: outcome.stopped,
            tables_scanned: outcome.tables_scanned,
            worker_threads: outcome.worker_threads,
            queue_wait_ms: outcome.queue_wait_ms,
            cancel_checks: outcome.cancel_checks,
        }
    }

    #[cfg(not(feature = "backend-bedrock-leveldb"))]
    #[derive(Debug, Clone, Copy)]
    pub struct BedrockLevelDbStorage;

    #[cfg(not(feature = "backend-bedrock-leveldb"))]
    impl BedrockLevelDbStorage {
        /// Returns an error because the LevelDB backend feature is disabled.
        pub fn open(_path: impl AsRef<Path>) -> Result<Self> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        /// Returns an error because the LevelDB backend feature is disabled.
        pub fn open_read_only(_path: impl AsRef<Path>) -> Result<Self> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }
    }

    #[cfg(not(feature = "backend-bedrock-leveldb"))]
    impl WorldStorage for BedrockLevelDbStorage {
        fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn delete(&self, _key: &[u8]) -> Result<()> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn for_each_key(
            &self,
            _options: StorageReadOptions,
            _visitor: &mut (dyn FnMut(&[u8]) -> Result<StorageVisitorControl> + Send),
        ) -> Result<StorageScanOutcome> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn for_each_prefix(
            &self,
            _prefix: &[u8],
            _options: StorageReadOptions,
            _visitor: &mut (dyn FnMut(&[u8], &Bytes) -> Result<StorageVisitorControl> + Send),
        ) -> Result<StorageScanOutcome> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn write_batch(&self, _batch: &StorageBatch) -> Result<()> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }

        fn flush(&self) -> Result<()> {
            Err(BedrockWorldError::LevelDb(
                "backend-bedrock-leveldb feature is disabled".to_string(),
            ))
        }
    }
}

fn check_cancelled(options: &StorageReadOptions) -> Result<()> {
    if options
        .cancel
        .as_ref()
        .is_some_and(StorageCancelFlag::is_cancelled)
    {
        return Err(BedrockWorldError::Cancelled {
            operation: "storage scan",
        });
    }
    Ok(())
}

fn emit_progress(options: &StorageReadOptions, outcome: StorageScanOutcome) {
    if let Some(progress) = &options.progress {
        progress.emit(StorageScanProgress {
            entries_seen: outcome.visited,
            bytes_read: outcome.bytes_read,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "backend-bedrock-leveldb")]
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn memory_storage_scans_prefix_without_copying_values() {
        let storage = MemoryStorage::new();
        storage.put(b"abc1", b"one").expect("put");
        storage.put(b"abc2", b"two").expect("put");
        storage.put(b"abd", b"three").expect("put");

        let mut entries = Vec::new();
        storage
            .for_each_prefix(b"abc", StorageReadOptions::default(), &mut |key, value| {
                entries.push(StorageEntry {
                    key: Bytes::copy_from_slice(key),
                    value: value.clone(),
                });
                Ok(StorageVisitorControl::Continue)
            })
            .expect("scan");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, Bytes::from_static(b"abc1"));
        assert_eq!(entries[1].value, Bytes::from_static(b"two"));
    }

    #[cfg(feature = "backend-bedrock-leveldb")]
    #[test]
    fn bedrock_leveldb_storage_roundtrips_raw_records() {
        let path = std::env::temp_dir().join(format!(
            "bedrock-world-storage-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create");
        drop(
            bedrock_leveldb::Db::open(&path, bedrock_leveldb::OpenOptions::default())
                .expect("initialize"),
        );

        let storage = backend::BedrockLevelDbStorage::open(&path).expect("open");
        storage.put(b"player_1", b"one").expect("put");
        storage.put(b"player_2", b"two").expect("put");
        storage.flush().expect("flush");

        let reopened = backend::BedrockLevelDbStorage::open(&path).expect("reopen");
        assert_eq!(
            reopened.get(b"player_1").expect("get"),
            Some(Bytes::from_static(b"one"))
        );
        let mut player_count = 0;
        reopened
            .for_each_prefix(
                b"player_",
                StorageReadOptions::default(),
                &mut |_key, _value| {
                    player_count += 1;
                    Ok(StorageVisitorControl::Continue)
                },
            )
            .expect("scan");
        assert_eq!(player_count, 2);

        std::fs::remove_dir_all(path).expect("cleanup");
    }
}
