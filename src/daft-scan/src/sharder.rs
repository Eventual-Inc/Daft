use core::fmt;
use std::hash::{Hash, Hasher};

use common_error::DaftResult;
use fnv::FnvHasher;
use serde::{Deserialize, Serialize};

use crate::ScanTaskRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ShardingStrategy {
    File,
}

impl fmt::Display for ShardingStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File => write!(f, "file"),
        }
    }
}

impl From<String> for ShardingStrategy {
    fn from(strategy: String) -> Self {
        match strategy.as_str() {
            "file" => Self::File,
            _ => unreachable!("Unsupported sharding strategy: {strategy}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Sharder {
    strategy: ShardingStrategy,
    world_size: usize,
    rank: usize,
}

impl Sharder {
    pub fn new(strategy: ShardingStrategy, world_size: usize, rank: usize) -> Self {
        debug_assert!(
            world_size > 0,
            "World size for sharding must be greater than zero"
        );
        debug_assert!(
            rank < world_size,
            "Rank must be less than the world size for sharding"
        );
        Self {
            strategy,
            world_size,
            rank,
        }
    }

    pub fn strategy(&self) -> &ShardingStrategy {
        &self.strategy
    }

    pub fn world_size(&self) -> usize {
        self.world_size
    }

    pub fn rank(&self) -> usize {
        self.rank
    }

    /// Maps a hash to a shard ID using modulo operation.
    fn shard_for_hash(&self, hash: u64) -> usize {
        (hash as usize) % self.world_size
    }

    /// Computes hash for any hashable item and returns which shard should handle it.
    fn shard_for_item<T: Hash>(&self, item: &T) -> usize {
        let hash = fnv_hash(item);
        self.shard_for_hash(hash)
    }

    /// Determines if the given hashable item should be handled by this shard.
    pub fn should_handle_item<T: Hash>(&self, item: &T) -> bool {
        self.shard_for_item(item) == self.rank
    }
}

impl fmt::Display for Sharder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Sharder(strategy={}, world_size={}, rank={})",
            self.strategy, self.world_size, self.rank
        )
    }
}

/// Computes FNV hash for any hashable item.
fn fnv_hash<T: Hash>(item: &T) -> u64 {
    let mut hasher = FnvHasher::default();
    item.hash(&mut hasher);
    hasher.finish()
}

/// Extension methods for `Sharder` that operate on concrete `ScanTaskRef` values.
pub trait SharderExt {
    fn shard_scan_tasks(&self, scan_tasks: &[ScanTaskRef]) -> DaftResult<Vec<ScanTaskRef>>;
}

impl SharderExt for Sharder {
    fn shard_scan_tasks(&self, scan_tasks: &[ScanTaskRef]) -> DaftResult<Vec<ScanTaskRef>> {
        match self.strategy() {
            ShardingStrategy::File => shard_scan_tasks_by_file(self, scan_tasks),
        }
    }
}

fn filter_scan_tasks_by_file(
    sharder: &Sharder,
    scan_tasks: &[ScanTaskRef],
) -> DaftResult<Vec<ScanTaskRef>> {
    let tasks: Vec<_> = scan_tasks
        .iter()
        .filter(|scan_task| {
            let paths = scan_task.get_file_paths();
            sharder.should_handle_item(&paths[0])
        })
        .cloned()
        .collect();
    Ok(tasks)
}

fn sort_scan_tasks_by_file(mut scan_tasks: Vec<ScanTaskRef>) -> Vec<ScanTaskRef> {
    scan_tasks.sort_by(|a, b| {
        let path_a = &a.get_file_paths()[0];
        let path_b = &b.get_file_paths()[0];
        path_a.cmp(path_b)
    });
    scan_tasks
}

fn shard_scan_tasks_by_file(
    sharder: &Sharder,
    scan_tasks: &[ScanTaskRef],
) -> DaftResult<Vec<ScanTaskRef>> {
    debug_assert!(
        scan_tasks
            .iter()
            .all(|task| task.get_file_paths().len() == 1),
        "All physical scan tasks have exactly one file path during logical plan optimization"
    );
    Ok(sort_scan_tasks_by_file(filter_scan_tasks_by_file(
        sharder, scan_tasks,
    )?))
}
