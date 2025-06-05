use core::fmt;
use std::hash::{Hash, Hasher};

use common_error::{DaftError, DaftResult};
use fnv::FnvHasher;
use serde::{Deserialize, Serialize};

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

    fn shard_for_hash(&self, hash: u64) -> usize {
        (hash as usize) % self.world_size
    }

    /// Determines if the given hash should be handled by this shard.
    pub fn should_handle_hash(&self, hash: u64) -> bool {
        self.shard_for_hash(hash) == self.rank
    }

    /// Computes hash for any hashable item and returns which shard should handle it.
    /// By default, we use FNV hash.
    pub fn shard_for_item<T: Hash>(&self, item: &T) -> usize {
        let hash = fnv_hash(item);
        self.shard_for_hash(hash)
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
