//! Worker-wide memory and spill resources, with per-pipeline accounting scopes.
//!
//! Configuration is read once per process. `DAFT_MEMORY_LIMIT` is the total
//! accounted budget in bytes (otherwise derived from system/container memory).
//! `DAFT_SPILL_MEMORY_LIMIT` reserves part of it for spill encoding; by default
//! this is the smaller of 64 MiB and one eighth of the total budget.
//! `DAFT_SPILL_DIRS` is a platform path-list, defaulting to `daft-spill` under
//! the system temporary directory. `DAFT_SPILL_IO_CONCURRENCY` bounds concurrent
//! blocking file operations across pipelines (default: 2).
//!
//! Only explicit reservations consume this budget. Upstream buffers, Python
//! heaps and operator state that has not adopted reservations are not covered.

use std::sync::{Arc, OnceLock};

use common_error::{DaftError, DaftResult};
use common_system_info::SystemInfo;
use daft_local_plan::InputId;
pub(crate) use daft_memory::{MemoryManager, MemoryPool, MemoryPoolKind};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::spilling::{DEFAULT_SPILL_MEMORY_BYTES, SpillError, SpillIoRuntime, SpillManager};

static MEMORY_MANAGER: OnceLock<Result<Arc<MemoryManager>, String>> = OnceLock::new();
static SPILL_IO_RUNTIME: OnceLock<Arc<SpillIoRuntime>> = OnceLock::new();

fn custom_memory_limit() -> Option<u64> {
    std::env::var("DAFT_MEMORY_LIMIT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

pub(crate) fn get_or_init_memory_manager() -> DaftResult<&'static Arc<MemoryManager>> {
    MEMORY_MANAGER
        .get_or_init(|| {
            let limit = custom_memory_limit()
                .unwrap_or_else(|| SystemInfo::default().calculate_total_memory());
            let spill_bytes = match std::env::var("DAFT_SPILL_MEMORY_LIMIT") {
                Ok(value) => value.parse::<u64>().map_err(|_| {
                    "DAFT_SPILL_MEMORY_LIMIT must be an integer number of bytes".to_string()
                })?,
                Err(std::env::VarError::NotPresent) => DEFAULT_SPILL_MEMORY_BYTES.min(limit / 8),
                Err(error) => return Err(error.to_string()),
            };
            MemoryManager::with_spill_reserve(limit, spill_bytes)
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| DaftError::ComputeError(error.clone()))
}

pub(crate) fn create_spill_manager(pipeline_id: impl ToString) -> Result<SpillManager, SpillError> {
    let spill_pool = get_or_init_memory_manager()
        .map_err(|error| SpillError::Memory(error.to_string()))?
        .spill_pool();
    let io_runtime = SPILL_IO_RUNTIME.get_or_init(|| {
        let configured_directories: Vec<_> = std::env::var_os("DAFT_SPILL_DIRS")
            .map(|value| {
                std::env::split_paths(&value)
                    .filter(|path| !path.as_os_str().is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let directories = if configured_directories.is_empty() {
            vec![std::env::temp_dir().join("daft-spill")]
        } else {
            configured_directories
        };
        let io_concurrency = std::env::var("DAFT_SPILL_IO_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2);
        SpillIoRuntime::new(directories, io_concurrency, spill_pool)
            .expect("the normalized spill directory list is non-empty")
    });
    Ok(SpillManager::for_execution(io_runtime.clone(), pipeline_id))
}

#[derive(Debug)]
pub(crate) struct PipelineMemoryContext {
    pipeline_pool: Arc<MemoryPool>,
    node_shared_pools: DashMap<usize, Arc<MemoryPool>>,
    input_pools: DashMap<InputId, Arc<MemoryPool>>,
}

impl PipelineMemoryContext {
    pub(crate) fn new(pipeline_pool: Arc<MemoryPool>) -> Self {
        debug_assert_eq!(pipeline_pool.kind(), MemoryPoolKind::Pipeline);
        Self {
            pipeline_pool,
            node_shared_pools: DashMap::new(),
            input_pools: DashMap::new(),
        }
    }

    pub(crate) fn pipeline_pool(&self) -> Arc<MemoryPool> {
        self.pipeline_pool.clone()
    }

    pub(crate) fn node_shared_pool(&self, node_id: usize, node_name: &str) -> Arc<MemoryPool> {
        match self.node_shared_pools.entry(node_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry
                .insert(self.pipeline_pool.child(
                    format!("node-{node_id}-{node_name}"),
                    MemoryPoolKind::NodeShared,
                    self.pipeline_pool.limit_bytes(),
                ))
                .clone(),
        }
    }

    pub(crate) fn input_pool(&self, input_id: InputId) -> Arc<MemoryPool> {
        match self.input_pools.entry(input_id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => entry
                .insert(self.pipeline_pool.child(
                    format!("input-{input_id}"),
                    MemoryPoolKind::Input,
                    self.pipeline_pool.limit_bytes(),
                ))
                .clone(),
        }
    }

    pub(crate) fn operator_pool(
        &self,
        input_id: InputId,
        node_id: usize,
        node_name: &str,
    ) -> Arc<MemoryPool> {
        let input_pool = self.input_pool(input_id);
        input_pool.child(
            format!("node-{node_id}-{node_name}"),
            MemoryPoolKind::Operator,
            input_pool.limit_bytes(),
        )
    }

    pub(crate) fn finish_input(&self, input_id: InputId) {
        self.input_pools.remove(&input_id);
    }
}
