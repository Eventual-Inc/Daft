//! Memory budgets and hierarchical accounting for Daft execution.
//!
//! A [`MemoryManager`] splits a worker budget between execution and spill workspace.
//! Execution pools form an accounting tree: reservations charge the leaf and all its
//! ancestors atomically. A [`MemoryPermit`] represents reserved capacity, not an
//! allocation; callers retain it alongside their data and drop the data before
//! releasing the permit. This does not track the allocator or impose an RSS limit.
//!
//! When a reservation cannot fit, the manager sends requests to registered
//! [`MemoryReleaseTarget`]s. The execution framework polls these endpoints and
//! moves idle operator state into a release task. The manager never accesses
//! operator state directly. Spill workspace has an independent wait domain so
//! reclaiming execution memory does not recursively require more reclamation.

mod manager;
mod pool;
mod release;

pub use manager::MemoryManager;
pub use pool::{MemoryError, MemoryPermit, MemoryPool, MemoryPoolKind};
pub use release::{
    MemoryReleaseError, MemoryReleaseOutcome, MemoryReleaseRequest, MemoryReleaseTarget,
};
