use std::sync::Arc;

use dashmap::DashMap;

use crate::pipeline::InputId;

/// The set of `input_id`s whose producers have been asked to stop, shared by
/// every node in one *subtree* of a pipeline.
///
/// A worker pipeline is reused across many flotilla tasks — keyed by plan
/// fingerprint in `NativeExecutor::run` — and each task arrives as its own
/// `input_id`. An operator that already has all the data it will ever need for
/// one of those inputs (a distributed `LIMIT` whose global counter reached
/// zero) cancels that `input_id` here, and the producers feeding it stop
/// generating data for it. The other `input_id`s riding the same pipeline are
/// untouched, which is why this is keyed per input and not the pipeline-wide
/// `CancellationToken` that `NativeExecutor` already owns.
///
/// The registry is scoped to a subtree rather than to the whole pipeline
/// because an operator may only cancel the sources that feed *it*. A
/// `distributed_limit` can end up as a subtree of a larger task plan —
/// `SwordfishTaskBuilder::combine_with` fuses both sides of a broadcast or
/// cross join into one plan — and cancelling by `input_id` alone would stop
/// the join's other side too, silently dropping rows. `StreamingSink`s that
/// opt in via [`StreamingSink::cancels_inputs`] get a fresh registry for their
/// own subtree; every other node forwards the one it was given.
///
/// Cancellation is cooperative and *advisory*: it never terminates a node, and
/// a producer that ignores it simply keeps working, which is the behavior that
/// predates this registry. That is what keeps the input's normal completion
/// path intact — a cancelled input still finishes through its own
/// `PipelineMessage::Flush`, so no bookkeeping has to learn about cancellation.
#[derive(Clone, Default)]
pub(crate) struct InputCancelRegistry {
    /// `DashMap` rather than a `Mutex<HashSet>` because producers poll this on
    /// every morsel while the cancelling sink writes to it from another task.
    inner: Arc<DashMap<InputId, ()>>,
}

impl InputCancelRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Asks the producers feeding `input_id` to stop generating data for it.
    /// Only a node that owns this registry may call this — see the type docs.
    pub(crate) fn cancel(&self, input_id: InputId) {
        self.inner.insert(input_id, ());
    }

    /// Polled by producers. Cheap enough to call per morsel.
    pub(crate) fn is_cancelled(&self, input_id: InputId) -> bool {
        self.inner.contains_key(&input_id)
    }

    /// Drops `input_id`'s entry once it has finished flowing through the
    /// subtree. Only the owning node may call this: clearing the flag while a
    /// sibling branch is still producing would let that branch resume.
    pub(crate) fn forget(&self, input_id: InputId) {
        self.inner.remove(&input_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_is_scoped_to_one_input() {
        let registry = InputCancelRegistry::new();
        registry.cancel(1);
        assert!(registry.is_cancelled(1));
        assert!(!registry.is_cancelled(2));
    }

    #[test]
    fn unknown_input_is_not_cancelled() {
        let registry = InputCancelRegistry::new();
        assert!(!registry.is_cancelled(42));
    }

    #[test]
    fn forget_removes_the_entry() {
        let registry = InputCancelRegistry::new();
        registry.cancel(3);
        registry.forget(3);
        assert!(!registry.is_cancelled(3));
    }

    /// Two subtrees must not see each other's cancellations — this is what
    /// keeps a `limit` fused under one side of a join from stopping the other.
    #[test]
    fn separate_registries_are_independent() {
        let left = InputCancelRegistry::new();
        let right = InputCancelRegistry::new();
        left.cancel(1);
        assert!(left.is_cancelled(1));
        assert!(!right.is_cancelled(1));
    }

    #[test]
    fn clones_share_state() {
        let registry = InputCancelRegistry::new();
        let clone = registry.clone();
        registry.cancel(9);
        assert!(clone.is_cancelled(9));
    }
}
