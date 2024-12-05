use std::sync::{Arc, OnceLock};

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_micropartition::MicroPartition;
use daft_table::Table;

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};

pub(crate) type CrossJoinStateBridgeRef = Arc<CrossJoinStateBridge>;

// TODO(Colin): rework into more generic broadcast bridge that can be used for both probe table and micropartition
pub(crate) struct CrossJoinStateBridge {
    inner: OnceLock<Arc<Vec<Table>>>,
    notify: tokio::sync::Notify,
}

impl CrossJoinStateBridge {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: OnceLock::new(),
            notify: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn set_state(&self, state: Arc<Vec<Table>>) {
        assert!(
            !self.inner.set(state).is_err(),
            "CrossJoinStateBridge should be set only once"
        );
        self.notify.notify_waiters();
    }

    pub(crate) async fn get_state(&self) -> Arc<Vec<Table>> {
        loop {
            if let Some(state) = self.inner.get() {
                return state.clone();
            }
            self.notify.notified().await;
        }
    }
}

struct CrossJoinCollectState(Option<Vec<Table>>);

impl BlockingSinkState for CrossJoinCollectState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct CrossJoinCollectSink {
    state_bridge: CrossJoinStateBridgeRef,
}

impl CrossJoinCollectSink {
    pub(crate) fn new(state_bridge: CrossJoinStateBridgeRef) -> Self {
        Self { state_bridge }
    }
}

impl BlockingSink for CrossJoinCollectSink {
    fn name(&self) -> &'static str {
        "CrossJoinCollectSink"
    }

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkSinkResult {
        if input.is_empty() {
            return Ok(BlockingSinkStatus::NeedMoreInput(state)).into();
        }

        runtime
            .spawn(async move {
                let cross_join_collect_state = state
                    .as_any_mut()
                    .downcast_mut::<CrossJoinCollectState>()
                    .expect("CrossJoinCollectSink should have CrossJoinCollectState");

                cross_join_collect_state
                    .0
                    .as_mut()
                    .expect("Collected tables should not be consumed before sink stage is done")
                    .extend(input.get_tables()?.iter().cloned());

                Ok(BlockingSinkStatus::NeedMoreInput(state))
            })
            .into()
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        _runtime: &RuntimeRef,
    ) -> BlockingSinkFinalizeResult {
        let mut state = states.into_iter().next().unwrap();
        let cross_join_collect_state = state
            .as_any_mut()
            .downcast_mut::<CrossJoinCollectState>()
            .expect("CrossJoinCollectSink should have CrossJoinCollectState");

        let tables = cross_join_collect_state
            .0
            .take()
            .expect("Cross join collect state should have tables before finalize is called");

        self.state_bridge.set_state(Arc::new(tables));
        Ok(None).into()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(CrossJoinCollectState(Some(Vec::new()))))
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
