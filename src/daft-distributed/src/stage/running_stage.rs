use std::{
    pin::Pin,
    task::{Context, Poll},
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use futures::{Stream, StreamExt};

use super::StageContext;
use crate::{
    channel::{create_channel, Receiver},
    pipeline_node::{PipelineOutput, RunningPipelineNode},
    runtime::JoinSet,
};

#[derive(Debug)]
enum RunningStageState {
    // Running: Stage is running and we are waiting for the result from the receiver
    Running(Receiver<PartitionRef>, Option<StageContext>),
    // Finishing: No more results will be produced, and we are waiting for the joinset to finish
    Finishing(JoinSet<DaftResult<()>>),
    // Finished: No more results will be produced, and the joinset has finished
    Finished,
}

#[allow(dead_code)]
pub(crate) struct RunningStage {
    running_stage_state: RunningStageState,
}

impl RunningStage {
    pub fn new(result_materializer: Receiver<PartitionRef>, stage_context: StageContext) -> Self {
        Self {
            running_stage_state: RunningStageState::Running(
                result_materializer,
                Some(stage_context),
            ),
        }
    }
}

impl Stream for RunningStage {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        fn poll_inner(
            state: &mut RunningStageState,
            cx: &mut Context<'_>,
        ) -> Option<Poll<Option<DaftResult<PartitionRef>>>> {
            match state {
                // Running: Stage is running and we are waiting for the result from the receiver
                RunningStageState::Running(result_receiver, stage_context) => {
                    match result_receiver.poll_recv(cx) {
                        // Received a result from the receiver
                        Poll::Ready(Some(result)) => Some(Poll::Ready(Some(Ok(result)))),
                        // No more results will be produced, transition to finishing state where we wait for the joinset to finish
                        Poll::Ready(None) => {
                            let (task_dispatcher_handle, joinset) = stage_context
                                .take()
                                .expect("StageContext should exist")
                                .into_inner();
                            // No more tasks to dispatch, drop the handle
                            drop(task_dispatcher_handle);
                            *state = RunningStageState::Finishing(joinset);
                            None
                        }
                        // Still waiting for a result from the receiver
                        Poll::Pending => Some(Poll::Pending),
                    }
                }
                // Finishing: No more results will be produced, and we are waiting for the joinset to finish
                RunningStageState::Finishing(joinset) => match joinset.poll_join_next(cx) {
                    // Received a result from the joinset
                    Poll::Ready(Some(result)) => match result {
                        Ok(Ok(())) => None,
                        Ok(Err(e)) => Some(Poll::Ready(Some(Err(e)))),
                        Err(e) => Some(Poll::Ready(Some(Err(DaftError::External(e.into()))))),
                    },
                    // Joinset is empty, transition to finished state
                    Poll::Ready(None) => {
                        *state = RunningStageState::Finished;
                        None
                    }
                    // Still waiting for a result from the joinset
                    Poll::Pending => Some(Poll::Pending),
                },
                // Finished: No more results will be produced, and the joinset has finished
                RunningStageState::Finished => Some(Poll::Ready(None)),
            }
        }

        loop {
            if let Some(poll) = poll_inner(&mut self.running_stage_state, cx) {
                return poll;
            }
        }
    }
}
