use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use futures::{stream::FuturesUnordered, Future, StreamExt};

use crate::{
    compute::{
        partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
        tree::{
            task_tree_to_state_tree, topological_sort, PartitionTaskNode, PartitionTaskState,
            RunningTask, SubmittableTask,
        },
    },
    executor::Executor,
};

use super::channel::OutputChannel;

#[derive(Debug)]
pub struct StreamingPartitionTaskScheduler<T: PartitionRef, E: Executor<T>, O: OutputChannel<T>> {
    state_root: Rc<PartitionTaskState<T>>,
    sorted_state: Vec<Rc<PartitionTaskState<T>>>,
    inflight_tasks: HashMap<usize, RunningTask<T>>,
    inflight_op_task_count: Vec<usize>,
    output_channel: O,
    // TODO(Clark): Abstract out into pluggable backpressure policy.
    max_output_queue_size: Option<usize>,
    // TODO(Clark): Add per-op resource utilization tracking.
    executor: Arc<E>,
}

impl<T: PartitionRef, E: Executor<T>, O: OutputChannel<T>>
    StreamingPartitionTaskScheduler<T, E, O>
{
    fn from_state(
        state_root: Rc<PartitionTaskState<T>>,
        output_channel: O,
        max_output_queue_size: Option<usize>,
        executor: Arc<E>,
    ) -> Self {
        // TODO(Clark): Defer topological sorting of state tree creation until execution?
        let sorted_state = topological_sort(state_root.clone());
        let max_op_id = sorted_state.iter().map(|x| x.op_id()).max().unwrap();
        Self {
            state_root,
            sorted_state,
            inflight_tasks: HashMap::new(),
            inflight_op_task_count: vec![0; max_op_id + 1],
            output_channel,
            max_output_queue_size,
            executor,
        }
    }

    pub fn new(
        task_tree_root: PartitionTaskNode,
        mut leaf_inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: O,
        max_output_queue_size: Option<usize>,
        executor: Arc<E>,
    ) -> Self {
        // TODO(Clark): Defer state tree creation until execution?
        log::debug!(
            "Building task tree state on {} leaf inputs: ",
            leaf_inputs.len()
        );
        // TODO(Clark): Support streaming inputs via an input channel receiver that's selected over in scheduling loop.
        let state_root = task_tree_to_state_tree::<T>(task_tree_root, &mut leaf_inputs);
        assert!(
            leaf_inputs.is_empty(),
            "leaf inputs should be empty, but has {} left",
            leaf_inputs.len()
        );
        log::debug!(
            "Built task tree state with {} inputs: ",
            state_root.num_queued_inputs()
        );
        Self::from_state(state_root, output_channel, max_output_queue_size, executor)
    }

    fn num_queued_inputs(&self) -> usize {
        self.sorted_state
            .iter()
            .map(|node| node.num_queued_inputs())
            .sum()
    }

    fn has_inputs(&self) -> bool {
        self.num_queued_inputs() > 0
    }

    fn submittable_unordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        self.sorted_state.iter().flat_map(|task_state| {
            // TODO(Clark): Lift this to admissible filtering?
            if let Some(max_size) = self.max_output_queue_size
                && task_state.num_queued_outputs() > max_size
            {
                return Box::new(std::iter::empty())
                    as Box<dyn Iterator<Item = SubmittableTask<T>>>;
            }
            Box::new(task_state.clone().all_unordered_submittable_tasks())
        })
    }

    fn submittable_ordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        self.sorted_state.iter().filter_map(|task_state| {
            // TODO(Clark): Lift this to admissible filtering?
            if let Some(max_size) = self.max_output_queue_size
                && task_state.num_queued_outputs() > max_size
            {
                return None;
            }
            task_state.clone().next_in_order_submittable_task()
        })
    }

    fn schedule_next_admissible(&self) -> Option<SubmittableTask<T>> {
        // TODO(Clark): Use submittable_unordered to mitigate head-of-line blocking.
        let submittable = self.submittable_ordered().collect::<Vec<_>>();
        log::debug!(
            "Trying to schedule next admissible: capacity = {:?}, utilization = {:?}, submittable = {:?}",
            self.executor.current_capacity(),
            self.executor.current_utilization(),
            submittable.iter().map(|t| (t.op_name(), t.resource_request())).collect::<Vec<_>>()
        );
        let admissible = submittable
            .into_iter()
            .filter(|t| self.executor.can_admit(t.resource_request()));
        // TODO(Clark): Use a better metric than output queue size.
        // TODO(Clark): Prioritize metadata-only ops.
        admissible.min_by(|x, y| x.num_queued_outputs().cmp(&y.num_queued_outputs()))
    }

    fn schedule_next_submittable(&self) -> Option<SubmittableTask<T>> {
        // TODO(Clark): Use submittable_unordered to mitigate head-of-line blocking.
        let submittable = self.submittable_ordered();
        // TODO(Clark): Return the most admissible task (smallest resource capacity violation).
        submittable.min_by(|x, y| x.num_queued_outputs().cmp(&y.num_queued_outputs()))
    }

    fn submit_task(
        &mut self,
        task: SubmittableTask<T>,
    ) -> impl Future<Output = DaftResult<(usize, Vec<T>)>> {
        let (task, running_task) = task.finalize_for_submission();
        self.inflight_op_task_count[running_task.op_id()] += 1;
        let task_id = task.task_id();
        log::debug!(
            "Submitting task for op {} with ID = {}.",
            running_task.node.op_name(),
            task_id
        );
        self.inflight_tasks.insert(task_id, running_task);
        let e = self.executor.clone();
        async move { e.submit_task(task).await }
    }

    pub async fn execute(mut self) {
        let mut running_task_futures = FuturesUnordered::new();
        let mut num_scheduling_loop_runs = 0;
        let mut num_tasks_run = 0;
        let mut shutdown = false;
        while !shutdown && (!running_task_futures.is_empty() || self.has_inputs()) {
            num_scheduling_loop_runs += 1;
            log::debug!(
                "Running scheduling loop - inflight futures: {}, num queued inputs: {}",
                running_task_futures.len(),
                self.num_queued_inputs(),
            );
            // Dispatch loop.
            loop {
                let next_task = self.schedule_next_admissible().or_else(|| {
                        if running_task_futures.is_empty() {
                            log::info!("No admissible tasks available and no inflight tasks - scheduling an unadmissible task.");
                            Some(self.schedule_next_submittable().unwrap())
                        } else {
                            None
                        }
                    });
                if let Some(task) = next_task {
                    let fut = self.submit_task(task);
                    running_task_futures.push(fut);
                    num_tasks_run += 1;
                    log::debug!(
                        "Submitted new task - inflight futures: {}, num queued inputs: {}",
                        running_task_futures.len(),
                        self.num_queued_inputs(),
                    );
                } else {
                    // No more tasks are schedulable, break out of dispatch loop.
                    break;
                }
            }
            // TODO(Clark): Tweak this timeout.
            let timeout = tokio::time::sleep(Duration::from_millis(10));
            tokio::pin!(timeout);
            // Wait loop.
            while !running_task_futures.is_empty() {
                tokio::select! {
                    // Bias polling order to be ctrl-c -> new task output -> timeout.
                    biased;
                    // Break out of wait loop on SIGINT/SIGTERM.
                    // TODO(Clark): Lift this to stage runner.
                    _ = tokio::signal::ctrl_c() => {
                        log::info!("Received ctrl-c signal, exiting scheduler.");
                        shutdown = true;
                        break;
                    }
                    // Wait for tasks to finish.
                    Some(result) = running_task_futures.next() => {
                        let (task_id, result) = match result {
                            Ok((task_id, result)) => (task_id, result),
                            Err(e) => {
                                self.output_channel.send_output(Err(e)).await.unwrap();
                                continue;
                            }
                        };
                        let running_task = self.inflight_tasks.remove(&task_id).unwrap();
                        log::debug!(
                            "Task done for op {} with ID = {}.",
                            running_task.node.op_name(),
                            task_id
                        );
                        self.inflight_op_task_count[running_task.op_id()] -= 1;
                        running_task
                            .node
                            .push_outputs_back(result, running_task.output_seq_no());
                        while let Some(outputs) = self.state_root.pop_outputs() {
                            if let Err(e) = self.output_channel.send_output(Ok(outputs)).await {
                                log::info!(
                                    "Early-terminating scheduling due to consumer being done: {:?}",
                                    e
                                );
                                log::info!(
                                    "Scheduling loop ran {} times, running {} tasks",
                                    num_scheduling_loop_runs,
                                    num_tasks_run
                                );
                                return;
                            }
                        }
                    }
                    // Only wait for a new future to finish for a total of 10ms, across all loops of this wait loop.
                    _ = &mut timeout => {
                        log::debug!("10ms future waiting window exhausted, running dispatch loop again.");
                        break;
                    }
                }
            }
        }
        log::info!(
            "Scheduling loop ran {} times, running {} tasks",
            num_scheduling_loop_runs,
            num_tasks_run
        );
        // Process shutdown case.
        if shutdown {
            if let Err(e) = self
                .output_channel
                .send_output(Err(DaftError::InternalError(
                    "Ctrl-c cancelled execution.".to_string(),
                )))
                .await
            {
                log::info!(
                        "Early-terminating scheduling due to consumer being done, but received ctrl-c signal that won't be propagated: {:?}",
                        e
                    );
            }
            return;
        }
        // Final output consumption.
        while let Some(outputs) = self.state_root.pop_outputs() {
            if let Err(e) = self.output_channel.send_output(Ok(outputs)).await {
                log::info!(
                    "Early-terminating scheduling due to consumer being done: {:?}",
                    e
                );
                log::info!(
                    "Scheduling loop ran {} times, running {} tasks",
                    num_scheduling_loop_runs,
                    num_tasks_run
                );
                return;
            }
        }
    }
}

pub struct SenderWrapper<T: PartitionRef>(pub tokio::sync::mpsc::Sender<DaftResult<Vec<T>>>);

#[async_trait(?Send)]
impl<T: PartitionRef> OutputChannel<T> for SenderWrapper<T> {
    async fn send_output(&mut self, output: DaftResult<Vec<T>>) -> DaftResult<()> {
        self.0.send(output).await.map_err(|_| {
            // NOTE: We don't use a Snafu contextual error here since we don't want to have to make T send + sync everywhere,
            // and we currently don't need the unsent value anyway.
            DaftError::InternalError("Receiver dropped before done sending".to_string())
        })
    }
}
