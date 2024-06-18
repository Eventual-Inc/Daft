use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use futures::{stream::FuturesUnordered, Future, StreamExt};

use crate::{
    executor::Executor,
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    tree::{task_tree_to_state_tree, topological_sort, OpNode, OpStateNode},
};

use super::task_state::{next_in_order_submittable_task, RunningTask, SubmittableTask};
use super::{channel::OutputChannel, task_state::all_unordered_submittable_tasks};

/// A scheduler for a tree of pipelinable partition tasks that streams outputs to a channel.
///
/// This task scheduler houses scheduling priorities for operators based on tree topology, input/output queue size,
/// a task or operator's utilization of a particular resource, etc. Execution of individual partition tasks is
/// performed by the executor, along with resource accounting and admission control.
#[derive(Debug)]
pub struct StreamingPartitionTaskScheduler<T: PartitionRef, E: Executor<T>, O: OutputChannel<T>> {
    // Root of the task tree.
    state_root: Rc<OpStateNode<T>>,
    // The task tree in topologically sorted order.
    sorted_state: Vec<Rc<OpStateNode<T>>>,
    // Currently in-flight tasks, keyed by task ID.
    inflight_tasks: HashMap<usize, RunningTask<T>>,
    // The number of tasks in-flight for a particular operator.
    inflight_op_task_count: Vec<usize>,
    // The channel that the scheduler sends (root) outputs to.
    output_channel: O,
    // TODO(Clark): Abstract out into pluggable backpressure policy.
    // Maximum size of the output queue for each operator.
    max_output_queue_size: Option<usize>,
    // TODO(Clark): Add per-op resource utilization tracking.
    // Executor for running tasks.
    executor: Arc<E>,
}

impl<T: PartitionRef, E: Executor<T>, O: OutputChannel<T>>
    StreamingPartitionTaskScheduler<T, E, O>
{
    fn from_state(
        state_root: Rc<OpStateNode<T>>,
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
        task_tree_root: OpNode,
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

    /// Number of inputs queued in entire operator tree.
    fn num_queued_inputs(&self) -> usize {
        self.sorted_state
            .iter()
            .map(|node| node.num_queued_inputs())
            .sum()
    }

    /// Whether any operator in tree has queued inputs.
    fn has_inputs(&self) -> bool {
        self.num_queued_inputs() > 0
    }

    /// Get all submittable tasks across all available input partitions, regardless of partition ordering.
    /// Submittable tasks are yielded in topologically sorted order.
    fn submittable_unordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        self.sorted_state.iter().flat_map(|task_state| {
            // TODO(Clark): Lift this to admissible filtering?
            if let Some(max_size) = self.max_output_queue_size
                && task_state.num_queued_outputs() >= max_size
            {
                return Box::new(std::iter::empty())
                    as Box<dyn Iterator<Item = SubmittableTask<T>>>;
            }
            Box::new(all_unordered_submittable_tasks(task_state.clone()))
        })
    }

    /// Get all submittable tasks across all operators, for the next partition (respecting partition ordering).
    /// A task is considered submittable if:
    /// 1. its inputs are ready,
    /// 2. the output queue for the operator doesn't exceed the configured max size.
    /// Submittable tasks are yielded in topologically sorted order.
    fn submittable_ordered(&self) -> impl Iterator<Item = SubmittableTask<T>> + '_ {
        self.sorted_state.iter().filter_map(|task_state| {
            // TODO(Clark): Lift this to admissible filtering?
            if let Some(max_size) = self.max_output_queue_size
                && task_state.num_queued_outputs() >= max_size
            {
                return None;
            }
            next_in_order_submittable_task(task_state.clone())
        })
    }

    /// Get next admissible task.
    ///
    /// A task is admissible if it is submittable and the executor has the resource
    /// availability to admit it. The next admissible task is the "best" admissible task in accordance with our
    /// scheduling policy, which is currently the admissible task whose operator has the least queued outputs.
    /// Ties are broken via the underlying topological ordering of the admissible tasks.
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

    /// Get next submittable task, disregarding whether the executor has the resource availability to admit it.
    ///
    /// This is required for liveness, where we must have at least one task in-flight in order to make progress.
    fn schedule_next_submittable(&self) -> Option<SubmittableTask<T>> {
        // TODO(Clark): Consolidate with schedule_next_admissible() via have a boolean require_admissible flag?
        // TODO(Clark): Use submittable_unordered to mitigate head-of-line blocking.
        let submittable = self.submittable_ordered();
        // TODO(Clark): Return the most admissible task (smallest resource capacity violation).
        submittable.min_by(|x, y| x.num_queued_outputs().cmp(&y.num_queued_outputs()))
    }

    /// Submit a task to the executor, returning a future for it's output.
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

    /// Execute operator task tree to completion.
    pub async fn execute(mut self) {
        // Set of running task futures. Polling this will internally poll all running task futures and return the first
        // that completes.
        let mut running_task_futures = FuturesUnordered::new();
        let mut num_scheduling_loop_runs = 0;
        let mut num_tasks_run = 0;
        // Shutdown signal, e.g. a SIGINT via ctrl-c.
        let mut shutdown = false;
        // Scheduling loop.
        // While we haven't received a shutdown signal, and we either have in-flight tasks or we still have queued
        // inputs in the operator task tree, keep running this scheduling loop.
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
                    // Add future to unordered running task future set.
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
            // Wait for task to finish.
            if !running_task_futures.is_empty() {
                tokio::select! {
                    // Bias polling order to be ctrl-c -> new task output.
                    biased;
                    // Break out of wait loop on SIGINT/SIGTERM.
                    // TODO(Clark): Lift this to stage runner.
                    _ = tokio::signal::ctrl_c() => {
                        log::info!("Received ctrl-c signal, exiting scheduler.");
                        shutdown = true;
                    }
                    // Wait for tasks to finish.
                    Some(result) = running_task_futures.next() => {
                        let (task_id, result) = match result {
                            Ok((task_id, result)) => (task_id, result),
                            Err(e) => {
                                // Send error to output channel.
                                if let Err(e) = self.output_channel.send_output(Err(e)).await {
                                    // Handle early termination due to the consumer being done.
                                    log::info!(
                                        "Early-terminating scheduling due to consumer being done, unable to propagate error: {:?}",
                                        e
                                    );
                                }
                                log::info!(
                                    "Scheduling loop ran {} times, running {} tasks",
                                    num_scheduling_loop_runs,
                                    num_tasks_run
                                );
                                return;
                            }
                        };
                        // Remove from in-flight tasks.
                        let running_task = self.inflight_tasks.remove(&task_id).unwrap();
                        log::debug!(
                            "Task done for op {} with ID = {}.",
                            running_task.node.op_name(),
                            task_id
                        );
                        self.inflight_op_task_count[running_task.op_id()] -= 1;
                        // Push result into the corresponding operator tree node's output queue.
                        running_task
                            .node
                            .push_outputs_back(result, running_task.output_seq_no());
                        // Process tree root outputs, sending any new outputs to the output channel.
                        while let Some(outputs) = self.state_root.pop_outputs() {
                            if let Err(e) = self.output_channel.send_output(Ok(outputs)).await {
                                // Handle early termination due to the consumer being done.
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
            }
        }
        log::info!(
            "Scheduling loop ran {} times, running {} tasks",
            num_scheduling_loop_runs,
            num_tasks_run
        );
        // Handle shutdown case.
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
        self.0.send(output).await.map_err(|out| match out.0 {
            Ok(_) => {
                DaftError::InternalError("Receiver dropped before done sending result".to_string())
            }
            Err(e) => DaftError::InternalError(format!(
                "Receiver dropped before done sending error: {:?}",
                e
            )),
        })
    }
}
