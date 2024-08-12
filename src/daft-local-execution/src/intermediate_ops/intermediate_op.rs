use std::{env, sync::{atomic::AtomicU64, Arc}, time::Instant};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use async_trait::async_trait;

use crate::{
    channel::{
        create_channel, create_single_channel, spawn_compute_task, MultiReceiver, MultiSender,
        SingleReceiver, SingleSender,
    },
    pipeline::PipelineNode,
    DEFAULT_MORSEL_SIZE, NUM_CPUS,
};

pub trait IntermediateOperator: Send + Sync {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
    #[allow(dead_code)]

    fn name(&self) -> &'static str;
}

/// The number of rows that will trigger an intermediate operator to output its data.
#[allow(dead_code)]
fn get_output_threshold() -> usize {
    env::var("OUTPUT_THRESHOLD")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .expect("OUTPUT_THRESHOLD must be a number")
}

/// State of an operator task, used to buffer data and output it when a threshold is reached.
pub struct OperatorTaskState {
    pub buffer: Vec<Arc<MicroPartition>>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl OperatorTaskState {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            curr_len: 0,
            threshold: DEFAULT_MORSEL_SIZE,
        }
    }

    // Add a micro partition to the buffer.
    pub fn add(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push(part);
    }

    // Try to clear the buffer if the threshold is reached.
    pub fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.curr_len >= self.threshold {
            self.clear()
        } else {
            None
        }
    }

    // Clear the buffer and return the concatenated MicroPartition.
    pub fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return None;
        }
        assert!(
            !self.buffer.is_empty(),
            "We can not run concat with no data"
        );
        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new);
        self.buffer.clear();
        self.curr_len = 0;
        Some(concated)
    }
}

/// An actor that runs an intermediate operator.
/// The actor can run multiple tasks in parallel, depending on the buffer size of the sender.
/// Each parallel task is mapped to a single sender.
pub struct IntermediateOpActor {
    sender: MultiSender,
    receiver: MultiReceiver,
    op: Arc<dyn IntermediateOperator>,
    rt_context: Arc<RuntimeStatsContext>,
}


#[derive(Default)]
struct RuntimeStatsContext {
    name: String,
    rows_received: AtomicU64,
    rows_emitted: AtomicU64,
    cpu_us: AtomicU64,
}

#[derive(Debug)]
struct RuntimeStats {
    rows_received: u64,
    rows_emitted: u64,
    cpu_us: u64,

}

impl RuntimeStatsContext {

    fn new(name: String) -> Self {
        Self { name:name, rows_received: AtomicU64::new(0), rows_emitted: AtomicU64::new(0), cpu_us: AtomicU64::new(0)}
    }
    fn in_span<F: FnOnce() -> T, T>(&self, span: &tracing::Span, f: F) -> T {
        let _enter = span.enter();
        let start = Instant::now();
        let result = f();
        let total = start.elapsed();
        let micros = total.as_micros() as u64;
        self.cpu_us.fetch_add(micros, std::sync::atomic::Ordering::Relaxed);
        result
    }

    fn mark_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    fn mark_rows_emitted(&self, rows: u64) {
        self.rows_emitted.fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    fn reset(&self) {
        self.rows_received.store(0, std::sync::atomic::Ordering::Release);
        self.rows_emitted.store(0, std::sync::atomic::Ordering::Release);
        self.cpu_us.store(0, std::sync::atomic::Ordering::Release);
    }

    fn result(&self) -> RuntimeStats {
        RuntimeStats {
            rows_received: self.rows_received.load(std::sync::atomic::Ordering::Relaxed),
            rows_emitted: self.rows_emitted.load(std::sync::atomic::Ordering::Relaxed),
            cpu_us: self.cpu_us.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl IntermediateOpActor {
    pub fn new(
        op: Arc<dyn IntermediateOperator>,
        receiver: MultiReceiver,
        sender: MultiSender,
    ) -> Self {
        let name = op.name().to_string();
        Self {
            op,
            receiver,
            sender,
            rt_context: Arc::new(RuntimeStatsContext::new(name))
        }
    }

    // Run a single instance of the operator.
    #[instrument(level = "info", skip_all, name = "IntermediateOpActor::run_single")]
    async fn run_single(
        mut receiver: SingleReceiver,
        sender: SingleSender,
        op: Arc<dyn IntermediateOperator>,
        rt_context: Arc<RuntimeStatsContext>,

    ) -> DaftResult<()> {
        let mut state = OperatorTaskState::new();
        let span = info_span!("IntermediateOp::execute");

        while let Some(morsel) = receiver.recv().await {
            let morsel = morsel?;
            rt_context.mark_rows_received(morsel.len() as u64);
            let result = rt_context.in_span(&span, || op.execute(&morsel))?;
            state.add(result);
            if let Some(part) = state.try_clear() {
                let part = part?;
                rt_context.mark_rows_emitted(part.len() as u64);
                let _ = sender.send(Ok(part)).await;
            }
        }
        if let Some(part) = state.clear() {
            let part = part?;
            rt_context.mark_rows_emitted(part.len() as u64);
            let _ = sender.send(Ok(part)).await;
        }
        Ok(())
    }

    // Create and run parallel tasks for the operator.
    #[instrument(level = "info", skip_all, name = "IntermediateOpActor::run_parallel")]
    pub async fn run_parallel(&mut self) -> DaftResult<()> {
        // Initialize senders to send data to parallel tasks.
        let mut inner_task_senders: Vec<SingleSender> =
            Vec::with_capacity(self.sender.buffer_size());
        let mut curr_task_idx = 0;

        while let Some(morsel) = self.receiver.recv().await {
            // If the task sender already exists for the current index, send the data to it.
            if let Some(s) = inner_task_senders.get(curr_task_idx) {
                let _ = s.send(morsel).await;
            }
            // Otherwise, create a new task and send the data to it.
            else {
                let (task_sender, task_receiver) = create_single_channel(1);
                let op = self.op.clone();
                let next_sender = self.sender.get_next_sender();
                spawn_compute_task(Self::run_single(task_receiver, next_sender, op, self.rt_context.clone()));
                let _ = task_sender.send(morsel).await;

                inner_task_senders.push(task_sender);
            }
            curr_task_idx = (curr_task_idx + 1) % self.sender.buffer_size();
        }
        Ok(())
    }
}


impl Drop for RuntimeStatsContext {
    fn drop(&mut self) {
        println!("{}={:?}", self.name, self.result());
    }
}

pub(crate) struct IntermediateNode {
    intermediate_op: Arc<dyn IntermediateOperator>,
    children: Vec<Box<dyn PipelineNode>>,
}

impl IntermediateNode {
    pub(crate) fn new(
        intermediate_op: Arc<dyn IntermediateOperator>,
        children: Vec<Box<dyn PipelineNode>>,
    ) -> Self {
        IntermediateNode {
            intermediate_op,
            children,
        }
    }
    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

#[async_trait]
impl PipelineNode for IntermediateNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        self.children.iter().map(|v| v.as_ref()).collect()
    }

    fn name(&self) -> &'static str {
        self.intermediate_op.name()
    }

    async fn start(&mut self, destination: MultiSender) -> DaftResult<()> {
        assert_eq!(
            self.children.len(),
            1,
            "we only support 1 child for Intermediate Node for now"
        );

        let (sender, receiver) = create_channel(*NUM_CPUS, destination.in_order());

        let child = self
            .children
            .get_mut(0)
            .expect("we should only have 1 child");
        child.start(sender).await?;

        let mut actor =
            IntermediateOpActor::new(self.intermediate_op.clone(), receiver, destination);
        // this should ideally be in the actor
        spawn_compute_task(async move { actor.run_parallel().await });
        Ok(())
    }
}
