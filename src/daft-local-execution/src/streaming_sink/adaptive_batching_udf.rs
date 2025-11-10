use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
    vec,
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use tokio::sync::Mutex;
use tracing::{Span, instrument};

use crate::{
    ExecutionTaskSpawner,
    intermediate_ops::udf::{UdfHandle, UdfOperator},
    pipeline::{MorselSizeRequirement, NodeName},
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
        StreamingSinkFinalizeResult, StreamingSinkOutput,
    },
};

#[derive(Clone)]
pub struct DynamicBatchingConfig {
    pub max_size: usize,
    pub min_size: usize,
    pub timing_window: usize, // Number of executions to average
}

impl Default for DynamicBatchingConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 1024 * 1024,
            timing_window: 128,
        }
    }
}

type Item = DaftResult<(RecordBatch, usize, Duration)>;
type UdfResultStream = Pin<Box<dyn Stream<Item = Item> + Send>>;
type SendableStream = Arc<Mutex<UdfResultStream>>;

/// Each UdfState holds a handle to a single Python process.
/// The concurrency of the Python process pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
pub(crate) struct UdfState {
    udf_handle: Arc<UdfHandle>,
    execution_times: VecDeque<(usize, Duration)>,
    current_batch_size: Arc<AtomicUsize>,
    dynamic_batching: DynamicBatchingConfig,
    // Could probably implement this without a stream and manually produce batches
    // but a stream is conceptually simpler and easier to reason about
    // The only issue is that its's not `Send` so we need to `Arc<Mutex<_>>` it
    batch_stream: Option<SendableStream>,
}

impl UdfState {
    /// This algorithm dynamically adjusts the batch size based on observed throughput measurements,
    /// balancing between maximizing throughput and avoiding excessive latency. It employs a multi-phase
    /// approach that transitions from aggressive exploration to careful optimization.
    ///
    /// # Algorithm Phases
    ///
    /// ## 1. Initial Bootstrap Phase (< 2 measurements)
    /// - Grows batch size exponentially by GROWTH_FACTOR to quickly explore the space
    /// - No throughput data available yet, so aggressive growth is used
    ///
    /// ## 2. Fast Exploration Phase (2-24 measurements)
    /// - Continues exponential growth but with safety checks
    /// - Predicts if next batch would exceed MAX_TARGET_DURATION
    /// - If predicted duration too long, calculates optimal size to fit within target
    /// - Ensures downstream consumers don't wait too long between batches
    ///
    /// ## 3. Convergence Phase (>= 24 measurements)
    /// - Requires MIN_MEASUREMENTS_PER_SIZE per batch size for reliable statistics
    /// - Calculates average throughput for each batch size
    /// - Identifies best performing sizes considering:
    ///   - All sizes within CLOSE_ENOUGH_THRESHOLD of max throughput
    ///   - Among these, selects the LARGEST batch size to minimize concatenation overhead
    ///
    /// ## 4. Decision Logic
    ///
    /// ### Switching to Better Size
    /// - If best size offers >20% better throughput than current, switches immediately
    /// - Ensures we don't stick with suboptimal sizes when better options exist
    ///
    /// ### Convergence Detection
    /// - If throughput variance across reliable sizes is <10%, system has converged
    /// - When converged and current size is within 90% of best, maintains stability
    ///
    /// ### Limited Exploration
    /// - When at best known size, explores ±10% to check for local improvements
    /// - Only explores sizes not yet measured sufficiently
    /// - Prevents getting stuck in local optima
    ///
    /// # Key Design Decisions
    ///
    /// 1. **Larger batches preferred**: Among similar performers, chooses larger batches
    ///    to reduce per-batch overhead and concatenation costs
    ///
    /// 2. **Duration capping**: Prevents individual batches from taking too long,
    ///    ensuring responsive downstream processing
    ///
    /// 3. **Statistical confidence**: Requires multiple measurements per size before
    ///    making decisions, avoiding noise-driven adjustments
    ///
    /// 4. **Efficient sizes**: Rounds to "efficient" batch sizes (implementation-specific)
    ///    to align with system boundaries or memory layouts
    ///
    /// # Performance Characteristics
    ///
    /// - Quickly finds near-optimal batch sizes through exponential growth
    /// - Stabilizes once optimal range is found, avoiding oscillation
    /// - Adapts to changing conditions by continuous measurement
    /// - Balances throughput optimization with latency constraints
    ///
    // Note: This algorithm could likely be improved with more sophisticated techniques
    // (e.g., gradient-based optimization, better statistical modeling), but provides
    // a reasonable initial approach that works well in practice.
    fn adjust_batch_size(&self) {
        const GROWTH_FACTOR: usize = 4;
        // We don't want to keep downstream nodes waiting too long, so we cap the duration at ~5 seconds per batch
        const MAX_TARGET_DURATION: f64 = 5.0;
        // Keep sampling until we have enough data points to start making decisions
        const MIN_SAMPLE_SIZE: usize = 24;
        // Require at least 4 measurements per batch size
        const MIN_MEASUREMENTS_PER_SIZE: usize = 4;

        if self.execution_times.len() < 2 {
            // Initial growth phase
            let current_size = self.current_batch_size.load(Ordering::Relaxed);

            let new_size = (current_size * GROWTH_FACTOR).min(self.dynamic_batching.max_size);
            self.current_batch_size
                .store(to_efficient_batch_size(new_size), Ordering::Relaxed);
            return;
        }

        // Calculate throughput for recent executions
        let throughputs: Vec<(usize, f64)> = self
            .execution_times
            .iter()
            .map(|(size, time)| (*size, if time.as_secs_f64() > 0.0 { *size as f64 / time.as_secs_f64() } else { f64::MAX }))
            .collect();

        // Group measurements by batch size and calculate averages
        let mut size_measurements: HashMap<usize, Vec<f64>> = HashMap::new();
        for (size, throughput) in &throughputs {
            size_measurements
                .entry(*size)
                .or_default()
                .push(*throughput);
        }

        // Calculate average throughput for each batch size
        let mut avg_throughputs: Vec<(usize, f64, usize)> = size_measurements
            .iter()
            .map(|(size, throughputs)| {
                let avg = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
                let count = throughputs.len();
                (*size, avg, count)
            })
            .collect();

        // Sort by batch size for easier comparison
        avg_throughputs.sort_by_key(|(size, _, _)| *size);

        // Get current size and throughput
        let current_size = self.current_batch_size.load(Ordering::Relaxed);
        let (current_throughput, current_count) = avg_throughputs
            .iter()
            .find(|(size, _, _)| *size == current_size)
            .map(|(_, throughput, count)| (*throughput, *count))
            .unwrap_or((0.0, 0));

        // Check if we need more measurements for current batch size
        if current_count < MIN_MEASUREMENTS_PER_SIZE {
            log::debug!(
                "{}: Need more measurements for batch size {} (have {}, need {}), staying",
                std::thread::current().name().unwrap_or("unknown"),
                current_size,
                current_count,
                MIN_MEASUREMENTS_PER_SIZE
            );
            return; // Stay with current batch size to get more measurements
        }

        // Find the best size so far (only consider sizes with enough measurements)

        let reliable_measurements: Vec<_> = avg_throughputs
            .iter()
            .filter(|(_, _, count)| *count >= MIN_MEASUREMENTS_PER_SIZE)
            .collect();

        let (best_size, best_throughput) = if reliable_measurements.is_empty() {
            // No batch sizes have enough measurements yet, stay with current
            log::debug!(
                "{}: No batch sizes have enough measurements yet, staying with current: {}",
                std::thread::current().name().unwrap_or("unknown"),
                current_size
            );
            return;
        } else {
            // Find the maximum throughput
            let max_throughput = reliable_measurements
                .iter()
                .map(|(_, throughput, _)| *throughput)
                .fold(0.0, f64::max);

            // Find all sizes within 10% of max throughput (close enough)
            const CLOSE_ENOUGH_THRESHOLD: f64 = 0.10; // 10% tolerance
            let close_enough_sizes: Vec<_> = reliable_measurements
                .iter()
                .filter(|(_, throughput, _)| {
                    *throughput >= max_throughput * (1.0 - CLOSE_ENOUGH_THRESHOLD)
                })
                .collect();

            // Among close enough sizes, pick the LARGEST batch size to reduce concatenation overhead
            let best_entry = close_enough_sizes
                .iter()
                .max_by_key(|(size, _, _)| *size) // Pick largest batch size among similar performers
                .unwrap();

            log::debug!(
                "{}: Close enough sizes (within {}%): {} → choosing largest: {}",
                std::thread::current().name().unwrap_or("unknown"),
                CLOSE_ENOUGH_THRESHOLD * 100.0,
                close_enough_sizes
                    .iter()
                    .map(|(size, throughput, _)| format!("{}={:.2}", size, throughput))
                    .collect::<Vec<_>>()
                    .join(", "),
                best_entry.0
            );

            (best_entry.0, best_entry.1)
        };

        log::debug!(
            "{}: Throughput comparison - Current: {} ({:.2} rows/sec, {}x), Best: {} ({:.2} rows/sec)",
            std::thread::current().name().unwrap_or("unknown"),
            current_size,
            current_throughput,
            current_count,
            best_size,
            best_throughput
        );

        // Fast exploration phase - try to quickly find good batch sizes
        if self.execution_times.len() < MIN_SAMPLE_SIZE {
            // Check if current batch would exceed duration limit ONLY during fast exploration
            if current_throughput > 0.0 {
                let next_size = (current_size * GROWTH_FACTOR).min(self.dynamic_batching.max_size);
                let predicted_batch_time = next_size as f64 / current_throughput;
                if predicted_batch_time > MAX_TARGET_DURATION {
                    // Predicted batch would be too long, calculate optimal size for MAX_TARGET_DURATION
                    // Use current throughput to estimate what size would fit in target duration
                    let target_size_for_duration =
                        (current_throughput * MAX_TARGET_DURATION).floor() as usize;

                    // Cap it to be smaller than current size since we know growing is too slow
                    let new_size = target_size_for_duration
                        .min(current_size) // Don't go larger than current
                        .max(self.dynamic_batching.min_size)
                        .min(self.dynamic_batching.max_size);
                    let new_size = to_efficient_batch_size(new_size);
                    log::debug!(
                        "{}: Predicted next batch would exceed duration limit ({:.1}s), reducing to optimal: {} → {} (target for {}s)",
                        std::thread::current().name().unwrap_or("unknown"),
                        predicted_batch_time,
                        current_size,
                        new_size,
                        MAX_TARGET_DURATION
                    );

                    self.current_batch_size.store(new_size, Ordering::Relaxed);
                    return;
                }
            }

            // Exponential growth in early phase
            let new_size = (current_size * GROWTH_FACTOR).min(self.dynamic_batching.max_size);
            log::debug!(
                "{}: Fast exploration: {} → {} (phase: {})",
                std::thread::current().name().unwrap_or("unknown"),
                current_size,
                new_size,
                self.execution_times.len()
            );
            self.current_batch_size
                .store(to_efficient_batch_size(new_size), Ordering::Relaxed);
            return;
        }
        // If we have enough samples, check if we're converging
        else {
            // If best is significantly better than current, move to best
            if best_size != current_size && best_throughput > current_throughput * 1.2 {
                log::debug!(
                    "{}: Moving to best known batch size: {} (throughput: {:.2} rows/sec) from {} (throughput: {:.2} rows/sec)",
                    std::thread::current().name().unwrap_or("unknown"),
                    best_size,
                    best_throughput,
                    current_size,
                    current_throughput
                );
                self.current_batch_size
                    .store(to_efficient_batch_size(best_size), Ordering::Relaxed);
                return;
            }

            // Check if we're not improving anymore (converged)
            let reliable_throughputs: Vec<f64> = avg_throughputs
                .iter()
                .filter(|(_, _, count)| *count >= MIN_MEASUREMENTS_PER_SIZE)
                .map(|(_, throughput, _)| *throughput)
                .collect();

            if reliable_throughputs.len() >= 3 {
                let min_throughput = reliable_throughputs
                    .iter()
                    .fold(f64::INFINITY, |a, &b| a.min(b));
                let max_throughput = reliable_throughputs
                    .iter()
                    .fold(0.0, |a, &b| if b > a { b } else { a });

                log::debug!(
                    "{}: Max throughput: {:.2}, Min throughput: {:.2}",
                    std::thread::current().name().unwrap_or("unknown"),
                    max_throughput,
                    min_throughput
                );

                // If throughput range is small (less than 10% variation), we've converged
                if (max_throughput - min_throughput) / max_throughput < 0.1 {
                    // Converged - just stay at current size if it's close to best
                    if current_throughput >= best_throughput * 0.9 {
                        log::debug!(
                            "{}: Converged on batch size {} (within 5% of best throughput)",
                            std::thread::current().name().unwrap_or("unknown"),
                            current_size
                        );
                        return;
                    }
                }
            }
        }

        // Limited exploration...  explore around best size
        let sizes_tried: HashSet<_> = avg_throughputs
            .iter()
            .filter(|(_, _, count)| *count >= MIN_MEASUREMENTS_PER_SIZE)
            .map(|(size, _, _)| *size)
            .collect();

        // Try a larger size if we're at the best so far
        if current_size == best_size {
            // Try 10% larger
            let larger = to_efficient_batch_size(
                (current_size + current_size / 10).min(self.dynamic_batching.max_size),
            );

            // If we haven't tried this size yet (with enough measurements)
            if !sizes_tried.contains(&larger) {
                log::debug!(
                    "{}: Exploring larger from best: {} → {}",
                    std::thread::current().name().unwrap_or("unknown"),
                    current_size,
                    larger
                );
                self.current_batch_size.store(larger, Ordering::Relaxed);
                return;
            }

            // Also try 10% smaller if larger sizes aren't helping
            let smaller = to_efficient_batch_size(
                (current_size - current_size / 10).max(self.dynamic_batching.min_size),
            );
            if !sizes_tried.contains(&smaller) {
                log::debug!(
                    "{}: Exploring smaller from best: {} → {}",
                    std::thread::current().name().unwrap_or("unknown"),
                    current_size,
                    smaller
                );
                self.current_batch_size.store(smaller, Ordering::Relaxed);
                return;
            }

            // We've tried nearby sizes, stick with best
            log::debug!(
                "{}: Sticking with best batch size: {}",
                std::thread::current().name().unwrap_or("unknown"),
                best_size
            );
            return;
        }

        // Not at best size - move toward best size
        log::debug!(
            "{}: Moving toward best: {} → {}",
            std::thread::current().name().unwrap_or("unknown"),
            current_size,
            best_size
        );
        self.current_batch_size
            .store(to_efficient_batch_size(best_size), Ordering::Relaxed);
    }
}

impl StreamingSink for UdfOperator {
    type State = UdfState;

    #[instrument(skip_all, name = "UdfOperator::execute")]
    #[cfg(feature = "python")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let params = self.params.clone();
        let handle = state.udf_handle.clone();

        let fut = task_spawner.spawn(
            async move {
                let shared_batch_size = state.current_batch_size.clone();

                if state.batch_stream.is_none() {
                    let params = params.clone();

                    let stream = async_stream::stream! {

                        let input_batches = match input.get_tables() {
                            Ok(batches) => batches,
                            Err(e) => {
                                yield Err(e.into());
                                return;
                            }
                        };

                        for batch in input_batches.as_ref() {
                            let total_rows = batch.num_rows();
                            let mut row_offset = 0;

                            while row_offset < total_rows {
                                let current_batch_size = shared_batch_size.load(Ordering::Relaxed).min(total_rows - row_offset);
                                let sub_batch = match batch.slice(row_offset, row_offset + current_batch_size) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        yield Err(e);
                                        return;
                                    }
                                };
                                let func_input = sub_batch.get_columns(params.required_cols.as_slice());


                                let num_rows = sub_batch.num_rows();
                                let start_time = std::time::Instant::now();

                                let mut sub_result = if let Some(h) = &handle.handle {
                                    match handle.eval_input_with_handle(func_input, h) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            yield Err(e);
                                            return;
                                        }
                                    }
                                } else {
                                    match handle.eval_input_inline(func_input) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            yield Err(e);
                                            return;
                                        }
                                    }
                                };

                                let duration = start_time.elapsed();


                                if sub_result.len() == 1 {
                                    sub_result = match sub_result.broadcast(num_rows) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            yield Err(e);
                                            return;
                                        }
                                    };
                                }

                                row_offset += current_batch_size;


                                let passthrough_input = match sub_batch.eval_expression_list(params.passthrough_columns.as_slice()) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        yield Err(e);
                                        return;
                                    }
                                };

                                let output_batch = match passthrough_input.append_column(params.output_schema.clone(), sub_result) {
                                    Ok(o) => o,
                                    Err(e) => {
                                        yield Err(e);
                                        return;
                                    }
                                };
                                log::info!("Output batch size: {}", output_batch.len());
                                yield Ok((output_batch, current_batch_size, duration));
                            }
                        }
                    };

                    state.batch_stream = Some(Arc::new(Mutex::new(Box::pin(stream))));

                }


                if let Some(stream_mutex) = &state.batch_stream {
                    let result = {
                        let mut stream = stream_mutex.lock().await;
                        stream.try_next().await
                    };
                    match result {
                        Ok(Some((batch, batch_size, duration))) => {
                            // Update state with performance data
                            state.execution_times.push_back((batch_size, duration));
                            if state.execution_times.len() > state.dynamic_batching.timing_window {
                                state.execution_times.pop_front();
                            }
                            state.adjust_batch_size();

                            let output = Arc::new(MicroPartition::new_loaded(
                                params.output_schema.clone(),
                                Arc::new(vec![batch]),
                                None,
                            ));

                            Ok((state, StreamingSinkOutput::HasMoreOutput(Some(output))))
                        },
                        Ok(None) => {
                            state.batch_stream = None;
                            Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
                        }
                        Err(e) => Err(e)
                    }


                } else {
                    Ok((state, StreamingSinkOutput::NeedMoreInput(None)))

                }
            },
            Span::current(),

        );
        fut.into()
    }

    #[cfg(not(feature = "python"))]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        unimplemented!()
    }

    #[cfg(feature = "python")]
    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        // The streams (should) never have anything left after the execute, but just to be safe,
        // we'll check if there's anything left in the streams push it downstream.
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    // Try to drain streams from all states
                    for (idx, state) in states.iter_mut().enumerate() {
                        if let Some(stream_mutex) = &state.batch_stream {
                            let result = {
                                let mut stream = stream_mutex.lock().await;
                                stream.try_next().await
                            };

                            match result {
                                Ok(Some((batch, batch_size, duration))) => {
                                    // Update state with performance data
                                    state.execution_times.push_back((batch_size, duration));
                                    if state.execution_times.len()
                                        > state.dynamic_batching.timing_window
                                    {
                                        state.execution_times.pop_front();
                                    }
                                    state.adjust_batch_size();

                                    log::debug!(
                                        "Finalize: Worker {} yielding batch of size {}",
                                        idx,
                                        batch_size
                                    );

                                    let output = Some(Arc::new(MicroPartition::new_loaded(
                                        params.output_schema.clone(),
                                        Arc::new(vec![batch]),
                                        None,
                                    )));

                                    // Still have more to drain
                                    return Ok(StreamingSinkFinalizeOutput::HasMoreOutput {
                                        states,
                                        output,
                                    });
                                }
                                Ok(None) => {
                                    // This stream is exhausted
                                    state.batch_stream = None;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }

                    // Check if any state still has an active stream
                    let has_active_streams = states.iter().any(|s| s.batch_stream.is_some());

                    if has_active_streams {
                        Ok(StreamingSinkFinalizeOutput::HasMoreOutput {
                            states,
                            output: None,
                        })
                    } else {
                        Ok(StreamingSinkFinalizeOutput::Finished(None))
                    }
                },
                Span::current(),
            )
            .into()
    }

    #[cfg(not(feature = "python"))]
    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        unimplemented!()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let worker_count = self.worker_count.fetch_add(1, Ordering::SeqCst);

        // Check if any inputs or the output are Python-dtype columns
        // Those should by default run on the same thread
        let fields = self.input_schema.fields();
        let is_arrow_dtype = self
            .params
            .required_cols
            .iter()
            .all(|idx| fields[*idx].dtype.is_arrow())
            && self
                .params
                .expr
                .inner()
                .to_field(self.input_schema.as_ref())?
                .dtype
                .is_arrow();

        let create_handle = self.params.udf_properties.is_actor_pool_udf()
            || self.params.udf_properties.use_process.unwrap_or(false);

        let mut udf_handle =
            UdfHandle::no_handle(self.params.clone(), self.params.expr.clone(), worker_count);

        if create_handle {
            if is_arrow_dtype {
                udf_handle.create_handle()?;
            } else {
                // Should only warn when concurrency or use_process is set
                log::warn!(
                    "UDF `{}` requires a non-arrow-serializable input column. The UDF will run on the same thread as the daft process.",
                    self.params.udf_properties.name
                );
            }
        }

        Ok(UdfState {
            udf_handle: Arc::new(udf_handle),
            current_batch_size: Arc::new(AtomicUsize::new(1)),
            dynamic_batching: Default::default(),
            execution_times: Default::default(),
            batch_stream: None,
        })
    }

    fn max_concurrency(&self) -> usize {
        self.concurrency
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        // this is the min/max batch sizes we'll produce
        Some(MorselSizeRequirement::Flexible(1, 1024 * 1024))
    }
    fn name(&self) -> NodeName {
        let udf_name = if let Some((_, udf_name)) = self.params.udf_properties.name.rsplit_once('.')
        {
            udf_name
        } else {
            self.params.udf_properties.name.as_str()
        };

        format!("UDF {}", udf_name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::UDFProject
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![
            format!("UDF: {}", self.params.udf_properties.name.as_str()),
            format!("Expr = {}", self.params.expr),
            format!(
                "Passthrough Columns = [{}]",
                self.params.passthrough_columns.iter().join(", ")
            ),
            format!("Concurrency = {}", self.concurrency),
        ];
        if let Some(resource_request) = &self.params.udf_properties.resource_request {
            let multiline_display = resource_request.multiline_display();
            res.push(format!(
                "Resource request = {{ {} }}",
                multiline_display.join(", ")
            ));
        } else {
            res.push("Resource request = None".to_string());
        }
        res
    }
}

/// Converts a given size into an efficient batch size.
/// The efficient batch size is rounded to the nearest multiple of 8, 16, or 32 based on the size.
/// The rounded ones are usually more efficient for memory alignment and cache utilization.
fn to_efficient_batch_size(size: usize) -> usize {
    // Round to nearest multiple of 8, 16, or 32 based on size
    let multiple = if size < 64 {
        2
    } else if size < 256 {
        8 // Multiples of 8: 8, 16, 24, 32, 40, 48, 56, 64...
    } else if size < 512 {
        16 // Multiples of 16: 64, 80, 96, 112, 128, 144...
    } else if size < 1024 {
        32 // Multiples of 32: 512, 544, 576, 608, 640...
    } else {
        64
    };

    let rounded = ((size + multiple / 2) / multiple) * multiple;
    rounded.max(multiple)
}
