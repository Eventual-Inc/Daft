use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::Duration,
    vec,
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
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
struct DynamicBatchingConfig {
    /// Max batch size
    max_size: usize,
    /// Min batch size
    min_size: usize,
    /// How many samples to keep in our sliding window.
    timing_window: usize,
    /// During the initial bootstrap and sampling phase, we'll increase the batch size by `growth_factor`
    /// until we reach `max_size` or the latency threshold is reached.
    growth_factor: usize,
    /// Max latency for batch processing in seconds
    max_latency_s: f64,
    /// Minimum number of measurements per batch size to use during sampling
    min_measurements_per_size: usize,
    /// How many samples to take per batch size before moving to the next phase
    min_sample_size: usize,
}

impl Default for DynamicBatchingConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 1024 * 1024,
            timing_window: 128,
            growth_factor: 4,
            max_latency_s: 5.0,
            min_measurements_per_size: 4,
            min_sample_size: 24,
        }
    }
}

/// Each UdfState holds a handle to a single Python process.
/// The concurrency of the Python process pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
pub(crate) struct UdfState {
    udf_handle: UdfHandle,
    execution_times: VecDeque<(usize, Duration)>,
    current_batch_size: usize,
    dynamic_batching: DynamicBatchingConfig,
    row_offset: usize,
    batches: VecDeque<RecordBatch>,
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
    fn adjust_batch_size(&mut self) {
        let growth_factor = self.dynamic_batching.growth_factor;
        let max_latency_seconds = self.dynamic_batching.max_latency_s;
        let min_sample_size = self.dynamic_batching.min_sample_size;
        let min_measurements_per_second = self.dynamic_batching.min_measurements_per_size;

        if self.execution_times.len() < 2 {
            // Initial growth phase
            let current_size = self.current_batch_size;

            let new_size = (current_size * growth_factor).min(self.dynamic_batching.max_size);
            self.current_batch_size = to_efficient_batch_size(new_size);
            return;
        }

        // Calculate throughput for recent executions
        let throughputs: Vec<(usize, f64)> = self
            .execution_times
            .iter()
            .map(|(size, time)| {
                (
                    *size,
                    if time.as_secs_f64() > 0.0 {
                        *size as f64 / time.as_secs_f64()
                    } else {
                        f64::MAX
                    },
                )
            })
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
        let current_size = self.current_batch_size;
        let (current_throughput, current_count) = avg_throughputs
            .iter()
            .find(|(size, _, _)| *size == current_size)
            .map(|(_, throughput, count)| (*throughput, *count))
            .unwrap_or((0.0, 0));

        // Check if we need more measurements for current batch size
        if current_count < min_measurements_per_second {
            log::debug!(
                "{}: Need more measurements for batch size {} (have {}, need {}), staying",
                std::thread::current().name().unwrap_or("unknown"),
                current_size,
                current_count,
                min_measurements_per_second
            );
            return; // Stay with current batch size to get more measurements
        }

        // Find the best size so far (only consider sizes with enough measurements)

        let reliable_measurements: Vec<_> = avg_throughputs
            .iter()
            .filter(|(_, _, count)| *count >= min_measurements_per_second)
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
        if self.execution_times.len() < min_sample_size {
            // Check if current batch would exceed duration limit ONLY during fast exploration
            if current_throughput > 0.0 {
                let next_size = (current_size * growth_factor).min(self.dynamic_batching.max_size);
                let predicted_batch_time = next_size as f64 / current_throughput;
                if predicted_batch_time > max_latency_seconds {
                    // Predicted batch would be too long, calculate optimal size for MAX_TARGET_DURATION
                    // Use current throughput to estimate what size would fit in target duration
                    let target_size_for_duration =
                        (current_throughput * max_latency_seconds).floor() as usize;

                    // Cap it to be smaller than current size since we know growing is too slow
                    let new_size = target_size_for_duration
                        .max(self.dynamic_batching.min_size)
                        .min(self.dynamic_batching.max_size);
                    let new_size = to_efficient_batch_size(new_size);
                    log::debug!(
                        "{}: Predicted next batch would exceed duration limit ({:.1}s), reducing to optimal: {} → {} (target for {}s)",
                        std::thread::current().name().unwrap_or("unknown"),
                        predicted_batch_time,
                        current_size,
                        new_size,
                        max_latency_seconds
                    );

                    self.current_batch_size = to_efficient_batch_size(new_size);

                    return;
                }
            }

            // Exponential growth in early phase
            let new_size = (current_size * growth_factor).min(self.dynamic_batching.max_size);
            log::debug!(
                "{}: Fast exploration: {} → {} (phase: {})",
                std::thread::current().name().unwrap_or("unknown"),
                current_size,
                new_size,
                self.execution_times.len()
            );
            self.current_batch_size = to_efficient_batch_size(new_size);

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
                self.current_batch_size = best_size;

                return;
            }
            // Check if we should continue exploring larger sizes
            // Calculate what size would take ~5 seconds at current throughput
            let max_size_for_duration = if current_throughput > 0.0 {
                to_efficient_batch_size(
                    (current_throughput * max_latency_seconds * 0.9).floor() as usize
                ) // 90% of max to leave margin
            } else {
                self.dynamic_batching.max_size
            };
            // Find the largest size we've tried so far
            let largest_tried = avg_throughputs
                .iter()
                .map(|(size, _, _)| *size)
                .max()
                .unwrap_or(current_size);

            // If we haven't explored close to the duration limit, keep growing
            if largest_tried < max_size_for_duration
                && largest_tried < self.dynamic_batching.max_size
            {
                let new_size = (largest_tried * 2)
                    .min(max_size_for_duration)
                    .min(self.dynamic_batching.max_size);
                log::debug!(
                    "{}: Haven't explored near duration limit. Largest tried: {}, max for duration: {}, trying: {}",
                    std::thread::current().name().unwrap_or("unknown"),
                    largest_tried,
                    max_size_for_duration,
                    new_size
                );

                self.current_batch_size = to_efficient_batch_size(new_size);
                return;
            }

            // Check if we're not improving anymore (converged)
            let reliable_throughputs: Vec<f64> = avg_throughputs
                .iter()
                .filter(|(_, _, count)| *count >= min_measurements_per_second)
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
            .filter(|(_, _, count)| *count >= min_measurements_per_second)
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
                self.current_batch_size = larger;

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
                self.current_batch_size = smaller;
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
        self.current_batch_size = best_size;
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

        let fut = task_spawner.spawn(
            async move {
                if !state.udf_handle.udf_initialized {
                    use daft_dsl::functions::python::initialize_udfs;

                    state.udf_handle.udf_expr = BoundExpr::new_unchecked(initialize_udfs(
                        state.udf_handle.udf_expr.inner().clone(),
                    )?);
                    state.udf_handle.udf_initialized = true;
                }

                let handle = &state.udf_handle;

                // Add new batches to our queue
                if state.batches.is_empty() && state.row_offset == 0 {
                    let input_batches = input.get_tables()?;
                    state.batches.extend(input_batches.iter().cloned());
                }

                // Process batches until we produce output or run out
                while let Some(batch) = state.batches.front() {
                    let total_rows = batch.num_rows();

                    // If we've processed all rows in current batch, move to next
                    if state.row_offset >= total_rows {
                        state.batches.pop_front();
                        state.row_offset = 0;
                        continue;
                    }

                    let current_batch_size =
                        state.current_batch_size.min(total_rows - state.row_offset);

                    let sub_batch =
                        batch.slice(state.row_offset, state.row_offset + current_batch_size)?;
                    let func_input = sub_batch.get_columns(params.required_cols.as_slice());

                    let num_rows = sub_batch.num_rows();
                    let start_time = std::time::Instant::now();

                    let mut sub_result = if let Some(h) = &handle.handle {
                        handle.eval_input_with_handle(func_input, h)?
                    } else {
                        handle.eval_input_inline(func_input)?
                    };

                    let duration = start_time.elapsed();

                    if sub_result.len() == 1 {
                        sub_result = sub_result.broadcast(num_rows)?;
                    }

                    state.row_offset += current_batch_size;

                    let passthrough_input =
                        sub_batch.eval_expression_list(params.passthrough_columns.as_slice())?;
                    let output_batch = passthrough_input
                        .append_column(params.output_schema.clone(), sub_result)?;

                    // Update performance tracking
                    state
                        .execution_times
                        .push_back((current_batch_size, duration));
                    if state.execution_times.len() > state.dynamic_batching.timing_window {
                        state.execution_times.pop_front();
                    }
                    state.adjust_batch_size();

                    let output = Arc::new(MicroPartition::new_loaded(
                        params.output_schema.clone(),
                        Arc::new(vec![output_batch]),
                        None,
                    ));

                    return Ok((state, StreamingSinkOutput::HasMoreOutput(Some(output))));
                }

                // No more batches to process
                Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
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
        _states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        spawner
            .spawn(
                async move { Ok(StreamingSinkFinalizeOutput::Finished(None)) },
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

        let dynamic_batching = DynamicBatchingConfig::default();
        let execution_times = VecDeque::with_capacity(dynamic_batching.max_size);

        Ok(UdfState {
            udf_handle,
            current_batch_size: 1,
            dynamic_batching,
            execution_times,
            row_offset: 0,
            batches: VecDeque::new(),
        })
    }

    fn max_concurrency(&self) -> usize {
        self.concurrency
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        // this is the min/max batch sizes we'll produce
        Some(MorselSizeRequirement::Flexible(0, 1024 * 1024))
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

#[cfg(test)]
mod tests {
    use daft_core::prelude::Schema;
    use daft_dsl::null_lit;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::{
        intermediate_ops::udf::{UdfHandle, UdfParams},
        streaming_sink::adaptive_batching_udf::to_efficient_batch_size,
    };
    #[rstest]
    #[case(1, 2)]
    #[case(7, 8)]
    #[case(15, 16)]
    #[case(31, 32)]
    #[case(63, 64)]
    #[case(127, 128)]
    #[case(255, 256)]
    #[case(511, 512)]
    #[case(1023, 1024)]
    fn test_to_efficient_batch_size(#[case] input: usize, #[case] expected: usize) {
        assert_eq!(to_efficient_batch_size(input), expected);
    }
    fn mock_udf_params() -> UdfParams {
        UdfParams {
            expr: BoundExpr::new_unchecked(null_lit()),
            udf_properties: daft_dsl::functions::python::UDFProperties {
                name: "mock".to_string(),
                resource_request: None,
                batch_size: None,
                concurrency: None,
                use_process: None,
                max_retries: None,
                is_async: false,
                is_scalar: true,
                on_error: None,
            },
            passthrough_columns: Default::default(),
            output_schema: Arc::new(Schema::empty()),
            required_cols: Default::default(),
        }
    }

    fn mock_udf_handle() -> UdfHandle {
        UdfHandle {
            params: Arc::new(mock_udf_params()),
            udf_expr: BoundExpr::new_unchecked(null_lit()),
            worker_idx: 0,
            #[cfg(feature = "python")]
            handle: None,
            udf_initialized: true,
        }
    }

    #[fixture]
    fn state() -> UdfState {
        UdfState {
            udf_handle: mock_udf_handle(),
            execution_times: Default::default(),
            current_batch_size: 1,
            dynamic_batching: Default::default(),
            row_offset: 0,
            batches: VecDeque::new(),
        }
    }

    // Initial Bootstrap Phase (< 2 measurements)
    #[rstest]
    fn test_initial_growth_no_measurements(mut state: UdfState) {
        // When execution_times is empty, should grow by growth_factor
        state.dynamic_batching.growth_factor = 2; // Test with 2x growth
        state.current_batch_size = 1;

        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 2);
    }

    #[rstest]
    fn test_initial_growth_one_measurement(mut state: UdfState) {
        // When execution_times has 1 entry, should continue growing by growth_factor
        state.dynamic_batching.growth_factor = 2; // Test with 2x growth
        state.current_batch_size = 1;

        state
            .execution_times
            .push_back((10, Duration::from_secs(1)));

        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 2);
    }

    #[rstest]
    fn test_initial_growth_respects_max_size(mut state: UdfState) {
        // Initial growth should not exceed dynamic_batching.max_size
        state.dynamic_batching.max_size = 10;
        state.dynamic_batching.growth_factor = 4;
        state.current_batch_size = 1;
        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 4);

        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 10);
    }

    #[rstest]
    fn test_fast_exploration_duration_capping_triggers(mut state: UdfState) {
        state.dynamic_batching.max_latency_s = 1.1;
        state.dynamic_batching.growth_factor = 2;
        state.dynamic_batching.min_measurements_per_size = 1;
        state.current_batch_size = 1;
        // 1 (under cap) -> 2 (exceeds)

        // Add measurements for current batch size (1)
        state
            .execution_times
            .push_back((1, Duration::from_secs_f64(0.95)));
        state
            .execution_times
            .push_back((2, Duration::from_secs_f64(2.0))); // 1 rows/s

        state.adjust_batch_size();

        assert_eq!(dbg!(state.current_batch_size), 2);

        // at this point, we should be locked in at a batch size of 2
        for _ in 2..=state.dynamic_batching.min_sample_size {
            let size = state.current_batch_size;

            // 1 row/s
            state
                .execution_times
                .push_back((size, Duration::from_secs(size as _)));
            state.adjust_batch_size();
            let size = state.current_batch_size;
            assert_eq!(size, 2);
        }
    }
    #[rstest]
    fn test_switches_to_significantly_better_size(mut state: UdfState) {
        state.dynamic_batching.min_sample_size = 10;
        state.dynamic_batching.min_measurements_per_size = 2;
        state.dynamic_batching.max_size = 256; // Limit exploration
        state.current_batch_size = 64;

        // Add enough measurements to exit fast exploration
        for i in 0..15 {
            let sizes = [32, 64, 128];
            let size = sizes[i % 3];

            // Performance characteristics:
            // - Size 32: 1000 rows/s
            // - Size 64: 2000 rows/s
            // - Size 128: 3000 rows/s (50% better than 64)
            let throughput = match size {
                32 => 1000.0,
                64 => 2000.0,
                128 => 3000.0,
                _ => unreachable!(),
            };

            let duration = Duration::from_secs_f64(size as f64 / throughput);
            state.execution_times.push_back((size, duration));
        }

        // Currently at size 64 with throughput 2000 rows/s
        // Size 128 has throughput 3000 rows/s (50% better)
        // Should switch to 128 since it's >20% better
        state.adjust_batch_size();

        assert_eq!(state.current_batch_size, 128);
    }

    #[rstest]
    fn test_switches_to_significantly_better_size_for_smaller_batch(mut state: UdfState) {
        state.dynamic_batching.min_sample_size = 10;
        state.dynamic_batching.min_measurements_per_size = 2;
        state.dynamic_batching.max_size = 500;
        state.dynamic_batching.max_latency_s = 0.03; // 30ms - this limits exploration
        state.current_batch_size = 64;

        // Add enough measurements to exit fast exploration
        for i in 0..24 {
            let sizes = [32, 64, 128, 256];
            let size = sizes[i % 4];

            // Performance characteristics:
            // - Size 32: 1000 rows/s
            // - Size 64: 9000 rows/s (BEST!)
            // - Size 128: 3000 rows/s
            // - Size 256: 3000 rows/s
            let throughput = match size {
                32 => 1000.0,
                64 => 9000.0,
                128 => 3000.0,
                256 => 3000.0,
                _ => unreachable!(),
            };

            let duration = Duration::from_secs_f64(size as f64 / throughput);
            state.execution_times.push_back((size, duration));
        }

        state.adjust_batch_size();
        // will try to adjust to see if ~10% > 64 will perform better
        assert_eq!(state.current_batch_size, 72);
        let size = 72;
        let throughput = 2000.0;
        let duration = Duration::from_secs_f64(size as f64 / throughput);
        state.execution_times.push_back((size, duration));
        state.execution_times.push_back((size, duration));
        state.adjust_batch_size();
        // it does not, so it adjusts back down to 64
        assert_eq!(state.current_batch_size, 64);
    }
    #[rstest]
    fn test_stays_at_current_if_not_significantly_better(mut state: UdfState) {
        state.dynamic_batching.min_sample_size = 10;
        state.dynamic_batching.min_measurements_per_size = 2;
        state.dynamic_batching.max_size = 256; // Limit exploration
        state.dynamic_batching.max_latency_s = 0.1; // 100ms latency limit
        state.current_batch_size = 64;

        // Add enough measurements to exit fast exploration
        for i in 0..20 {
            let sizes = [32, 64, 128, 256];
            let size = sizes[i % 4];

            // Performance characteristics (small differences):
            // - Size 32: 1900 rows/s
            // - Size 64: 2000 rows/s
            // - Size 128: 2100 rows/s (only 5% better)
            // - Size 256: 2150 rows/s
            let throughput = match size {
                32 => 1900.0,
                64 => 2000.0,
                128 => 2100.0,
                256 => 2150.0,
                _ => unreachable!(),
            };

            let duration = Duration::from_secs_f64(size as f64 / throughput);
            state.execution_times.push_back((size, duration));
        }

        state.adjust_batch_size();

        // With max throughput ~2150, sizes within 10% threshold are those >= 1935
        // All sizes qualify, so algorithm picks largest: 256
        assert_eq!(state.current_batch_size, 256);
    }

    #[rstest]
    fn test_prefers_larger_size_among_similar_performers(mut state: UdfState) {
        state.dynamic_batching.min_sample_size = 12; // Ensure we're past fast exploration
        state.dynamic_batching.min_measurements_per_size = 3;
        state.dynamic_batching.max_size = 260;
        state.dynamic_batching.max_latency_s = 0.03;
        state.current_batch_size = 32;
        state.dynamic_batching.growth_factor = 2;

        // Add enough measurements for all sizes to be considered reliable
        let sizes = [32, 64, 128, 256];
        for size in &sizes {
            for _ in 0..3 {
                // Exactly min_measurements_per_size for each
                // All sizes have very similar throughput (within 10%)
                let throughput = 10000.0 + (*size as f64 * 0.01);
                let duration = Duration::from_secs_f64(*size as f64 / throughput);
                state.execution_times.push_back((*size, duration));
            }
        }

        // Now we have exactly 12 measurements, 3 for each size
        // This ensures all sizes are considered "reliable"
        state.adjust_batch_size();

        // Should pick the largest size (256) since all are within 10% of max throughput
        assert_eq!(state.current_batch_size, 256);
    }

    #[rstest]
    fn test_duration_capping_with_custom_latency(mut state: UdfState) {
        state.dynamic_batching.max_latency_s = 1.0; // Only 1 second allowed
        state.dynamic_batching.growth_factor = 10;
        state.current_batch_size = 1000;

        // Throughput of 1000 rows/sec
        state
            .execution_times
            .push_back((500, Duration::from_secs_f64(0.5)));
        state
            .execution_times
            .push_back((1000, Duration::from_secs_f64(1.0)));

        state.adjust_batch_size();

        // Next would be 10,000 rows = 10 seconds. Should cap to ~1000 rows for 1 second
        assert!(state.current_batch_size <= 1000);
    }

    #[rstest]
    fn test_min_measurements_requirement(mut state: UdfState) {
        state.dynamic_batching.min_measurements_per_size = 10; // Need 10 measurements
        state.dynamic_batching.min_sample_size = 50;
        state.current_batch_size = 256;

        // Add 9 measurements for current size (not enough)
        for _ in 0..9 {
            state
                .execution_times
                .push_back((256, Duration::from_secs_f64(0.1)));
        }

        // Add many measurements for other sizes
        for _ in 0..30 {
            state
                .execution_times
                .push_back((512, Duration::from_secs_f64(0.1)));
            state
                .execution_times
                .push_back((1024, Duration::from_secs_f64(0.1)));
        }

        state.adjust_batch_size();

        // Should stay at 256 to get more measurements
        assert_eq!(state.current_batch_size, 256);
    }

    #[rstest]
    fn test_fast_exploration_duration(mut state: UdfState) {
        state.dynamic_batching.min_sample_size = 10; // Short exploration phase
        state.dynamic_batching.min_measurements_per_size = 1;
        state.dynamic_batching.growth_factor = 2;
        state.current_batch_size = 1;

        // Run through fast exploration
        for _ in 0..9 {
            let size = state.current_batch_size;
            state
                .execution_times
                .push_back((size, Duration::from_secs_f64(0.01)));

            state.adjust_batch_size();
        }
        // 1 -> 2 -> 4 -> 8 -> 16 -> 32 -> 64 -> 128 -> 256 -> 512
        assert!(state.current_batch_size == 512);
    }

    #[rstest]
    fn test_convergence_with_tight_latency(mut state: UdfState) {
        state.dynamic_batching.max_latency_s = 0.1; // Very tight 100ms latency
        state.dynamic_batching.min_sample_size = 24;
        state.dynamic_batching.min_measurements_per_size = 2;

        state.dynamic_batching.max_size = 100_000;
        state.current_batch_size = 1;
        // We should reach convergence as soon as we're done sampling.
        // idx, size
        // 1 4 4 16 16 64 64 256 256 -> 992 ... (24) 992
        // Throughput of 10,000 rows/sec
        for _ in 0..23 {
            let size = state.current_batch_size;
            state
                .execution_times
                .push_back((size, Duration::from_secs_f64(size as f64 / 10_000.0)));
            state.adjust_batch_size();
        }

        let final_size = state.current_batch_size;
        // With 10k rows/sec and 0.1s latency, max size should converge on 896
        assert!(final_size == 992);

        // make sure we've converged
        for _ in 0..100 {
            let size = state.current_batch_size;
            assert!(size == 992);

            state
                .execution_times
                .push_back((size, Duration::from_secs_f64(size as f64 / 10_000.0)));
            state.adjust_batch_size();
        }
    }

    #[rstest]
    fn test_adjust_batch_size_initial_bootstrap(mut state: UdfState) {
        state.current_batch_size = 100;
        state.dynamic_batching.max_size = 10000;

        // First adjustment with no measurements
        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 400);

        // Second adjustment with 1 measurement
        state
            .execution_times
            .push_back((400, Duration::from_secs_f64(0.1)));
        state.adjust_batch_size();
        assert_eq!(state.current_batch_size, 1600);
    }

    #[rstest]
    fn test_adjust_batch_size_duration_capping(mut state: UdfState) {
        state.current_batch_size = 2000;
        state.dynamic_batching.max_size = 100000;
        state.dynamic_batching.min_size = 10;

        // Add measurements showing current throughput of 400 rows/sec
        state
            .execution_times
            .push_back((1000, Duration::from_secs_f64(2.5)));
        state
            .execution_times
            .push_back((2000, Duration::from_secs_f64(4.0)));

        // Next growth would be 8000, taking 16 seconds - way over 5 second limit
        state.adjust_batch_size();

        let new_size = state.current_batch_size;

        // Should reduce to fit within 5 seconds
        assert!(new_size <= 2000);
    }

    #[rstest]
    fn test_adjust_batch_size_should_explore_with_constant_throughput(mut state: UdfState) {
        state.current_batch_size = 1;
        state.dynamic_batching.max_size = 5_000_000;
        state.dynamic_batching.min_size = 1;

        for _ in 0..500 {
            let size = state.current_batch_size;
            state
                .execution_times
                .push_back((size, Duration::from_secs_f64(size as f64 / 1_000_000.0)));
            state.adjust_batch_size();
        }

        let final_size = state.current_batch_size;
        // Constant throughput of 1,000,000 rows/sec
        // At this throughput, 5 seconds = 5,000,000 rows

        // Growth pattern with GROWTH_FACTOR=4:
        // 1 → 4 → 16 → 64 → 256 → 1024 → 4096 → 16384 → 65536 → 262144 → 1048576 → 4194304

        // At size 4194304:
        // - Next growth would be 4194304 * 4 = 16,777,216
        // - Predicted time = 16,777,216 / 1,000,000 = 16.78 seconds
        // - This exceeds MAX_TARGET_DURATION (5.0 seconds)
        // - So it calculates: target_size_for_duration = 1,000,000 * 5.0 = 5,000,000
        // - Then applies to_efficient_batch_size(5,000,000) = 4,500,032

        // After reaching 4,500,032:
        // - It stays there because growing would exceed 5 seconds
        // - 4,500,032 * 4 = 18,000,128 rows → 18 seconds (too long)
        // - And the limited exploration (±10%) also can't grow past 5 second limit
        assert_eq!(
            final_size, 4_500_032,
            "Converges at 4,500,032 - the efficient batch size closest to 5M rows (5 sec limit)"
        );
    }
}
