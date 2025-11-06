use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
    vec,
};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_resource_request::ResourceRequest;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_core::series::Series;
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    Column, Expr, ExprRef,
    common_treenode::{Transformed, TreeNode},
    expr::{BoundColumn, bound_expr::BoundExpr},
    functions::python::UDFProperties,
};
use daft_micropartition::MicroPartition;
#[cfg(feature = "python")]
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
};

/// Given an expression, extract the indexes of used columns and remap them to
/// new indexes from 0...count-1, where count is the # of used columns.
///
/// Note that if there are no used columns, we just return the first
/// because we can't execute UDFs on empty recordbatches.
pub(crate) fn remap_used_cols(expr: BoundExpr) -> (BoundExpr, Vec<usize>) {
    let mut count = 0;
    let mut cols_to_idx = HashMap::new();
    let new_expr = expr
        .into_inner()
        .transform_down(|expr: ExprRef| {
            if let Expr::Column(Column::Bound(BoundColumn { index, field })) = expr.as_ref() {
                if !cols_to_idx.contains_key(index) {
                    cols_to_idx.insert(*index, count);
                    count += 1;
                }

                let new_index = cols_to_idx[index];
                Ok(Transformed::yes(Arc::new(Expr::Column(Column::Bound(
                    BoundColumn {
                        index: new_index,
                        field: field.clone(),
                    },
                )))))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .expect("Error occurred when visiting for required columns");

    let required_cols = if cols_to_idx.is_empty() {
        vec![0]
    } else {
        let mut required_cols = vec![0; count];
        for (original_idx, final_idx) in cols_to_idx {
            required_cols[final_idx] = original_idx;
        }
        required_cols
    };

    (BoundExpr::new_unchecked(new_expr.data), required_cols)
}

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

/// Common parameters for UDF handle and operator
struct UdfParams {
    expr: BoundExpr,
    udf_properties: UDFProperties,
    passthrough_columns: Vec<BoundExpr>,
    output_schema: SchemaRef,
    required_cols: Vec<usize>,
}

pub(crate) struct UdfHandle {
    params: Arc<UdfParams>,
    udf_expr: BoundExpr,
    worker_idx: usize,
    // Optional PyObject handle to external UDF worker.
    // Required for ActorPoolUDFs
    // Optional for stateless UDFs
    //   - Starts as None indicating that the UDF is run in-line with the thread
    //   - If excessive GIL contention is detected, the UDF will be moved to an external worker
    // Second bool indicates if the UDF was initialized
    #[cfg(feature = "python")]
    handle: Option<Py<PyAny>>,
    current_batch_size: NonZeroUsize,
    execution_times: VecDeque<(usize, Duration)>, // (batch_size, duration) pairs
    dynamic_batching: DynamicBatchingConfig,
}

impl UdfHandle {
    fn no_handle(params: Arc<UdfParams>, udf_expr: BoundExpr, worker_idx: usize) -> Self {
        Self {
            params,
            udf_expr,
            worker_idx,
            #[cfg(feature = "python")]
            handle: None,
            current_batch_size: NonZeroUsize::new(1).unwrap(),
            execution_times: VecDeque::new(),
            dynamic_batching: DynamicBatchingConfig::default(),
        }
    }

    fn create_handle(&mut self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let py_expr = PyExpr::from(self.udf_expr.inner().clone());

            let handle = Python::attach(|py| {
                // create python object
                Ok::<pyo3::Py<pyo3::PyAny>, PyErr>(
                    py.import(pyo3::intern!(py, "daft.execution.udf"))?
                        .getattr(pyo3::intern!(py, "UdfHandle"))?
                        .call1((py_expr,))?
                        .unbind(),
                )
            })?;

            self.handle = Some(handle);
        }

        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot create a UDF handle without compiling for Python");
        }

        Ok(())
    }

    #[cfg(feature = "python")]
    fn eval_input_with_handle(
        &self,
        input: RecordBatch,
        handle: &pyo3::Py<pyo3::PyAny>,
    ) -> DaftResult<Series> {
        use daft_recordbatch::python::PyRecordBatch;

        use crate::STDOUT;

        let (result, stdout_lines) = Python::attach(|py| {
            handle
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "eval_input"),
                    (PyRecordBatch::from(input),),
                )?
                .extract::<(PyRecordBatch, Vec<String>)>()
        })?;

        let label = format!(
            "[`{}` Worker #{}]",
            self.params.udf_properties.name, self.worker_idx
        );
        for line in stdout_lines {
            STDOUT.print(&label, &line);
        }

        let result: RecordBatch = result.into();
        debug_assert!(
            result.num_columns() == 1,
            "UDF should return a single column"
        );
        Ok(result.get_column(0).clone())
    }

    #[cfg(feature = "python")]
    fn eval_input_inline(&mut self, func_input: RecordBatch) -> DaftResult<Series> {
        use daft_dsl::functions::python::initialize_udfs;

        // Only actually initialized the first time
        self.udf_expr = BoundExpr::new_unchecked(initialize_udfs(self.udf_expr.inner().clone())?);
        func_input.eval_expression(&self.udf_expr)
    }

    #[cfg(not(feature = "python"))]
    fn eval_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        panic!("Cannot evaluate a UDF without compiling for Python");
    }

    #[cfg(feature = "python")]
    fn eval_input(&mut self, input: Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        if self.params.udf_properties.batch_size.is_none() {
            self.eval_input_adaptive_batching(input)
        } else {
            let input_batches = input.get_tables()?;
            let mut output_batches = Vec::with_capacity(input_batches.len());

            for batch in input_batches.as_ref() {
                // Prepare inputs
                let func_input = batch.get_columns(self.params.required_cols.as_slice());

                // Call the UDF
                let mut result = if let Some(handle) = &self.handle {
                    self.eval_input_with_handle(func_input, handle)
                } else {
                    self.eval_input_inline(func_input)
                }?;
                // If result.len() == 1 (because it was a 0-column UDF), broadcast to right size
                if result.len() == 1 {
                    result = result.broadcast(batch.num_rows())?;
                }

                // Append result to passthrough
                let passthrough_input =
                    batch.eval_expression_list(self.params.passthrough_columns.as_slice())?;
                let output_batch =
                    passthrough_input.append_column(self.params.output_schema.clone(), result)?;
                output_batches.push(output_batch);
            }

            Ok(Arc::new(MicroPartition::new_loaded(
                self.params.output_schema.clone(),
                Arc::new(output_batches),
                None,
            )))
        }
    }

    fn eval_input_adaptive_batching(
        &mut self,
        input: Arc<MicroPartition>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let input_batches = input.get_tables()?;
        let mut output_batches = Vec::with_capacity(input_batches.len());
        for batch in input_batches.as_ref() {
            // Prepare inputs
            let func_input = batch.get_columns(self.params.required_cols.as_slice());
            let total_rows = func_input.num_rows();
            let mut row_offset = 0;
            let mut results = Vec::new();

            while row_offset < total_rows {
                let batch_size = self.current_batch_size.get().min(total_rows - row_offset);

                let sub_batch = func_input.slice(row_offset, row_offset + batch_size)?;
                let num_rows = sub_batch.num_rows();
                let start_time = std::time::Instant::now();
                let mut sub_result = if let Some(handle) = &self.handle {
                    self.eval_input_with_handle(sub_batch, handle)
                } else {
                    self.eval_input_inline(sub_batch)
                }?;
                let duration = start_time.elapsed();
                self.execution_times.push_back((batch_size, duration));
                if self.execution_times.len() > self.dynamic_batching.timing_window {
                    self.execution_times.pop_front();
                }
                // If result.len() == 1 (because it was a 0-column UDF), broadcast to right size
                if sub_result.len() == 1 {
                    sub_result = sub_result.broadcast(num_rows)?;
                }

                // Adjust batch size based on performance
                let prev = self.current_batch_size;
                self.adjust_batch_size();
                if self.current_batch_size != prev {
                    log::debug!(
                        "{}: Changed Batch Size from {} -> {}",
                        std::thread::current().name().unwrap_or("Unknown"),
                        prev,
                        self.current_batch_size
                    );
                }
                results.push(sub_result);
                row_offset += batch_size;
            }

            let results_slice = results.iter().collect::<Vec<_>>();

            // Combine results
            let result = Series::concat(results_slice.as_ref())?;

            // Append result to passthrough
            let passthrough_input =
                batch.eval_expression_list(self.params.passthrough_columns.as_slice())?;
            let output_batch =
                passthrough_input.append_column(self.params.output_schema.clone(), result)?;
            output_batches.push(output_batch);
        }

        Ok(Arc::new(MicroPartition::new_loaded(
            self.params.output_schema.clone(),
            Arc::new(output_batches),
            None,
        )))
    }

    fn teardown(&self) -> DaftResult<()> {
        #[cfg(feature = "python")]
        {
            let Some(handle) = &self.handle else {
                return Ok(());
            };

            Python::attach(|py| {
                handle
                    .bind(py)
                    .call_method0(pyo3::intern!(py, "teardown"))?;
                Ok(())
            })
        }

        #[cfg(not(feature = "python"))]
        {
            Ok(())
        }
    }

    /// Dynamically adjusts UDF batch size to maximize throughput while respecting user experience constraints.
    ///
    /// This adaptive algorithm optimizes batch sizes for User-Defined Functions (UDFs) by exploring different
    /// batch sizes and measuring their throughput (rows processed per second). The goal is to find the batch
    /// size that maximizes processing efficiency while ensuring individual batches don't take too long, which
    /// would create a poor user experience with stalled progress indicators.
    /// ///
    /// ## Algorithm Overview
    ///
    /// The algorithm operates in several distinct phases:
    ///
    /// 1. **Initial Growth Phase** (samples < 2):
    ///    - Aggressively grows batch size by `GROWTH_FACTOR` (4x) to quickly escape small batch sizes
    ///    - No measurement analysis at this stage
    ///
    /// 2. **Fast Exploration Phase** (samples < MIN_SAMPLE_SIZE × MIN_MEASUREMENTS_PER_SIZE):
    ///    - Continues exponential growth by `GROWTH_FACTOR` to explore the performance landscape
    ///    - Includes duration limit checking to prevent batches that would exceed `MAX_TARGET_DURATION`
    ///    - If next batch would be too slow, calculates and jumps to duration-optimal size
    ///    - Requires `MIN_MEASUREMENTS_PER_SIZE` measurements per batch size for statistical reliability
    ///
    /// 3. **Convergence Detection Phase** (samples ≥ MIN_SAMPLE_SIZE × MIN_MEASUREMENTS_PER_SIZE):
    ///    - Analyzes all reliable measurements (those with ≥ MIN_MEASUREMENTS_PER_SIZE samples)
    ///    - Moves to significantly better batch sizes (>20% throughput improvement)
    ///    - Detects convergence when throughput variation across all sizes is <5%
    ///    - Converges when current throughput is within 5% of best observed
    ///
    /// 4. **Limited Exploration Phase** (when at best known size):
    ///    - Makes small exploratory moves (±10%) around the best known batch size
    ///    - Only explores sizes that haven't been adequately measured yet
    ///    - Designed to fine-tune performance around the optimal region
    ///
    /// ## Key Metrics & Constraints
    ///
    /// - **Primary Metric**: Throughput (rows processed per second)
    /// - **User Experience Constraint**: No batch should take longer than `MAX_TARGET_DURATION` seconds
    /// - **Statistical Reliability**: Each batch size needs `MIN_MEASUREMENTS_PER_SIZE` measurements
    /// - **Convergence Threshold**: <5% variation in throughput across different batch sizes
    ///
    /// ## Algorithm Parameters
    ///
    /// - `GROWTH_FACTOR` (4): Exponential growth rate during exploration phases
    /// - `MAX_TARGET_DURATION` (5.0s): Maximum acceptable time per batch for good UX
    /// - `MIN_SAMPLE_SIZE` (24): Minimum batch sizes to test before considering convergence
    /// - `MIN_MEASUREMENTS_PER_SIZE` (4): Required measurements per batch size for reliability
    ///
    /// ## Behavior Characteristics
    ///
    /// **Exploration Strategy**:
    /// - Starts with aggressive growth to quickly find the performance region
    /// - Switches to conservative exploration once duration limits are encountered
    /// - Makes statistical decisions based on multiple measurements per batch size
    ///
    /// **Duration Limit Handling**:
    /// - Predicts if next batch size would exceed duration limits
    /// - Calculates duration-optimal batch size using current throughput measurements
    /// - Caps optimal size to current size when growth would be counterproductive
    ///
    /// **Convergence Criteria**:
    /// - Requires significant throughput differences (>20%) to change batch sizes
    /// - Detects convergence through low throughput variation across measurements
    /// - Stays at current size when within 5% of best performance
    ///
    /// **Statistical Reliability**:
    /// - Averages multiple measurements per batch size to handle timing variance
    /// - Only makes decisions on batch sizes with adequate sample counts
    /// - Filters unreliable measurements from optimization decisions
    ///
    /// ## Expected Outcomes
    ///
    /// For typical UDFs, the algorithm should:
    /// - Converge within 64-256 total measurements (16 sizes × 4 measurements each)
    /// - Find batch sizes that maximize throughput while respecting duration constraints
    /// - Provide stable performance once converged (no oscillation)
    /// - Adapt to different UDF performance characteristics (fast vs slow functions)
    ///
    /// The algorithm is designed to work across a wide range of UDF types, from fast transformations
    /// that benefit from large batch sizes to slow model inference that requires smaller batches
    /// due to duration constraints.
    fn adjust_batch_size(&mut self) {
        const GROWTH_FACTOR: usize = 4;
        const MAX_TARGET_DURATION: f64 = 20.0;
        const MIN_SAMPLE_SIZE: usize = 24;
        const MIN_MEASUREMENTS_PER_SIZE: usize = 4; // Require at least 4 measurements per batch size
        fn to_efficient_batch_size(size: usize) -> NonZeroUsize {
            if size == 0 {
                return NonZeroUsize::new(2).unwrap();
            }

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
            NonZeroUsize::new(rounded.max(multiple)).unwrap()
        }
        if self.execution_times.len() < 2 {
            // Initial growth phase
            let new_size =
                (self.current_batch_size.get() * GROWTH_FACTOR).min(self.dynamic_batching.max_size);
            self.current_batch_size = to_efficient_batch_size(new_size);
            return;
        }

        // Calculate throughput for recent executions
        let throughputs: Vec<(usize, f64)> = self
            .execution_times
            .iter()
            .map(|(size, time)| (*size, *size as f64 / time.as_secs_f64()))
            .collect();

        // Group measurements by batch size and calculate averages
        let mut size_measurements: HashMap<usize, Vec<f64>> = HashMap::new();
        for (size, throughput) in &throughputs {
            size_measurements
                .entry(*size)
                .or_insert_with(Vec::new)
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

        // Print measurement counts for each batch size
        let measurement_counts_str = avg_throughputs
            .iter()
            .map(|(size, avg_throughput, count)| {
                format!("{}=({:.2}, {}x)", size, avg_throughput, count)
            })
            .collect::<Vec<_>>()
            .join(", ");
        log::debug!(
            "{}: Avg Throughputs (size=(rows/sec, count)): {}",
            std::thread::current().name().unwrap_or("unknown"),
            measurement_counts_str
        );

        // Get current size and throughput
        let current_size = self.current_batch_size.get();
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

                    self.current_batch_size = new_size;
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
                self.current_batch_size = to_efficient_batch_size(best_size);
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

        // Limited exploration - explore around best size
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
            if !sizes_tried.contains(&larger.get()) {
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
            if !sizes_tried.contains(&smaller.get()) {
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
        self.current_batch_size = to_efficient_batch_size(best_size);
    }
}

impl Drop for UdfHandle {
    fn drop(&mut self) {
        let result = self.teardown();

        if let Err(e) = result {
            log::error!("Error tearing down UDF actor: {}", e);
        }
    }
}

/// Each UdfState holds a handle to a single Python process.
/// The concurrency of the Python process pool is thus tied to the concurrency of the operator
/// and the local executor handles task scheduling.
pub(crate) struct UdfState {
    udf_handle: UdfHandle,
}

pub(crate) struct UdfOperator {
    params: Arc<UdfParams>,
    worker_count: AtomicUsize,
    concurrency: usize,
    memory_request: u64,
    input_schema: SchemaRef,
}

impl UdfOperator {
    pub fn try_new(
        expr: BoundExpr,
        udf_properties: UDFProperties,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
        input_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        // Determine optimal parallelism
        let resource_request = udf_properties.resource_request.as_ref();
        let max_concurrency =
            Self::get_optimal_allocation(udf_properties.name.as_str(), resource_request)?;
        // If parallelism is already specified, use that
        let concurrency = udf_properties.concurrency.unwrap_or(max_concurrency);

        let memory_request = resource_request
            .and_then(|req| req.memory_bytes())
            .map(|m| m as u64)
            .unwrap_or(0);

        let (expr, required_cols) = remap_used_cols(expr);

        Ok(Self {
            params: Arc::new(UdfParams {
                expr,
                udf_properties,
                passthrough_columns,
                output_schema: output_schema.clone(),
                required_cols,
            }),
            worker_count: AtomicUsize::new(0),
            concurrency,
            memory_request,
            input_schema: input_schema.clone(),
        })
    }

    // This function is used to determine the optimal allocation of concurrency and expression parallelism
    fn get_optimal_allocation(
        full_name: &str,
        resource_request: Option<&ResourceRequest>,
    ) -> DaftResult<usize> {
        let num_cpus = get_compute_pool_num_threads();
        // The number of CPUs available for the operator.
        let available_cpus = match resource_request {
            // If the resource request specifies a number of CPUs, the available cpus is the number of actual CPUs
            // divided by the requested number of CPUs, clamped to (1, NUM_CPUS).
            // E.g. if the resource request specifies 2 CPUs and NUM_CPUS is 4, the number of available cpus is 2.
            Some(resource_request) if resource_request.num_cpus().is_some() => {
                let requested_num_cpus = resource_request.num_cpus().unwrap();
                if requested_num_cpus > num_cpus as f64 {
                    Err(DaftError::ValueError(format!(
                        "`{full_name}` requested {requested_num_cpus} CPUs but found only {num_cpus} available"
                    )))
                } else {
                    Ok((num_cpus as f64 / requested_num_cpus).clamp(1.0, num_cpus as f64) as usize)
                }
            }
            _ => Ok(num_cpus),
        }?;

        Ok(available_cpus)
    }
}

impl IntermediateOperator for UdfOperator {
    type State = UdfState;

    #[instrument(skip_all, name = "UdfOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let memory_request = self.memory_request;
        let fut = task_spawner.spawn_with_memory_request(
            memory_request,
            async move {
                let res = state
                    .udf_handle
                    .eval_input(input)
                    .map(|result| IntermediateOperatorResult::NeedMoreInput(Some(result)))?;
                Ok((state, res))
            },
            Span::current(),
        );
        fut.into()
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

        Ok(UdfState { udf_handle })
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(self.concurrency)
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.params
            .udf_properties
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }
}
