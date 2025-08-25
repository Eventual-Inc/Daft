use std::{sync::Arc, time::Duration, vec};

use common_error::DaftResult;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
#[cfg(feature = "python")]
use daft_core::python::{PyDataType, PySeries};
use daft_dsl::{expr::bound_expr::BoundExpr, python_udf::GpuUdf};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
#[cfg(feature = "python")]
use pyo3::{types::PyAnyMethods, PyErr, PyObject, Python};
use tracing::{instrument, Span};

use crate::{
    ops::NodeType,
    pipeline::{MorselSizeRequirement, NodeName},
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    },
    ExecutionTaskSpawner,
};

struct GPUArgs {
    passthrough_columns: Vec<BoundExpr>,
    out_name: Arc<str>,
    gpu_udf: GpuUdf,
}

// Init:
// 1. Create the model via init_args on the device
// 2. Create CUDA streams
// Iteration:
// 1. Load input to input buffer
// 2. While there are unused CUDA streams:
//    a. Pop batch from input buffer
//    b. Start up inference with stream
// 3. Check all active streams if any are done
//    a. If any are done, get results and append to output buffer
// 4. Return output buffer if non-empty

struct StreamState {
    input: Option<RecordBatch>,
    #[cfg(feature = "python")]
    py: PyObject,
}

pub struct GPUSinkState {
    // Args
    args: Arc<GPUArgs>,
    // Input buffer
    input_buffer: Vec<RecordBatch>,
    // Initial Value
    #[cfg(feature = "python")]
    init_arg: PyObject,
    // Track running streams
    stream_states: Vec<StreamState>,
}

impl GPUSinkState {
    #[cfg(feature = "python")]
    fn new(args: Arc<GPUArgs>) -> Self {
        let init_arg = Python::with_gil(|py| {
            use pyo3::PyErr;

            let init_fn = args.gpu_udf.init_fn.as_ref().bind(py);
            Ok::<_, PyErr>(init_fn.call0()?.unbind())
        })
        .unwrap();

        let num_streams = get_compute_pool_num_threads();

        let stream_states = (0..num_streams)
            .into_iter()
            .map(|_| StreamState {
                input: None,
                #[cfg(feature = "python")]
                py: Python::with_gil(|py| {
                    use pyo3::PyErr;

                    Ok::<_, PyErr>(
                        py.import(pyo3::intern!(py, "daft.udf.gpu"))?
                            .getattr(pyo3::intern!(py, "StreamManager"))?
                            .call1((args.gpu_udf.device.as_ref(), args.gpu_udf.inner.as_ref()))?
                            .unbind(),
                    )
                })
                .unwrap(),
            })
            .collect();

        Self {
            args,
            input_buffer: vec![],
            init_arg,
            stream_states,
        }
    }

    pub fn append_to_input(&mut self, mp: Arc<MicroPartition>) -> DaftResult<()> {
        let io_stats = IOStatsContext::new("GPUSinkState::append_to_input");

        if let Some(rb) = mp.concat_or_get(io_stats)? {
            // TODO: Only save passthrough and input columns
            self.input_buffer.push(rb);
        }

        Ok(())
    }

    #[cfg(feature = "python")]
    pub fn start_stream(&mut self) -> DaftResult<()> {
        for stream_state in &mut self.stream_states {
            if stream_state.input.is_none()
                && let Some(rb) = self.input_buffer.pop()
            {
                let input_expr = BoundExpr::new_unchecked(self.args.gpu_udf.arg.clone());
                let input_col = rb.eval_expression(&input_expr)?;

                Python::with_gil(|py| {
                    let py_state = stream_state.py.bind(py);
                    let py_init_arg = self.init_arg.bind(py);
                    let py_series = PySeries::from(input_col);

                    let _out = py_state
                        .call_method1(pyo3::intern!(py, "start"), (py_init_arg, py_series))?;

                    Ok::<_, PyErr>(())
                })?;

                stream_state.input = Some(rb);
            }
        }

        Ok(())
    }

    #[cfg(not(feature = "python"))]
    pub fn start_stream(&mut self) -> DaftResult<()> {
        panic!("GPU UDFs are not supported without Python support");
    }

    #[cfg(feature = "python")]
    pub fn get_completed(&mut self) -> DaftResult<Option<Vec<RecordBatch>>> {
        use crate::STDOUT;

        let mut completed_streams = vec![];
        for stream_state in &mut self.stream_states {
            if stream_state.input.is_none() {
                continue;
            }

            let out = Python::with_gil(|py| {
                let py_state = stream_state.py.bind(py);
                let py_dtype = PyDataType::from(self.args.gpu_udf.return_dtype.clone());

                let out = py_state.call_method1(pyo3::intern!(py, "get_output"), (py_dtype,))?;

                out.extract::<Option<(PySeries, f64, f64, f64)>>()
            })?;

            if let Some((output, h2d_time, func_time, d2h_time)) = out {
                let output_col = output.series.rename(self.args.out_name.as_ref());
                let input_rb = stream_state.input.take().unwrap();
                let output_rb = input_rb.eval_expression_list(&self.args.passthrough_columns)?;

                completed_streams.push(output_rb.append_column(output_col)?);
                STDOUT.print(
                    "GPU UDF",
                    &format!("h2d: {h2d_time}, func: {func_time}, d2h: {d2h_time}"),
                );
            }
        }

        Ok(if completed_streams.is_empty() {
            None
        } else {
            Some(completed_streams)
        })
    }

    #[cfg(not(feature = "python"))]
    pub fn get_completed(&mut self) -> DaftResult<Option<Vec<RecordBatch>>> {
        panic!("GPU UDFs are not supported without Python support");
    }

    pub fn any_running(&self) -> bool {
        self.stream_states.iter().any(|s| s.input.is_some())
    }

    pub fn all_running(&self) -> bool {
        self.stream_states.iter().all(|s| s.input.is_some())
    }
}

pub struct GPUOperator {
    args: Arc<GPUArgs>,
    output_schema: SchemaRef,
}

impl GPUOperator {
    pub fn new(
        gpu_udf: GpuUdf,
        out_name: Arc<str>,
        passthrough_columns: Vec<BoundExpr>,
        output_schema: &SchemaRef,
    ) -> Self {
        Self {
            args: Arc::new(GPUArgs {
                passthrough_columns,
                out_name,
                gpu_udf,
            }),
            output_schema: output_schema.clone(),
        }
    }
}

impl StreamingSink for GPUOperator {
    type State = GPUSinkState;

    #[instrument(skip_all, name = "GPUOperator::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: GPUSinkState,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let output_schema = self.output_schema.clone();

        spawner
            .spawn(
                async move {
                    state.append_to_input(input)?;

                    // Start up a new stream
                    state.start_stream()?;

                    // Pause if all streams are running
                    if !state.all_running() {
                        return Ok((state, StreamingSinkOutput::HasMoreOutput(None)));
                    }

                    // Get any completed streams
                    let completed = state.get_completed()?;
                    Ok((
                        state,
                        StreamingSinkOutput::NeedMoreInput(completed.map(|batches| {
                            Arc::new(MicroPartition::new_loaded(
                                output_schema.clone(),
                                batches.into(),
                                None,
                            ))
                        })),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize(
        &self,
        mut states: Vec<GPUSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        assert_eq!(states.len(), 1);
        let mut state = states.pop().unwrap();
        let output_schema = self.output_schema.clone();

        spawner
            .spawn(
                async move {
                    let mut completed = vec![];

                    while state.any_running() {
                        if let Some(batches) = state.get_completed()? {
                            completed.extend(batches);
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }

                    if completed.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(Arc::new(MicroPartition::new_loaded(
                            output_schema.clone(),
                            completed.into(),
                            None,
                        ))))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("GPU UDF {}", self.args.gpu_udf.function_name).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::GPUProject
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("GPU UDF: {}", self.args.gpu_udf.function_name),
            format!("Expr = {}", self.args.gpu_udf.arg),
            format!(
                "Passthrough Columns = {}",
                self.args.passthrough_columns.iter().join(", ")
            ),
        ]
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> Self::State {
        #[cfg(feature = "python")]
        {
            GPUSinkState::new(self.args.clone())
        }
        #[cfg(not(feature = "python"))]
        {
            panic!("GPU UDFs are not supported without Python support");
        }
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        Some(MorselSizeRequirement::Strict(64))
    }
}
