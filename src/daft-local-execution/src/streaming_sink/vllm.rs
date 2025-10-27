use std::{collections::VecDeque, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::{AsArrow, BooleanArray, DataType, SchemaRef, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::expr::{
    VLLMExpr,
    bound_expr::{BoundExpr, BoundVLLMExpr},
};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{intern, prelude::*};
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
        StreamingSinkFinalizeResult, StreamingSinkOutput,
    },
};

pub(crate) struct VLLMSink {
    expr: BoundVLLMExpr,
    schema: SchemaRef,
}

pub(crate) struct VLLMState {
    executor: VLLMExecutor,
    buffer: VecDeque<Arc<MicroPartition>>,
    buffer_size: usize,
}

#[cfg(feature = "python")]
struct VLLMExecutor(Py<PyAny>);

#[cfg(not(feature = "python"))]
struct VLLMExecutor;

#[cfg(feature = "python")]
impl VLLMExecutor {
    fn new(expr: &VLLMExpr) -> DaftResult<Self> {
        let executor = Python::attach(|py| {
            py.import(intern!(py, "daft.execution.vllm"))?
                .getattr(intern!(py, "VLLMExecutor"))?
                .call1((
                    &expr.model,
                    expr.engine_args.as_ref(),
                    expr.generate_args.as_ref(),
                ))
                .map(Bound::unbind)
        })?;
        Ok(Self(executor))
    }

    fn submit(&self, prompts: Vec<&str>, rows: RecordBatch) -> DaftResult<()> {
        use daft_recordbatch::python::PyRecordBatch;

        let py_rows = PyRecordBatch { record_batch: rows };

        Python::attach(|py| {
            let recordbatch_class = py
                .import(intern!(py, "daft.recordbatch"))?
                .getattr(intern!(py, "RecordBatch"))?;
            let recordbatch =
                recordbatch_class.call_method1(intern!(py, "_from_pyrecordbatch"), (py_rows,))?;

            self.0
                .call_method1(py, intern!(py, "submit"), (prompts, recordbatch))
        })?;
        Ok(())
    }

    fn poll(&self, schema: SchemaRef) -> DaftResult<Option<RecordBatch>> {
        Python::attach(|py| {
            use daft_recordbatch::python::PyRecordBatch;

            let output = self.0.bind(py).call_method0(intern!(py, "poll"))?;

            if output.is_none() {
                return Ok(None);
            }

            let (outputs, rows): (Vec<String>, Bound<PyAny>) = output.extract()?;

            let rows = rows
                .getattr(intern!(py, "_recordbatch"))?
                .extract::<PyRecordBatch>()?
                .record_batch;

            let output_series =
                Utf8Array::from(("daft_vllm_output", outputs.as_slice())).into_series();

            let output_batch = rows.append_column(schema, output_series)?;
            Ok(Some(output_batch))
        })
    }

    fn num_running_tasks(&self) -> DaftResult<usize> {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call_method0(intern!(py, "num_running_tasks"))?
                .extract::<usize>()
        })
        .map_err(Into::into)
    }
}

#[cfg(not(feature = "python"))]
impl VLLMExecutor {
    fn new(_expr: &VLLMExpr) -> DaftResult<Self> {
        Ok(Self)
    }

    fn submit(&self, _prompts: Vec<&str>, _rows: RecordBatch) -> DaftResult<()> {
        unimplemented!("VLLMExecutor::submit is not supported without the Python feature.")
    }

    fn poll(&self, _schema: SchemaRef) -> DaftResult<Option<RecordBatch>> {
        unimplemented!("VLLMExecutor::poll is not supported without the Python feature.")
    }

    fn num_running_tasks(&self) -> DaftResult<usize> {
        unimplemented!(
            "VLLMExecutor::num_running_tasks is not supported without the Python feature."
        )
    }
}

impl VLLMSink {
    pub(crate) fn new(expr: BoundVLLMExpr, schema: SchemaRef) -> Self {
        Self { expr, schema }
    }

    fn submit_tasks(
        state: &mut VLLMState,
        max_running_tasks: usize,
        expr_input: BoundExpr,
        schema: SchemaRef,
    ) -> DaftResult<Vec<RecordBatch>> {
        let io_stats = IOStatsContext::new("VLLMSink::execute");

        let mut available_tasks =
            max_running_tasks as isize - state.executor.num_running_tasks()? as isize;

        let mut null_rows = Vec::new();

        while available_tasks > 0 && !state.buffer.is_empty() {
            let mp = state.buffer.pop_front().unwrap();
            state.buffer_size -= mp.len();
            available_tasks -= mp.len() as isize;

            let Some(rb) = mp.concat_or_get_update(io_stats.clone())? else {
                // micropartition is empty, don't submit to executor
                continue;
            };

            let prompt_series = rb.eval_expression(&expr_input)?;
            let (prompt_series, rb) = if let Some(validity) = prompt_series.validity()
                && validity.unset_bits() > 0
            {
                let valid_mask = BooleanArray::from(("valid_mask", validity.clone()));
                let invalid_mask = BooleanArray::from(("valid_mask", !validity)).into_series();

                let nulls = rb.mask_filter(&invalid_mask)?;
                null_rows.push(nulls);

                let valid_prompt_series = prompt_series.filter(&valid_mask)?;
                let valid_rb = rb.mask_filter(&valid_mask.into_series())?;
                (valid_prompt_series, valid_rb)
            } else {
                (prompt_series, rb)
            };

            let prompts = prompt_series
                .utf8()
                .map_err(|_| {
                    DaftError::type_error(format!(
                        "Expected input to `prompt` to be string, got {}",
                        prompt_series.data_type()
                    ))
                })?
                .as_arrow()
                .values_iter()
                .collect::<Vec<_>>();

            state.executor.submit(prompts, rb)?;
        }

        let null_rows = null_rows
            .into_iter()
            .map(|rb| {
                rb.append_column(
                    schema.clone(),
                    Series::full_null("daft_vllm_output", &DataType::Utf8, rb.len()),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        Ok(null_rows)
    }

    fn combine_output_with_null_rows(
        output_batch: Option<RecordBatch>,
        null_rows: Vec<RecordBatch>,
        schema: SchemaRef,
    ) -> Option<Arc<MicroPartition>> {
        if let Some(output_batch) = output_batch {
            let mut output_batches = null_rows;
            output_batches.push(output_batch);

            Some(Arc::new(MicroPartition::new_loaded(
                schema,
                Arc::new(output_batches),
                None,
            )))
        } else if null_rows.is_empty() {
            None
        } else {
            Some(Arc::new(MicroPartition::new_loaded(
                schema,
                Arc::new(null_rows),
                None,
            )))
        }
    }
}

impl StreamingSink for VLLMSink {
    type State = VLLMState;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let schema = self.schema.clone();
        let expr = self.expr.inner();
        let max_running_tasks = expr.max_running_tasks;
        let max_buffer_size = expr.max_buffer_size;
        let expr_input = BoundExpr::new_unchecked(expr.input.clone());

        spawner
            .spawn(
                async move {
                    let null_rows = Self::submit_tasks(
                        &mut state,
                        max_running_tasks,
                        expr_input,
                        schema.clone(),
                    )?;

                    let output_batch = state.executor.poll(schema.clone())?;

                    let output = Self::combine_output_with_null_rows(
                        output_batch,
                        null_rows,
                        schema.clone(),
                    );

                    if state.buffer_size < max_buffer_size {
                        state.buffer_size += input.len();
                        state.buffer.push_back(input);

                        Ok((state, StreamingSinkOutput::NeedMoreInput(output)))
                    } else {
                        Ok((state, StreamingSinkOutput::HasMoreOutput(output)))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        let schema = self.schema.clone();
        let expr = self.expr.inner();
        let max_running_tasks = expr.max_running_tasks;
        let expr_input = BoundExpr::new_unchecked(expr.input.clone());

        spawner
            .spawn(
                async move {
                    let state = &mut states[0];

                    let null_rows: Vec<RecordBatch> =
                        Self::submit_tasks(state, max_running_tasks, expr_input, schema.clone())?;

                    let finished =
                        state.executor.num_running_tasks()? == 0 && state.buffer_size == 0;

                    let output_batch = state.executor.poll(schema.clone())?;

                    let output = Self::combine_output_with_null_rows(
                        output_batch,
                        null_rows,
                        schema.clone(),
                    );

                    if finished {
                        Ok(StreamingSinkFinalizeOutput::Finished(output))
                    } else {
                        Ok(StreamingSinkFinalizeOutput::HasMoreOutput { states, output })
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        format!("VLLM {}", self.expr).into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::VLLMProject
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("VLLM: {}", self.expr)]
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(VLLMState {
            executor: VLLMExecutor::new(self.expr.inner())?,
            buffer: VecDeque::new(),
            buffer_size: 0,
        })
    }

    fn max_concurrency(&self) -> usize {
        self.expr.inner().concurrency
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.expr
            .inner()
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }
}
