use std::{collections::BinaryHeap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::{AsArrow, SchemaRef, Utf8Array},
    series::IntoSeries,
};
use daft_dsl::{
    expr::{
        VLLMExpr,
        bound_expr::{BoundExpr, BoundVLLMExpr},
    },
    functions::python::RuntimePyObject,
};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
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

#[derive(Clone)]
pub(crate) struct VLLMSink {
    expr: Arc<BoundVLLMExpr>,
    output_column_name: Arc<str>,
    llm_actors: Option<RuntimePyObject>,
    schema: SchemaRef,
}

pub(crate) struct VLLMState {
    executor: VLLMExecutor,
    buffer: Vec<Arc<MicroPartition>>,
    buffer_size: usize,
    finished_submitting: bool,
}

#[cfg(feature = "python")]
struct VLLMExecutor(Py<PyAny>);

#[cfg(not(feature = "python"))]
struct VLLMExecutor;

#[cfg(feature = "python")]
impl VLLMExecutor {
    fn new_local(expr: &VLLMExpr) -> DaftResult<Self> {
        let executor = Python::attach(|py| {
            py.import(intern!(py, "daft.execution.vllm"))?
                .getattr(intern!(py, "LocalVLLMExecutor"))?
                .call1((
                    &expr.model,
                    expr.engine_args.as_ref(),
                    expr.generate_args.as_ref(),
                ))
                .map(Bound::unbind)
        })?;
        Ok(Self(executor))
    }

    fn new_distributed(llm_actors: &RuntimePyObject) -> DaftResult<Self> {
        let executor = Python::attach(|py| {
            py.import(intern!(py, "daft.execution.vllm"))?
                .getattr(intern!(py, "DistributedVLLMExecutor"))?
                .call1((llm_actors.as_ref(),))
                .map(Bound::unbind)
        })?;
        Ok(Self(executor))
    }

    fn teardown(&self) -> DaftResult<()> {
        Python::attach(|py| self.0.call_method0(py, intern!(py, "teardown")))?;
        Ok(())
    }

    fn submit(&self, prefix: String, prompts: Vec<String>, rows: RecordBatch) -> DaftResult<()> {
        use daft_recordbatch::python::PyRecordBatch;

        let py_rows = PyRecordBatch { record_batch: rows };

        Python::attach(|py| {
            let recordbatch_class = py
                .import(intern!(py, "daft.recordbatch"))?
                .getattr(intern!(py, "RecordBatch"))?;
            let recordbatch =
                recordbatch_class.call_method1(intern!(py, "_from_pyrecordbatch"), (py_rows,))?;

            self.0
                .call_method1(py, intern!(py, "submit"), (prefix, prompts, recordbatch))
        })?;
        Ok(())
    }

    fn poll(&self) -> DaftResult<Option<(Vec<String>, RecordBatch)>> {
        Python::attach(|py| {
            use daft_recordbatch::python::PyRecordBatch;

            let output = self.0.bind(py).call_method0(intern!(py, "poll"))?;

            if output.is_none() {
                Ok(None)
            } else {
                let (outputs, rows): (Vec<String>, Bound<PyAny>) = output.extract()?;
                let rows = rows
                    .getattr(intern!(py, "_recordbatch"))?
                    .extract::<PyRecordBatch>()?
                    .record_batch;

                Ok(Some((outputs, rows)))
            }
        })
    }

    fn finished_submitting(&self) -> DaftResult<()> {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call_method0(intern!(py, "finished_submitting"))?;

            Ok::<_, PyErr>(())
        })?;

        Ok(())
    }

    fn all_tasks_finished(&self) -> DaftResult<bool> {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call_method0(intern!(py, "all_tasks_finished"))?
                .extract::<bool>()
        })
        .map_err(Into::into)
    }
}

#[cfg(not(feature = "python"))]
impl VLLMExecutor {
    fn new_local(_expr: &VLLMExpr) -> DaftResult<Self> {
        Ok(Self)
    }

    fn new_distributed(_llm_actors: &RuntimePyObject) -> DaftResult<Self> {
        Ok(Self)
    }

    fn submit(&self, _prefix: String, _prompts: Vec<String>, _rows: RecordBatch) -> DaftResult<()> {
        unimplemented!("VLLMExecutor::submit is not supported without the Python feature.")
    }

    fn poll(&self) -> DaftResult<Option<(Vec<String>, RecordBatch)>> {
        unimplemented!("VLLMExecutor::poll is not supported without the Python feature.")
    }

    fn finished_submitting(&self) -> DaftResult<()> {
        unimplemented!(
            "VLLMExecutor::finished_submitting is not supported without the Python feature."
        )
    }

    fn all_tasks_finished(&self) -> DaftResult<bool> {
        unimplemented!(
            "VLLMExecutor::all_tasks_finished is not supported without the Python feature."
        )
    }

    fn teardown(&self) -> DaftResult<()> {
        unimplemented!("VLLMExecutor::teardown is not supported without the Python feature.")
    }
}

impl VLLMSink {
    pub(crate) fn new(
        expr: Arc<BoundVLLMExpr>,
        output_column_name: Arc<str>,
        llm_actors: Option<RuntimePyObject>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            expr,
            output_column_name,
            llm_actors,
            schema,
        }
    }

    fn poll_tasks(&self, state: &VLLMState) -> DaftResult<Option<Arc<MicroPartition>>> {
        Ok(state.executor.poll()?.map(|(outputs, rows)| {
            let output_series =
                Utf8Array::from((self.output_column_name.as_ref(), outputs.as_slice()))
                    .into_series();

            let rb = rows
                .append_column(self.schema.clone(), output_series)
                .unwrap();

            Arc::new(MicroPartition::new_loaded(
                self.schema.clone(),
                Arc::new(vec![rb]),
                None,
            ))
        }))
    }

    /// Pop values from the buffer for submission until the buffer length is less than the max buffer size.
    ///
    /// Pops them by bucketing based on prefix and submitting the largest buckets first.
    fn pop_tasks(
        &self,
        state: &mut VLLMState,
        max_buffer_size: usize,
    ) -> DaftResult<Vec<(String, Vec<String>, RecordBatch)>> {
        let expr = self.expr.inner();
        let prefix_match_threshold = expr.prefix_match_threshold.0;
        let expr_input = BoundExpr::new_unchecked(expr.input.clone());

        if state.buffer_size > max_buffer_size {
            let concatted = MicroPartition::concat(&state.buffer)?
                .concat_or_get_update(IOStatsContext::new("VLLMSink::pop_tasks"))?
                .unwrap();

            let sorted = concatted.sort(std::slice::from_ref(&expr_input), &[false], &[false])?;

            let prompts_vec = self.get_prompts_for_batch(&sorted)?;

            let mut splits = BinaryHeap::new();
            let mut prev_split_idx = 0;

            for (i, (p1, p2)) in prompts_vec.iter().tuple_windows().enumerate() {
                let common_prefix_len = p1
                    .bytes()
                    .zip(p2.bytes())
                    .take_while(|(c1, c2)| c1 == c2)
                    .count();

                let p1_prefix_ratio = common_prefix_len as f64 / p1.len() as f64;
                let p2_prefix_ratio = common_prefix_len as f64 / p2.len() as f64;

                if p1_prefix_ratio < prefix_match_threshold
                    && p2_prefix_ratio < prefix_match_threshold
                {
                    let next_split_idx = i + 1;
                    let split_len = next_split_idx - prev_split_idx;
                    splits.push((split_len, prev_split_idx, next_split_idx));
                    prev_split_idx = next_split_idx;
                }
            }

            let end_idx = prompts_vec.len();
            let split_len = end_idx - prev_split_idx;
            splits.push((split_len, prev_split_idx, end_idx));

            let mut tasks = Vec::new();
            while state.buffer_size > max_buffer_size {
                let (len, start_idx, end_idx) = splits.pop().unwrap();
                let prompts = prompts_vec[start_idx..end_idx].to_vec();

                // find the longest common prefix in all prompts
                let mut prefix_len = 0;
                for i in 0..prompts[0].len() {
                    if prompts
                        .iter()
                        .all(|p| p.as_bytes().get(i) == prompts[0].as_bytes().get(i))
                    {
                        prefix_len = i + 1;
                    } else {
                        break;
                    }
                }

                let prefix = prompts[0][..prefix_len].to_string();

                let rb = sorted.slice(start_idx, end_idx)?;
                tasks.push((prefix, prompts, rb));
                state.buffer_size -= len;
            }

            let mut remaining = Vec::with_capacity(splits.len());
            for (_, start_idx, end_idx) in splits {
                remaining.push(sorted.slice(start_idx, end_idx)?);
            }

            let mp = MicroPartition::new_loaded(sorted.schema, Arc::new(remaining), None);
            state.buffer = vec![mp.into()];

            Ok(tasks)
        } else {
            Ok(vec![])
        }
    }

    fn get_prompts_for_batch(&self, batch: &RecordBatch) -> DaftResult<Vec<String>> {
        let expr = self.expr.inner();
        let expr_input = BoundExpr::new_unchecked(expr.input.clone());

        let prompts = batch.eval_expression(&expr_input)?;

        // TODO: handle nulls
        let prompts_vec = prompts
            .utf8()
            .map_err(|_| {
                DaftError::type_error(format!(
                    "Expected input to `prompt` to be string, got {}",
                    prompts.data_type()
                ))
            })?
            .as_arrow()
            .values_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        Ok(prompts_vec)
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
        let this = self.clone();

        spawner
            .spawn(
                async move {
                    let tasks = if this.expr.inner().do_prefix_routing {
                        state.buffer_size += input.len();
                        state.buffer.push(input);
                        this.pop_tasks(&mut state, this.expr.inner().max_buffer_size)?
                    } else if input.is_empty() {
                        vec![]
                    } else {
                        let batch = input
                            .concat_or_get_update(IOStatsContext::new("VLLMSink::execute"))?
                            .unwrap();
                        let prompts = this.get_prompts_for_batch(&batch)?;

                        vec![(String::new(), prompts, batch)]
                    };

                    for (prefix, prompts, rows) in tasks {
                        state.executor.submit(prefix, prompts, rows)?;
                    }

                    let output = this.poll_tasks(&state)?;
                    Ok((state, StreamingSinkOutput::NeedMoreInput(output)))
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
        let this = self.clone();

        spawner
            .spawn(
                async move {
                    let [state] = states.as_mut_slice() else {
                        unreachable!("VLLMSink should have exactly one state");
                    };

                    if !state.finished_submitting {
                        let tasks = this.pop_tasks(state, 0)?;
                        for (prefix, prompts, rows) in tasks {
                            state.executor.submit(prefix, prompts, rows)?;
                        }
                        state.finished_submitting = true;
                        state.executor.finished_submitting()?;
                    }

                    let all_tasks_finished = state.executor.all_tasks_finished()?;

                    let output = this.poll_tasks(state)?;

                    if all_tasks_finished {
                        state.executor.teardown()?;
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
        let executor = if let Some(llm_actors) = &self.llm_actors {
            VLLMExecutor::new_distributed(llm_actors)?
        } else {
            VLLMExecutor::new_local(self.expr.inner())?
        };

        Ok(VLLMState {
            executor,
            buffer: Vec::new(),
            buffer_size: 0,
            finished_submitting: false,
        })
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        self.expr
            .inner()
            .batch_size
            .map(MorselSizeRequirement::Strict)
    }
}
