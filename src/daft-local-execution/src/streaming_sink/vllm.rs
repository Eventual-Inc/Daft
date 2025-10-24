use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::{DataType, SchemaRef};
#[cfg(feature = "python")]
use daft_core::python::PySeries;
use daft_dsl::expr::bound_expr::{BoundExpr, BoundVLLMExpr};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
#[cfg(feature = "python")]
use pyo3::{intern, prelude::*, types::PyList};
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    pipeline::{MorselSizeRequirement, NodeName},
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    },
};

pub(crate) struct VLLMSink {
    expr: BoundVLLMExpr,
    schema: SchemaRef,
}

pub(crate) struct VLLMState {
    #[cfg(feature = "python")]
    executor: Py<PyAny>,
}

impl VLLMSink {
    pub(crate) fn new(expr: BoundVLLMExpr, schema: SchemaRef) -> Self {
        Self { expr, schema }
    }
}

impl StreamingSink for VLLMSink {
    type State = VLLMState;

    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        #[cfg(feature = "python")]
        {
            let schema = self.schema.clone();
            let expr = self.expr.inner();
            let expr_input = BoundExpr::new_unchecked(expr.input.clone());

            spawner
                .spawn(
                    async move {
                        let io_stats = IOStatsContext::new("VLLMSink::execute");
                        let Some(input) = input.concat_or_get_update(io_stats)? else {
                            let empty = Arc::new(MicroPartition::empty(Some(schema)));
                            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty))))
                                .into();
                        };

                        let series_input = input.eval_expression(&expr_input)?;

                        let series_output = Python::attach(|py| {
                            let list_input = PySeries {
                                series: series_input,
                            }
                            .to_pylist(py)?;

                            let output = state.executor.bind(py).call1((list_input,))?;
                            let list_output = output.downcast::<PyList>()?;

                            let series_output = PySeries::from_pylist(
                                list_output,
                                Some("daft_vllm_output"),
                                Some(DataType::Utf8.into()),
                            )?
                            .series;

                            Ok::<_, PyErr>(series_output)
                        })?;

                        let output_columns =
                            [input.columns().to_vec(), vec![series_output]].concat();
                        let output_recordbatch =
                            RecordBatch::new_unchecked(schema.clone(), output_columns, input.len());
                        let output = Arc::new(MicroPartition::new_loaded(
                            schema,
                            Arc::new(vec![output_recordbatch]),
                            None,
                        ));
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output))))
                    },
                    Span::current(),
                )
                .into()
        }
        #[cfg(not(feature = "python"))]
        {
            unimplemented!("VLLMProject is not supported without the Python feature.")
        }
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
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
        #[cfg(feature = "python")]
        {
            let expr = self.expr.inner();
            let executor = Python::attach(|py| {
                Ok::<_, PyErr>(
                    py.import(intern!(py, "daft.execution.vllm"))?
                        .getattr(intern!(py, "VLLMExecutor"))?
                        .call1((
                            &expr.model,
                            expr.engine_args.as_ref(),
                            expr.generate_args.as_ref(),
                        ))?
                        .unbind(),
                )
            })?;
            Ok(VLLMState { executor })
        }
        #[cfg(not(feature = "python"))]
        {
            Ok(VLLMState {})
        }
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
