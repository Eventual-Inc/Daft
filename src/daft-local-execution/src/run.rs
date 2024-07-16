use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    translate, Concat, Filter, InMemoryScan, Limit, LocalPhysicalPlan, PhysicalScan, Project,
    UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;

#[cfg(feature = "python")]
use {
    daft_micropartition::python::PyMicroPartition,
    daft_plan::PyLogicalPlanBuilder,
    pyo3::{pyclass, pymethods, IntoPy, PyObject, PyRef, PyRefMut, PyResult, Python},
};

use crate::{
    create_channel,
    intermediate_ops::{
        aggregate::AggregateOperator, filter::FilterOperator, intermediate_op::run_intermediate_op,
        project::ProjectOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        concat::ConcatSink,
        limit::LimitSink,
        sink::{run_double_input_sink, run_single_input_sink},
    },
    sources::{in_memory::InMemorySource, scan_task::ScanTaskSource, source::run_source},
    MultiSender,
};

#[cfg(feature = "python")]
#[pyclass]
struct LocalPartitionIterator {
    iter: Box<dyn Iterator<Item = DaftResult<PyObject>> + Send>,
}

#[cfg(feature = "python")]
#[pymethods]
impl LocalPartitionIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let iter = &mut slf.iter;
        Ok(py.allow_threads(|| iter.next().transpose())?)
    }
}

#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct NativeExecutor {
    local_physical_plan: Arc<LocalPhysicalPlan>,
}

#[cfg(feature = "python")]
#[pymethods]
impl NativeExecutor {
    #[staticmethod]
    pub fn from_logical_plan_builder(
        logical_plan_builder: &PyLogicalPlanBuilder,
        py: Python<'_>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let logical_plan = logical_plan_builder.builder.build();
            let local_physical_plan = translate(&logical_plan)?;
            Ok(Self {
                local_physical_plan,
            })
        })
    }

    pub fn run(
        &self,
        py: Python,
        psets: HashMap<String, Vec<PyMicroPartition>>,
    ) -> PyResult<PyObject> {
        let native_psets: HashMap<String, Vec<Arc<MicroPartition>>> = psets
            .into_iter()
            .map(|(part_id, parts)| {
                (
                    part_id,
                    parts
                        .into_iter()
                        .map(|part| part.into())
                        .collect::<Vec<Arc<MicroPartition>>>(),
                )
            })
            .collect();
        let out = py.allow_threads(|| run_local(&self.local_physical_plan, native_psets))?;
        let iter = Box::new(out.map(|part| {
            part.map(|p| pyo3::Python::with_gil(|py| PyMicroPartition::from(p).into_py(py)))
        }));
        let part_iter = LocalPartitionIterator { iter };
        Ok(part_iter.into_py(py))
    }
}

pub fn run_local(
    physical_plan: &LocalPhysicalPlan,
    psets: HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let res = runtime.block_on(async {
        let (sender, mut receiver) = create_channel(1, true);
        run_physical_plan(physical_plan, &psets, sender);
        let mut result = vec![];
        while let Some(val) = receiver.recv().await {
            result.push(val);
        }
        result.into_iter()
    });
    Ok(Box::new(res))
}

pub fn run_physical_plan(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
    sender: MultiSender,
) {
    match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            run_source(Arc::new(ScanTaskSource::new(scan_tasks.clone())), sender);
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            run_source(Arc::new(InMemorySource::new(partitions.clone())), sender);
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let next_sender = run_intermediate_op(Box::new(proj_op), sender);
            run_physical_plan(input, psets, next_sender);
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let next_sender = run_intermediate_op(Box::new(filter_op), sender);
            run_physical_plan(input, psets, next_sender);
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let sink_sender = run_single_input_sink(Box::new(sink), sender);
            run_physical_plan(input, psets, sink_sender);
        }
        LocalPhysicalPlan::Concat(Concat { input, other, .. }) => {
            let sink = ConcatSink::new();
            let (left_sender, right_sender) = run_double_input_sink(Box::new(sink), sender);

            run_physical_plan(input, psets, left_sender);
            run_physical_plan(other, psets, right_sender);
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, &[]);

            let final_stage_project = ProjectOperator::new(final_exprs);
            let next_sender = run_intermediate_op(Box::new(final_stage_project), sender);

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let next_sender = run_single_input_sink(Box::new(second_stage_agg_sink), next_sender);

            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let next_sender = run_intermediate_op(Box::new(first_stage_agg_op), next_sender);
            run_physical_plan(input, psets, next_sender);
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    }
}
