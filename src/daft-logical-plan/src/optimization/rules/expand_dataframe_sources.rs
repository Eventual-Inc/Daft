use std::sync::Arc;
#[cfg(feature = "python")]
use std::{cell::RefCell, collections::HashSet};

#[cfg(feature = "python")]
use common_error::DaftError;
use common_error::DaftResult;
use common_treenode::Transformed;
#[cfg(feature = "python")]
use common_treenode::{TreeNode, TreeNodeRecursion};
#[cfg(feature = "python")]
use daft_scan::ScanState;

use super::OptimizerRule;
use crate::LogicalPlan;
#[cfg(feature = "python")]
use crate::{SourceInfo, ops::Filter, ops::Project};

#[derive(Default, Debug)]
pub struct ExpandDataFrameSources {
    #[cfg(feature = "python")]
    expanding: RefCell<HashSet<usize>>,
}

impl ExpandDataFrameSources {
    pub fn new() -> Self {
        Self::default()
    }
}

// Replace a DataFrameSource leaf with the plan from get_dataframe.
impl OptimizerRule for ExpandDataFrameSources {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        #[cfg(feature = "python")]
        {
            self.expanding.borrow_mut().clear();
            plan.transform_down(|node| self.try_optimize_node(node))
        }
        #[cfg(not(feature = "python"))]
        {
            Ok(Transformed::no(plan))
        }
    }
}

#[cfg(feature = "python")]
impl ExpandDataFrameSources {
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let LogicalPlan::Source(source) = plan.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let SourceInfo::Physical(physical) = source.source_info.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let ScanState::Operator(scan_op) = &physical.scan_state else {
            return Ok(Transformed::no(plan));
        };
        let Some(expander) = scan_op.0.as_dataframe_expander() else {
            return Ok(Transformed::no(plan));
        };

        let id = expander.python_object_id();
        if !self.expanding.borrow_mut().insert(id) {
            return Ok(Transformed::no(plan));
        }

        // Keep `id` in the set while we walk the spliced plan so `self.read()` is
        // still a cycle. Remove it before returning so a sibling `.read()` on the
        // same object can expand. Walk here (then Jump) so the id is still present
        // for children; Transformed::yes would pop too early.
        let source_name = scan_op.0.name().to_string();
        let output_schema = source.output_schema.clone();
        let pushdowns = physical.pushdowns.clone();
        let result = self.expand_and_walk(plan.clone(), output_schema, pushdowns, expander, &source_name);
        self.expanding.borrow_mut().remove(&id);
        result
    }

    fn expand_and_walk(
        &self,
        plan: Arc<LogicalPlan>,
        output_schema: daft_schema::schema::SchemaRef,
        pushdowns: daft_scan::Pushdowns,
        expander: &dyn daft_scan::ExpandsToDataFrame,
        source_name: &str,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let Some(py_df) = expander.expand_dataframe(&pushdowns)? else {
            return Ok(Transformed::no(plan));
        };

        let mut inner = logical_plan_from_py_dataframe(&py_df, source_name)?;

        let predicate = match (pushdowns.filters, pushdowns.partition_filters) {
            (Some(f), Some(p)) => Some(f.and(p)),
            (Some(f), None) => Some(f),
            (None, Some(p)) => Some(p),
            (None, None) => None,
        };
        if let Some(predicate) = predicate {
            inner = Filter::try_new(inner, predicate)
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "Failed to apply filter to DataFrameSource '{source_name}': {e}"
                    ))
                })?
                .into();
        }

        if inner.schema() != output_schema {
            let inner_schema = inner.schema();
            for name in output_schema.names() {
                let expected = output_schema.get_field(&name)?;
                match inner_schema.get_field(&name) {
                    Ok(got) if got.dtype == expected.dtype => {}
                    Ok(got) => {
                        return Err(DaftError::ValueError(format!(
                            "DataFrameSource '{source_name}' column '{name}' has type {}, expected {}",
                            got.dtype, expected.dtype
                        )));
                    }
                    Err(_) => {
                        return Err(DaftError::ValueError(format!(
                            "DataFrameSource '{source_name}' is missing column '{name}' (got: {})",
                            inner_schema.names().join(", ")
                        )));
                    }
                }
            }
            inner = Project::new_from_schema(inner, output_schema)
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "Failed to project DataFrameSource '{source_name}' to source schema: {e}"
                    ))
                })?
                .into();
        }

        let walked = inner.transform_down(|node| self.try_optimize_node(node))?;
        Ok(Transformed::new(walked.data, true, TreeNodeRecursion::Jump))
    }
}

#[cfg(feature = "python")]
fn logical_plan_from_py_dataframe(
    df: &pyo3::Py<pyo3::PyAny>,
    source_name: &str,
) -> DaftResult<Arc<LogicalPlan>> {
    use pyo3::{Python, intern, types::PyAnyMethods};

    use crate::PyLogicalPlanBuilder;

    Python::attach(|py| {
        let bound = df.bind(py);
        let current = bound
            .call_method0(intern!(py, "_get_current_builder"))
            .map_err(|e| {
                DaftError::ValueError(format!(
                    "DataFrameSource '{source_name}' get_dataframe must return a DataFrame: {e}"
                ))
            })?;
        let inner = current.getattr(intern!(py, "_builder")).map_err(|e| {
            DaftError::ValueError(format!(
                "DataFrameSource '{source_name}' get_dataframe returned a DataFrame without a logical plan: {e}"
            ))
        })?;
        let py_lpb = inner.extract::<PyLogicalPlanBuilder>().map_err(|e| {
            DaftError::ValueError(format!(
                "DataFrameSource '{source_name}' get_dataframe returned an unexpected builder: {e}"
            ))
        })?;
        Ok(py_lpb.builder.plan)
    })
}
