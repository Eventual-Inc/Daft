pub(crate) mod proto;

// ---------------------------------------
//   DAFT IR ORGANIZATION VIA RE-EXPORTS
// ---------------------------------------

// todo(conner): consider scan, partition, pushdown mod
pub use common_scan_info::{
    PartitionField, PartitionTransform, Pushdowns, ScanState, ScanTaskLike, ScanTaskLikeRef,
};
// todo(conner): why here?
pub use daft_core::count_mode::CountMode;

pub use crate::{
    rel::{LogicalPlan, LogicalPlanRef},
    rex::{Expr, ExprRef},
    schema::{DataType, Schema, SchemaRef},
};

#[rustfmt::skip]
pub mod rex {
    use std::sync::Arc;

    use daft_dsl::functions::{python::PythonUDF, FunctionArgs, FunctionExpr, ScalarFunction, ScalarUDF};
    pub use daft_dsl::*;

    /// Creates an expression from a python-scalar function
    pub fn from_py_func<A, E>(func: PythonUDF, args: A) -> Expr
    where
        A: IntoIterator<Item = E>,
        E: Into<Arc<Expr>>,
    {
        let func: FunctionExpr = FunctionExpr::Python(func);
        let inputs: Vec<Arc<Expr>> = args.into_iter().map(|e| e.into()).collect();
        Expr::Function { func, inputs }
    }

    /// Creates an expression from a python-scalar function
    pub fn from_rs_func(func: Arc<dyn ScalarUDF>, args: FunctionArgs<ExprRef>) -> Expr {
        // don't use ::new
        let func = ScalarFunction { udf: func, inputs: args };
        Expr::ScalarFunction(func)
    }
}

#[rustfmt::skip]
pub mod functions {
    use std::sync::Arc;

    pub use daft_dsl::functions::*;

    // Link the function (infallibly) .. todo(conner): add error handling later.
    pub fn get_function(name: &str) -> Arc<dyn ScalarFunctionFactory + 'static> {
        FUNCTION_REGISTRY
        .read()
        .expect("Failed to get FUNCTION_REGISTRY read lock")
        .get(name)
        .expect("Missing function implementation, should have been impossible.")
    }
}

#[rustfmt::skip]
pub mod rel {
    use std::sync::Arc;
    use crate::rex::Expr;

    use common_error::DaftError;
    use common_error::DaftResult;
    pub use daft_logical_plan::*;
    pub use daft_logical_plan::ops::*;
    use crate::schema::Schema;

    /// Consider updating other set operators to use the setq as it's better practice.
    pub use daft_logical_plan::ops::SetQuantifier;
    pub use daft_logical_plan::ops::UnionStrategy;

    /// Keep scan info together..
    pub use daft_logical_plan::source_info::*;
    pub use common_scan_info::PhysicalScanInfo;

    /// Creates a new source relational operator.
    pub fn new_source<S, I>(schema: S, info: I) -> DaftResult<Source>
    where
        S: Into<Arc<Schema>>,
        I: Into<Arc<SourceInfo>>,
     {
        Ok(Source::new(schema.into(), info.into()))
    }

    /// Creates a new projection relational operator.
    pub fn new_project<I, P, E>(input: I, projections: P) -> DaftResult<Project>
    where
        I: Into<Arc<LogicalPlan>>,
        P: IntoIterator<Item = E>,
        E: Into<Arc<Expr>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        let projections: Vec<Arc<Expr>> = projections.into_iter().map(|e| e.into()).collect();
        Project::new(input, projections)
    }

    /// Creates a new projection relational operator where at least one input is an actor pool udf.
    pub fn new_project_with_actor_pool<I, P, E>(input: I, projections: P) -> DaftResult<ActorPoolProject>
    where
        I: Into<Arc<LogicalPlan>>,
        P: IntoIterator<Item = E>,
        E: Into<Arc<Expr>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        let projections: Vec<Arc<Expr>> = projections.into_iter().map(|e| e.into()).collect();
        ActorPoolProject::new(input, projections)
    }

    /// Creates a new filter relational operator.
    pub fn new_filter<I, P>(input: I, predicate: P) -> DaftResult<Filter>
    where
        I: Into<Arc<LogicalPlan>>,
        P: Into<Arc<Expr>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        let predicate: Arc<Expr> = predicate.into();
        Ok(Filter { plan_id: None,
            node_id: None, input, predicate, stats_state: stats::StatsState::NotMaterialized })
    }

    /// Creates a new limit relational operator.
    pub fn new_limit<I>(input: I, limit: u64) -> DaftResult<Limit>
    where
        I: Into<Arc<LogicalPlan>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        Ok(Limit { plan_id: None,
            node_id: None, input, limit, eager: false, stats_state: stats::StatsState::NotMaterialized })
    }

    /// Creates a new distinct relational operator.
    pub fn new_distinct<I>(input: I) -> DaftResult<Distinct>
    where
        I: Into<Arc<LogicalPlan>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        Ok(Distinct { plan_id: None,
            node_id: None, input, stats_state: stats::StatsState::NotMaterialized, columns: None })
    }

    /// Creates a new concat relational operator.
    pub fn new_concat<R>(lhs: R, rhs: R) -> DaftResult<Concat>
    where
        R: Into<Arc<LogicalPlan>>,
    {
        let lhs = lhs.into();
        let rhs = rhs.into();
        Ok(Concat { plan_id: None,
            node_id: None, input: lhs, other: rhs, stats_state: stats::StatsState::NotMaterialized })
    }

    /// Creates a new intersect relational operator.
    pub fn new_intersect<R>(lhs: R, rhs: R, is_all: bool) -> DaftResult<Intersect>
    where
        R: Into<Arc<LogicalPlan>>,
    {
        let lhs = lhs.into();
        let rhs = rhs.into();
        Ok(Intersect { plan_id: None,
            node_id: None, lhs, rhs, is_all })
    }

    /// Creates a new union relational operator.
    pub fn new_union<R>(lhs: R, rhs: R, is_all: bool, is_by_name: bool) -> DaftResult<Union>
    where
        R: Into<Arc<LogicalPlan>>,
    {
        let lhs = lhs.into();
        let rhs = rhs.into();
        let quantifier = if is_all { SetQuantifier::All } else { SetQuantifier::Distinct};
        let strategy = if is_by_name { UnionStrategy::ByName } else { UnionStrategy::Positional };
        Ok(Union { plan_id: None,
            node_id: None, lhs, rhs, quantifier, strategy })
    }

    /// Creates a new except relational operator.
    pub fn new_except<R>(lhs: R, rhs: R, is_all: bool) -> DaftResult<Except>
    where
        R: Into<Arc<LogicalPlan>>,
    {
        let lhs = lhs.into();
        let rhs = rhs.into();
        Ok(Except { plan_id: None,
            node_id: None, lhs, rhs, is_all })
    }

    /// Creates a new aggregation relational operator.
    pub fn new_aggregate<I, A, G, E>(input: I, aggs: A, groups: G) -> DaftResult<Aggregate>
    where
        I: Into<Arc<LogicalPlan>>,
        A: IntoIterator<Item = E>,
        G: IntoIterator<Item = E>,
        E: Into<Arc<Expr>>,
     {
        // Going through the builder for this because there's logic in the constructor, which
        // is package private with a package private return type, neither of which I wish to change now.
        let input = input.into();
        let agg_exprs = aggs.into_iter().map(|e| e.into()).collect();
        let groupby_exprs = groups.into_iter().map(|e| e.into()).collect();
        let builder = LogicalPlanBuilder::new(input, None);
        let builder = builder.aggregate(agg_exprs, groupby_exprs)?;
        // We just created this, so we immediately take back ownership of it, the builder arc'd it.
        if let LogicalPlan::Aggregate(aggregate) = Arc::try_unwrap(builder.plan).unwrap_or_else(|_| panic!("Expected LogicalPlan::Aggregate!")) {
            Ok(aggregate)
        } else {
            Err(DaftError::InternalError("Expected LogicalPlan::Aggregate!".to_string()))
        }
    }
}

/// Flatten the daft_schema package, consider the prelude.
#[rustfmt::skip]
pub mod schema {
    pub use common_scan_info::PartitionField;
    pub use common_scan_info::PartitionTransform;
    pub use daft_schema::schema::*;
    pub use daft_schema::dtype::*;
    pub use daft_schema::field::*;
    pub use daft_schema::time_unit::TimeUnit;
    pub use daft_schema::image_format::ImageFormat;
    pub use daft_schema::image_mode::ImageMode;
}

/// Python exports for daft_ir.
#[cfg(feature = "python")]
pub mod python;

/// Export for daft.daft registration.
#[cfg(feature = "python")]
pub use python::register_modules;
