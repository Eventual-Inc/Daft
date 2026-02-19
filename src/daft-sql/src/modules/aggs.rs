use std::sync::Arc;

use common_hashable_float_wrapper::FloatWrapper;
use daft_core::{prelude::*, utils::stats};
use daft_dsl::{AggExpr, Expr, ExprRef, lit, unresolved_col};
use sqlparser::ast::{FunctionArg, FunctionArgExpr};

use super::SQLModule;
use crate::{
    ensure,
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions, SQLLiteral},
    object_name_to_identifier,
    planner::SQLPlanner,
    table_not_found_err, unsupported_sql_err,
};

pub struct SQLModuleAggs;

impl SQLModule for SQLModuleAggs {
    fn register(parent: &mut SQLFunctions) {
        // HACK TO USE AggExpr as an enum rather than a
        let nil = Arc::new(Expr::Literal(Literal::Null));
        let float_nil = FloatWrapper(0.0);
        parent.add_fn("count", AggExpr::Count(nil.clone(), CountMode::Valid));
        parent.add_fn("count_distinct", AggExpr::CountDistinct(nil.clone()));
        parent.add_fn("sum", AggExpr::Sum(nil.clone()));
        parent.add_fn("product", AggExpr::Product(nil.clone()));
        parent.add_fn("avg", AggExpr::Mean(nil.clone()));
        parent.add_fn("mean", AggExpr::Mean(nil.clone()));
        parent.add_fn("percentile", AggExpr::Percentile(nil.clone(), float_nil));
        parent.add_fn("median", AggExpr::Median(nil.clone()));
        parent.add_fn("min", AggExpr::Min(nil.clone()));
        parent.add_fn("max", AggExpr::Max(nil.clone()));
        parent.add_fn("bool_and", AggExpr::BoolAnd(nil.clone()));
        parent.add_fn("bool_or", AggExpr::BoolOr(nil.clone()));
        parent.add_fn("stddev", AggExpr::Stddev(nil.clone()));
        parent.add_fn("stddev_samp", AggExpr::Stddev(nil.clone()));
        parent.add_fn("var", AggExpr::Var(nil.clone(), 1));
        parent.add_fn("var_samp", AggExpr::Var(nil.clone(), 1));
        parent.add_fn("var_pop", AggExpr::Var(nil.clone(), 0));
        parent.add_fn("variance", AggExpr::Var(nil, 1));
    }
}

impl SQLFunction for AggExpr {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        if let Self::Count(_, _) = self {
            handle_count(inputs, planner)
        } else if let Self::Percentile(_, _) = self {
            handle_percentile(inputs, planner)
        } else {
            let inputs = self.args_to_expr_unnamed(inputs, planner)?;
            to_expr(self, inputs.into_inner().as_slice())
        }
    }

    fn docstrings(&self, alias: &str) -> String {
        match self {
            Self::Count(_, _) => static_docs::COUNT_DOCSTRING.to_string(),
            Self::CountDistinct(_) => static_docs::COUNT_DISTINCT_DOCSTRING.to_string(),
            Self::Sum(_) => static_docs::SUM_DOCSTRING.to_string(),
            Self::Product(_) => static_docs::PRODUCT_DOCSTRING.to_string(),
            Self::Mean(_) => static_docs::AVG_DOCSTRING.replace("{}", alias),
            Self::Percentile(_, _) => static_docs::PERCENTILE_DOCSTRING.to_string(),
            Self::Median(_) => static_docs::MEDIAN_DOCSTRING.to_string(),
            Self::Min(_) => static_docs::MIN_DOCSTRING.to_string(),
            Self::Max(_) => static_docs::MAX_DOCSTRING.to_string(),
            Self::Stddev(_) => static_docs::STDDEV_DOCSTRING.to_string(),
            Self::Var(_, _) => static_docs::VAR_DOCSTRING.to_string(),
            Self::BoolAnd(_) => static_docs::BOOL_AND_DOCSTRING.to_string(),
            Self::BoolOr(_) => static_docs::BOOL_OR_DOCSTRING.to_string(),
            e => unimplemented!("Need to implement docstrings for {e}"),
        }
    }

    fn arg_names(&self) -> &'static [&'static str] {
        match self {
            Self::Count(_, _)
            | Self::CountDistinct(_)
            | Self::Sum(_)
            | Self::Product(_)
            | Self::Mean(_)
            | Self::Median(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::Stddev(_)
            | Self::Var(_, _)
            | Self::BoolAnd(_)
            | Self::BoolOr(_) => &["input"],
            Self::Percentile(_, _) => &["input", "percentage"],
            e => unimplemented!("Need to implement arg names for {e}"),
        }
    }
}

fn handle_count(inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
    Ok(match inputs {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => match &planner.current_plan {
            Some(plan) => {
                let schema = plan.schema();
                let pushdown_col = schema
                    .min_estimated_size_column()
                    .map(|name| name.to_string())
                    .unwrap_or_else(|| schema[0].name.clone());
                unresolved_col(pushdown_col)
                    .count(daft_core::count_mode::CountMode::All)
                    .alias("count")
            }
            None => unsupported_sql_err!("Wildcard is not supported in this context"),
        },
        [FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name))] => {
            let ident = object_name_to_identifier(name);

            match &planner.current_plan {
                Some(plan) => {
                    if let Some(schema) =
                        plan.plan.clone().get_schema_for_alias(&ident.to_string())?
                    {
                        let pushdown_col = schema
                            .min_estimated_size_column()
                            .map(|name| name.to_string())
                            .unwrap_or_else(|| schema[0].name.clone());
                        unresolved_col(pushdown_col)
                            .count(daft_core::count_mode::CountMode::All)
                            .alias("count")
                    } else {
                        table_not_found_err!(ident.to_string())
                    }
                }
                _ => unsupported_sql_err!("Wildcard is not supported in this context"),
            }
        }
        [expr] => {
            let input = planner.plan_function_arg(expr)?.into_inner();

            match input.as_literal() {
                // count(null) always returns 0
                Some(Literal::Null) => lit(0).alias("count"),
                // in SQL, any count(<non null literal>) is functionally the same as count(*)
                Some(_) => match &planner.current_plan {
                    Some(plan) => {
                        let schema = plan.schema();
                        let pushdown_col = schema
                            .min_estimated_size_column()
                            .map(|name| name.to_string())
                            .unwrap_or_else(|| schema[0].name.clone());
                        unresolved_col(pushdown_col)
                            .count(daft_core::count_mode::CountMode::All)
                            .alias("count")
                    }
                    None => {
                        unsupported_sql_err!("count(<literal>) is not supported in this context")
                    }
                },
                // SQL default COUNT ignores nulls
                None => input.count(daft_core::count_mode::CountMode::Valid),
            }
        }
        _ => unsupported_sql_err!("COUNT takes exactly one argument"),
    })
}

fn handle_percentile(inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
    ensure!(
        inputs.len() == 2,
        "percentile takes exactly two arguments: percentile(input, percentage)"
    );

    let input = planner.plan_function_arg(&inputs[0])?.into_inner();
    let percentage_expr = planner.plan_function_arg(&inputs[1])?.into_inner();
    let percentage = f64::from_expr(&percentage_expr)?;
    ensure!(
        stats::is_valid_percentile_percentage(percentage),
        "percentile percentage must be between 0.0 and 1.0 inclusive"
    );
    Ok(input.percentile(percentage))
}

fn to_expr(expr: &AggExpr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    match expr {
        AggExpr::Count(_, _) => unreachable!("count should be handled by by this point"),
        AggExpr::CountDistinct(_) => {
            ensure!(args.len() == 1, "count_distinct takes exactly one argument");
            Ok(args[0].clone().count_distinct())
        }
        AggExpr::Sum(_) => {
            ensure!(args.len() == 1, "sum takes exactly one argument");
            Ok(args[0].clone().sum())
        }
        AggExpr::Product(_) => {
            ensure!(args.len() == 1, "product takes exactly one argument");
            Ok(args[0].clone().product())
        }
        AggExpr::ApproxCountDistinct(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxPercentile(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxSketch(_, _) => unsupported_sql_err!("approx_sketch"),
        AggExpr::MergeSketch(_, _) => unsupported_sql_err!("merge_sketch"),
        AggExpr::Mean(_) => {
            ensure!(args.len() == 1, "mean takes exactly one argument");
            Ok(args[0].clone().mean())
        }
        AggExpr::Percentile(_, _) => unreachable!("percentile should be handled by this point"),
        AggExpr::Median(_) => {
            ensure!(args.len() == 1, "median takes exactly one argument");
            Ok(args[0].clone().median())
        }
        AggExpr::Stddev(_) => {
            ensure!(args.len() == 1, "stddev takes exactly one argument");
            Ok(args[0].clone().stddev())
        }
        AggExpr::Var(_, ddof) => {
            ensure!(args.len() == 1, "var takes exactly one argument");
            Ok(args[0].clone().var(*ddof))
        }
        AggExpr::Min(_) => {
            ensure!(args.len() == 1, "min takes exactly one argument");
            Ok(args[0].clone().min())
        }
        AggExpr::Max(_) => {
            ensure!(args.len() == 1, "max takes exactly one argument");
            Ok(args[0].clone().max())
        }
        AggExpr::BoolAnd(_) => {
            ensure!(args.len() == 1, "bool_and takes exactly one argument");
            Ok(args[0].clone().bool_and())
        }
        AggExpr::BoolOr(_) => {
            ensure!(args.len() == 1, "bool_or takes exactly one argument");
            Ok(args[0].clone().bool_or())
        }
        AggExpr::AnyValue(_, _) => unsupported_sql_err!("any_value"),
        AggExpr::List(_) => unsupported_sql_err!("list"),
        AggExpr::Concat(_, _) => unsupported_sql_err!("concat"),
        AggExpr::MapGroups { .. } => unsupported_sql_err!("map_groups"),
        AggExpr::Set(_) => unsupported_sql_err!("set"),
        AggExpr::Skew(_) => unsupported_sql_err!("skew"),
    }
}

mod static_docs {
    pub(crate) const COUNT_DOCSTRING: &str =
        "Counts the number of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT count(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 2     │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const COUNT_DISTINCT_DOCSTRING: &str =
        "Counts the number of distinct, non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT count(distinct x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 2     │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const SUM_DOCSTRING: &str =
        "Calculates the sum of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT sum(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 300   │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const PRODUCT_DOCSTRING: &str =
        "Calculates the product of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT product(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 2     │
    ├╌╌╌╌╌╌╌┤
    │ 3     │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 6     │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const AVG_DOCSTRING: &str =
        "Calculates the average (mean) of non-null elements in the input expression.

.. seealso::
    This SQL Function has aliases.

    * :func:`~daft.sql._sql_funcs.mean`
    * :func:`~daft.sql._sql_funcs.avg`

Example:

.. code-block:: sql
    :caption: SQL

    SELECT {}(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ x         │
    │ ---       │
    │ Float64   │
    ╞═══════════╡
    │ 150.0     │
    ╰───────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const PERCENTILE_DOCSTRING: &str =
        "Calculates the exact percentile of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT percentile(x, 0.5) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Float64 │
    ╞═════════╡
    │ 150.0   │
    ╰─────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const MEDIAN_DOCSTRING: &str =
        "Calculates the median (50th percentile) of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT median(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Float64 │
    ╞═════════╡
    │ 150.0   │
    ╰─────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const MIN_DOCSTRING: &str =
        "Finds the minimum value among non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT min(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const MAX_DOCSTRING: &str =
        "Finds the maximum value among non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT max(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 200   │
    ╰───────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const STDDEV_DOCSTRING: &str =
        "Calculates the standard deviation of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT stddev(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭──────────────╮
    │ x            │
    │ ---          │
    │ Float64      │
    ╞══════════════╡
    │ 70.710678118 │
    ╰──────────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const VAR_DOCSTRING: &str =
        "Calculates the variance of non-null elements in the input expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT var(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 100   │
    ├╌╌╌╌╌╌╌┤
    │ 200   │
    ├╌╌╌╌╌╌╌┤
    │ null  │
    ╰───────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭────────╮
    │ x      │
    │ ---    │
    │ Float64│
    ╞════════╡
    │ 5000.0 │
    ╰────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const BOOL_AND_DOCSTRING: &str =
        "Returns true if all non-null elements in the input expression are true, false if any are false, and null if all elements are null.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT bool_and(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Boolean │
    ╞═════════╡
    │ true    │
    ├╌╌╌╌╌╌╌╌╌┤
    │ true    │
    ├╌╌╌╌╌╌╌╌╌┤
    │ null    │
    ╰─────────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Boolean │
    ╞═════════╡
    │ true    │
    ╰─────────╯
    (Showing first 1 of 1 rows)";

    pub(crate) const BOOL_OR_DOCSTRING: &str =
        "Returns true if any non-null elements in the input expression are true, false if all are false, and null if all elements are null.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT bool_or(x) FROM tbl

.. code-block:: text
    :caption: Input

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Boolean │
    ╞═════════╡
    │ false   │
    ├╌╌╌╌╌╌╌╌╌┤
    │ true    │
    ├╌╌╌╌╌╌╌╌╌┤
    │ null    │
    ╰─────────╯
    (Showing first 3 of 3 rows)

.. code-block:: text
    :caption: Output

    ╭─────────╮
    │ x       │
    │ ---     │
    │ Boolean │
    ╞═════════╡
    │ true    │
    ╰─────────╯
    (Showing first 1 of 1 rows)";
}
