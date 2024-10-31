use std::sync::Arc;

use daft_dsl::{col, AggExpr, Expr, ExprRef, LiteralValue};
use sqlparser::ast::{FunctionArg, FunctionArgExpr};

use super::SQLModule;
use crate::{
    ensure,
    error::SQLPlannerResult,
    functions::{SQLFunction, SQLFunctions},
    planner::SQLPlanner,
    unsupported_sql_err,
};

pub struct SQLModuleAggs;

impl SQLModule for SQLModuleAggs {
    fn register(parent: &mut SQLFunctions) {
        use AggExpr::{Count, Max, Mean, Min, Stddev, Sum};
        // HACK TO USE AggExpr as an enum rather than a
        let nil = Arc::new(Expr::Literal(LiteralValue::Null));
        parent.add_fn(
            "count",
            Count(nil.clone(), daft_core::count_mode::CountMode::Valid),
        );
        parent.add_fn("sum", Sum(nil.clone()));
        parent.add_fn("avg", Mean(nil.clone()));
        parent.add_fn("mean", Mean(nil.clone()));
        parent.add_fn("min", Min(nil.clone()));
        parent.add_fn("max", Max(nil.clone()));
        parent.add_fn("stddev", Stddev(nil.clone()));
        parent.add_fn("stddev_samp", Stddev(nil));
    }
}

impl SQLFunction for AggExpr {
    fn to_expr(&self, inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
        // COUNT(*) needs a bit of extra handling, so we process that outside of `to_expr`
        if let Self::Count(_, _) = self {
            handle_count(inputs, planner)
        } else {
            let inputs = self.args_to_expr_unnamed(inputs, planner)?;
            to_expr(self, inputs.as_slice())
        }
    }

    fn docstrings(&self, alias: &str) -> String {
        match self {
            Self::Count(_, _) => static_docs::COUNT_DOCSTRING.to_string(),
            Self::Sum(_) => static_docs::SUM_DOCSTRING.to_string(),
            Self::Mean(_) => static_docs::AVG_DOCSTRING.replace("{}", alias),
            Self::Min(_) => static_docs::MIN_DOCSTRING.to_string(),
            Self::Max(_) => static_docs::MAX_DOCSTRING.to_string(),
            Self::Stddev(_) => static_docs::STDDEV_DOCSTRING.to_string(),
            e => unimplemented!("Need to implement docstrings for {e}"),
        }
    }

    fn arg_names(&self) -> &'static [&'static str] {
        match self {
            Self::Count(_, _)
            | Self::Sum(_)
            | Self::Mean(_)
            | Self::Min(_)
            | Self::Max(_)
            | Self::Stddev(_) => &["input"],
            e => unimplemented!("Need to implement arg names for {e}"),
        }
    }
}

fn handle_count(inputs: &[FunctionArg], planner: &SQLPlanner) -> SQLPlannerResult<ExprRef> {
    Ok(match inputs {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => match planner.relation_opt() {
            Some(rel) => {
                let schema = rel.schema();
                col(schema.fields[0].name.clone())
                    .count(daft_core::count_mode::CountMode::All)
                    .alias("count")
            }
            None => unsupported_sql_err!("Wildcard is not supported in this context"),
        },
        [FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name))] => {
            match planner.relation_opt() {
                Some(rel) if name.to_string() == rel.get_name() => {
                    let schema = rel.schema();
                    col(schema.fields[0].name.clone())
                        .count(daft_core::count_mode::CountMode::All)
                        .alias("count")
                }
                _ => unsupported_sql_err!("Wildcard is not supported in this context"),
            }
        }
        [expr] => {
            // SQL default COUNT ignores nulls
            let input = planner.plan_function_arg(expr)?;
            input.count(daft_core::count_mode::CountMode::Valid)
        }
        _ => unsupported_sql_err!("COUNT takes exactly one argument"),
    })
}

pub fn to_expr(expr: &AggExpr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    match expr {
        AggExpr::Count(_, _) => unreachable!("count should be handled by by this point"),
        AggExpr::Sum(_) => {
            ensure!(args.len() == 1, "sum takes exactly one argument");
            Ok(args[0].clone().sum())
        }
        AggExpr::ApproxCountDistinct(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxPercentile(_) => unsupported_sql_err!("approx_percentile"),
        AggExpr::ApproxSketch(_, _) => unsupported_sql_err!("approx_sketch"),
        AggExpr::MergeSketch(_, _) => unsupported_sql_err!("merge_sketch"),
        AggExpr::Mean(_) => {
            ensure!(args.len() == 1, "mean takes exactly one argument");
            Ok(args[0].clone().mean())
        }
        AggExpr::Stddev(_) => {
            ensure!(args.len() == 1, "stddev takes exactly one argument");
            Ok(args[0].clone().stddev())
        }
        AggExpr::Min(_) => {
            ensure!(args.len() == 1, "min takes exactly one argument");
            Ok(args[0].clone().min())
        }
        AggExpr::Max(_) => {
            ensure!(args.len() == 1, "max takes exactly one argument");
            Ok(args[0].clone().max())
        }
        AggExpr::AnyValue(_, _) => unsupported_sql_err!("any_value"),
        AggExpr::List(_) => unsupported_sql_err!("list"),
        AggExpr::Concat(_) => unsupported_sql_err!("concat"),
        AggExpr::MapGroups { .. } => unsupported_sql_err!("map_groups"),
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
    │ 1     │
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
}
