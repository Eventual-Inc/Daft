use daft_core::prelude::CountMode;
use daft_dsl::{lit, Expr, LiteralValue};

use super::SQLModule;
use crate::{
    error::PlannerError,
    functions::{SQLFunction, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleList;

impl SQLModule for SQLModuleList {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("list_chunk", SQLListChunk);
        parent.add_fn("list_count", SQLListCount);
        parent.add_fn("explode", SQLExplode);
        parent.add_fn("unnest", SQLExplode);
        // this is commonly called `array_to_string` in other SQL dialects
        parent.add_fn("array_to_string", SQLListJoin);
        // but we also want to support our `list_join` alias as well
        parent.add_fn("list_join", SQLListJoin);
        parent.add_fn("list_max", SQLListMax);
        parent.add_fn("list_min", SQLListMin);
        parent.add_fn("list_sum", SQLListSum);
        parent.add_fn("list_mean", SQLListMean);
        parent.add_fn("list_slice", SQLListSlice);
        parent.add_fn("list_sort", SQLListSort);

        // TODO
    }
}

pub struct SQLListChunk;

impl SQLFunction for SQLListChunk {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input, chunk_size] => {
                let input = planner.plan_function_arg(input)?;
                let chunk_size = planner
                    .plan_function_arg(chunk_size)
                    .and_then(|arg| match arg.as_ref() {
                        Expr::Literal(LiteralValue::Int64(n)) => Ok(*n as usize),
                        _ => unsupported_sql_err!("Expected chunk size to be a number"),
                    })?;
                Ok(daft_functions::list::chunk(input, chunk_size))
            }
            _ => unsupported_sql_err!(
                "invalid arguments for list_chunk. Expected list_chunk(expr, chunk_size)"
            ),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_CHUNK_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "chunk_size"]
    }
}

pub struct SQLListCount;

impl SQLFunction for SQLListCount {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::count(input, CountMode::Valid))
            }
            [input, count_mode] => {
                let input = planner.plan_function_arg(input)?;
                let mode =
                    planner
                        .plan_function_arg(count_mode)
                        .and_then(|arg| match arg.as_ref() {
                            Expr::Literal(LiteralValue::Utf8(s)) => {
                                s.parse().map_err(PlannerError::from)
                            }
                            _ => unsupported_sql_err!("Expected mode to be a string"),
                        })?;
                Ok(daft_functions::list::count(input, mode))
            }
            _ => unsupported_sql_err!("invalid arguments for list_count. Expected either list_count(expr) or list_count(expr, mode)"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_COUNT_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "mode"]
    }
}

pub struct SQLExplode;

impl SQLFunction for SQLExplode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::explode(input))
            }
            _ => unsupported_sql_err!("Expected 1 argument"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::EXPLODE_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLListJoin;

impl SQLFunction for SQLListJoin {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input, separator] => {
                let input = planner.plan_function_arg(input)?;
                let separator = planner.plan_function_arg(separator)?;
                Ok(daft_functions::list::join(input, separator))
            }
            _ => unsupported_sql_err!(
                "invalid arguments for list_join. Expected list_join(expr, separator)"
            ),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_JOIN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "separator"]
    }
}

pub struct SQLListMax;

impl SQLFunction for SQLListMax {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::max(input))
            }
            _ => unsupported_sql_err!("invalid arguments for list_max. Expected list_max(expr)"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_MAX_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLListMean;

impl SQLFunction for SQLListMean {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::mean(input))
            }
            _ => unsupported_sql_err!("invalid arguments for list_mean. Expected list_mean(expr)"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_MEAN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLListMin;

impl SQLFunction for SQLListMin {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::min(input))
            }
            _ => unsupported_sql_err!("invalid arguments for list_min. Expected list_min(expr)"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_MIN_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLListSum;

impl SQLFunction for SQLListSum {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::sum(input))
            }
            _ => unsupported_sql_err!("invalid arguments for list_sum. Expected list_sum(expr)"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_SUM_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}

pub struct SQLListSlice;

impl SQLFunction for SQLListSlice {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input, start, end] => {
                let input = planner.plan_function_arg(input)?;
                let start = planner.plan_function_arg(start)?;
                let end = planner.plan_function_arg(end)?;
                Ok(daft_functions::list::slice(input, start, end))
            }
            _ => unsupported_sql_err!(
                "invalid arguments for list_slice. Expected list_slice(expr, start, end)"
            ),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_SLICE_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "start", "end"]
    }
}

pub struct SQLListSort;

impl SQLFunction for SQLListSort {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::list::sort(input, None))
            }
            [input, order] => {
                let input = planner.plan_function_arg(input)?;
                use sqlparser::ast::{
                    Expr::Identifier as SQLIdent, FunctionArg::Unnamed,
                    FunctionArgExpr::Expr as SQLExpr,
                };

                let order = match order {
                    Unnamed(SQLExpr(SQLIdent(ident))) => {
                        match ident.value.to_lowercase().as_str() {
                            "asc" => lit(false),
                            "desc" => lit(true),
                            _ => unsupported_sql_err!("invalid order for list_sort"),
                        }
                    }
                    _ => unsupported_sql_err!("invalid order for list_sort"),
                };
                Ok(daft_functions::list::sort(input, Some(order)))
            }
            _ => unsupported_sql_err!(
                "invalid arguments for list_sort. Expected list_sort(expr, ASC|DESC)"
            ),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::LIST_SORT_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "order"]
    }
}

mod static_docs {
    pub(crate) const LIST_CHUNK_DOCSTRING: &str = "Splits a list into chunks of a specified size.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_chunk(numbers, 2) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 2, 3, 4, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 7, 8, 9, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────────────────────╮
    │ list_chunk                │
    │ ----------                │
    │ List[List[Int64]]         │
    ╞═══════════════════════════╡
    │ [[1, 2], [3, 4], [5]]     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [[6, 7], [8, 9], [10]]    │
    ╰───────────────────────────╯";

    pub(crate) const LIST_COUNT_DOCSTRING: &str = "Counts the number of elements in a list.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_count(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 2, 3, 4, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 7, 8, 9, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ list_count│
    │ ----------│
    │ Int64     │
    ╞═══════════╡
    │ 5         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 5         │
    ╰───────────╯";

    pub(crate) const EXPLODE_DOCSTRING: &str = "Expands a list column into multiple rows.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT explode(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 2, 3]         │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [4, 5]            │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ explode   │
    │ -------   │
    │ Int64     │
    ╞═══════════╡
    │ 1         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 3         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 4         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 5         │
    ╰───────────╯";

    pub(crate) const LIST_JOIN_DOCSTRING: &str =
        "Joins elements of a list into a single string using a specified separator.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_join(words, ', ') FROM word_table

.. code-block:: text
    :caption: Input

    ╭───────────────────────╮
    │ words                 │
    │ -----                 │
    │ List[String]          │
    ╞═══════════════════════╡
    │ ['apple', 'banana']   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ ['cherry', 'date']    │
    ╰───────────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────────────╮
    │ list_join         │
    │ ---------         │
    │ String            │
    ╞═══════════════════╡
    │ apple, banana     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ cherry, date      │
    ╰───────────────────╯";

    pub(crate) const LIST_MAX_DOCSTRING: &str = "Returns the maximum value in a list.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_max(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 3, 2, 5, 4]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 8, 7, 10, 9]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ list_max  │
    │ --------  │
    │ Int64     │
    ╞═══════════╡
    │ 5         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 10        │
    ╰───────────╯";

    pub(crate) const LIST_MEAN_DOCSTRING: &str =
        "Calculates the mean (average) of values in a list.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_mean(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 2, 3, 4, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 7, 8, 9, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ list_mean │
    │ --------- │
    │ Float64   │
    ╞═══════════╡
    │ 3.0       │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 8.0       │
    ╰───────────╯";

    pub(crate) const LIST_MIN_DOCSTRING: &str = "Returns the minimum value in a list.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_min(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [3, 1, 4, 2, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [8, 6, 9, 7, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ list_min  │
    │ --------  │
    │ Int64     │
    ╞═══════════╡
    │ 1         │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 6         │
    ╰───────────╯";

    pub(crate) const LIST_SUM_DOCSTRING: &str = "Calculates the sum of values in a list.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_sum(numbers) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [1, 2, 3, 4, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 7, 8, 9, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────╮
    │ list_sum  │
    │ --------  │
    │ Int64     │
    ╞═══════════╡
    │ 15        │
    ├╌╌╌╌╌╌╌╌╌╌╌┤
    │ 40        │
    ╰───────────╯";

    pub(crate) const LIST_SLICE_DOCSTRING: &str =
        "Extracts a portion of a list from a start index to an end index.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_slice(numbers, 1, 4) FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────────╮
    │ numbers               │
    │ -------               │
    │ List[Int64]           │
    ╞═══════════════════════╡
    │ [1, 2, 3, 4, 5]       │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [6, 7, 8, 9, 10]      │
    ╰───────────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────────╮
    │ list_slice    │
    │ ----------    │
    │ List[Int64]   │
    ╞═══════════════╡
    │ [2, 3, 4]     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [7, 8, 9]     │
    ╰───────────────╯";

    pub(crate) const LIST_SORT_DOCSTRING: &str =
        "Sorts the elements of a list in ascending or descending order.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT list_sort(numbers, 'DESC') FROM number_table

.. code-block:: text
    :caption: Input

    ╭───────────────────╮
    │ numbers           │
    │ -------           │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [3, 1, 4, 2, 5]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [8, 6, 9, 7, 10]  │
    ╰───────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────────────────╮
    │ list_sort         │
    │ ---------         │
    │ List[Int64]       │
    ╞═══════════════════╡
    │ [5, 4, 3, 2, 1]   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ [10, 9, 8, 7, 6]  │
    ╰───────────────────╯";
}
