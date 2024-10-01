use super::SQLModule;
use crate::{
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleJson;

impl SQLModule for SQLModuleJson {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("json_query", JsonQuery);
    }
}

struct JsonQuery;

impl SQLFunction for JsonQuery {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input, query] => {
                let input = planner.plan_function_arg(input)?;
                let query = planner.plan_function_arg(query)?;
                if let Some(q) = query.as_literal().and_then(|l| l.as_str()) {
                    Ok(daft_functions_json::json_query(input, q))
                } else {
                    invalid_operation_err!("Expected a string literal for the query argument")
                }
            }
            _ => invalid_operation_err!(
                "invalid arguments for json_query. expected json_query(input, query)"
            ),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::JSON_QUERY_DOCSTRING.to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "query"]
    }
}

mod static_docs {
    pub(crate) const JSON_QUERY_DOCSTRING: &str =
        "Extracts a JSON object from a JSON string using a JSONPath expression.

Example:

.. code-block:: sql
    :caption: SQL

    SELECT json_query(data, '$.store.book[0].title') FROM json_table

.. code-block:: text
    :caption: Input

    ╭────────────────────────────────────────────────────────────────────╮
    │ data                                                               │
    │ ----                                                               │
    │ String                                                             │
    ╞════════════════════════════════════════════════════════════════════╡
    │ {\"store\": {\"book\": [{\"title\": \"Sayings of the Century\"}]}} │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ {\"store\": {\"book\": [{\"title\": \"Sword of Honour\"}]}}        │
    ╰────────────────────────────────────────────────────────────────────╯
    (Showing first 2 of 2 rows)

.. code-block:: text
    :caption: Output

    ╭────────────────────────────╮
    │ data                       │
    │ -------------------------- │
    │ String                     │
    ╞════════════════════════════╡
    │ Sayings of the Century     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌--╌╌┤
    │ Sword of Honour            │
    ╰────────────────────────────╯
    (Showing first 2 of 2 rows)";
}
