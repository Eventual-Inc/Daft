use super::SQLModule;
use crate::{
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleMap;

impl SQLModule for SQLModuleMap {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("map_get", MapGet);
        parent.add_fn("map_extract", MapGet);
    }
}

pub struct MapGet;

impl SQLFunction for MapGet {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input, key] => {
                let input = planner.plan_function_arg(input)?;
                let key = planner.plan_function_arg(key)?;
                Ok(daft_dsl::functions::map::get(input, key))
            }
            _ => invalid_operation_err!("Expected 2 input args"),
        }
    }

    fn docstrings(&self, alias: &str) -> String {
        static_docs::MAP_GET_DOCSTRING.replace("{}", alias)
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "key"]
    }
}

// ... existing code ...

mod static_docs {
    pub(crate) const MAP_GET_DOCSTRING: &str =
        "Retrieves the value associated with a given key from a map.

.. seealso::

    * :func:`~daft.sql._sql_funcs.map_get`
    * :func:`~daft.sql._sql_funcs.map_extract`

Example:

.. code-block:: sql
    :caption: SQL

    SELECT {}(user_data, 'age') FROM users_table

.. code-block:: text
    :caption: Input

    ╭───────────────────────────────╮
    │ user_data                     │
    │ ---------                     │
    │ Map[Utf8, Int64]              │
    ╞═══════════════════════════════╡
    │ {'name': 'Alice', 'age': 30}  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ {'name': 'Bob', 'age': 25}    │
    ╰───────────────────────────────╯

.. code-block:: text
    :caption: Output

    ╭───────╮
    │ age   │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 30    │
    ├╌╌╌╌╌╌╌┤
    │ 25    │
    ╰───────╯";
}
