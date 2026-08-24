use super::SQLModule;
use crate::{
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleMap;

impl SQLModule for SQLModuleMap {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("map", MapConstructor);
        parent.add_fn("map_get", MapGet);
        parent.add_fn("map_extract", MapGet);
        parent.add_fn("map_keys", MapKeys);
    }
}

pub struct MapConstructor;

impl SQLFunction for MapConstructor {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        if inputs.is_empty() {
            invalid_operation_err!("map requires at least one key/value pair");
        }
        if inputs.len() % 2 != 0 {
            invalid_operation_err!("map requires alternating key/value pairs");
        }

        let args = inputs
            .iter()
            .map(|arg| planner.plan_function_arg(arg).map(|arg| arg.into_inner()))
            .collect::<crate::error::SQLPlannerResult<Vec<_>>>()?;

        Ok(daft_functions::map::map(args))
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Builds a map from alternating key and value expressions.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["key", "value"]
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
                let input = planner.plan_function_arg(input)?.into_inner();
                let key = planner.plan_function_arg(key)?.into_inner();
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

mod static_docs {
    pub(crate) const MAP_GET_DOCSTRING: &str =
        "Retrieves the value associated with a given key from a map.

.. seealso::

    * :func:`~daft.sql._sql_funcs.map_get`
    * :func:`~daft.sql._sql_funcs.map_extract`
";
}

pub struct MapKeys;

impl SQLFunction for MapKeys {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                Ok(daft_dsl::functions::map::map_keys(input))
            }
            _ => invalid_operation_err!("Expected 1 input arg"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns a list of all keys in the map.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input"]
    }
}
