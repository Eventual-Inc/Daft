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
}
