use super::SQLModule;
use crate::{
    functions::{SQLFunction, SQLFunctions},
    invalid_operation_err,
};

pub struct SQLModuleStructs;

impl SQLModule for SQLModuleStructs {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("struct_get", StructGet);
        parent.add_fn("struct_extract", StructGet);
    }
}

pub struct StructGet;

impl SQLFunction for StructGet {
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
