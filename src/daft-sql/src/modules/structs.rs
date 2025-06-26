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
                let input = planner.plan_function_arg(input)?.into_inner();
                let key = planner.plan_function_arg(key).map(|arg| arg.into_inner())?;
                if let Some(lit) = key.as_literal().and_then(|lit| lit.as_str()) {
                    Ok(daft_dsl::functions::struct_::get(input, lit))
                } else {
                    invalid_operation_err!("Expected key to be a string literal")
                }
            }
            _ => invalid_operation_err!("Expected 2 input args"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Extracts a field from a struct expression by name.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "field"]
    }
}
