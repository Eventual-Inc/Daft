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
        parent.add_fn("named_struct", NamedStruct);
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

pub struct NamedStruct;

impl SQLFunction for NamedStruct {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        if inputs.is_empty() || inputs.len() % 2 != 0 {
            invalid_operation_err!("named_struct requires name/value pairs")
        }

        let mut struct_values = Vec::with_capacity(inputs.len() / 2);
        for pair in inputs.chunks(2) {
            let name_expr = planner.plan_function_arg(&pair[0])?.into_inner();
            let name = name_expr
                .as_literal()
                .and_then(|lit| lit.as_str())
                .ok_or_else(|| {
                    crate::error::PlannerError::invalid_operation(
                        "named_struct field names must be string literals",
                    )
                })?;

            let value = planner.plan_function_arg(&pair[1])?.into_inner();
            struct_values.push(value.alias(name));
        }

        Ok(daft_functions::to_struct::to_struct(struct_values))
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Builds a struct from alternating field names and values.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["field_name", "value"]
    }
}
