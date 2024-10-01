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
}
