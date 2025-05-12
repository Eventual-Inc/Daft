use crate::functions::SQLFunction;

pub struct SQLCoalesce {}

impl SQLFunction for SQLCoalesce {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        let args = inputs
            .iter()
            .map(|arg| {
                let arg = planner.plan_function_arg(arg)?;
                Ok(arg.into_inner())
            })
            .collect::<crate::error::SQLPlannerResult<Vec<_>>>()?;

        Ok(daft_functions::coalesce::coalesce(args))
    }

    fn docstrings(&self, _alias: &str) -> String {
        static_docs::DOCSTRING.to_string()
    }
}

mod static_docs {
    pub(super) const DOCSTRING: &str = "Coalesce the first non-null value from a list of inputs.";
}
