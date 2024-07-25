use std::str::FromStr;

use daft_dsl::{Expr, ExprRef, LiteralValue};
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use crate::{
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err,
    planner::{Relation, SQLPlanner},
    unsupported_sql_err,
};

// TODO: expand this to support more functions
pub enum SQLFunctions {
    /// SQL `abs()` function
    Abs,
    /// SQL `sign()` function
    Sign,
    /// SQL `round(<n>)` function
    Round,
    /// SQL `max` function
    Max,
}

impl FromStr for SQLFunctions {
    type Err = PlannerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "abs" => Ok(SQLFunctions::Abs),
            "sign" => Ok(SQLFunctions::Sign),
            "round" => Ok(SQLFunctions::Round),
            "max" => Ok(SQLFunctions::Max),
            _ => unsupported_sql_err!("unknown function: '{value}'"),
        }
    }
}

impl SQLPlanner {
    pub(crate) fn plan_function(
        &self,
        func: &Function,
        current_rel: &Relation,
    ) -> SQLPlannerResult<ExprRef> {
        if func.null_treatment.is_some() {
            unsupported_sql_err!("null treatment");
        }
        if func.filter.is_some() {
            unsupported_sql_err!("filter");
        }
        if func.over.is_some() {
            unsupported_sql_err!("over");
        }
        if !func.within_group.is_empty() {
            unsupported_sql_err!("within group");
        }
        let args = match &func.args {
            sqlparser::ast::FunctionArguments::None => vec![],
            sqlparser::ast::FunctionArguments::Subquery(_) => {
                unsupported_sql_err!("subquery")
            }
            sqlparser::ast::FunctionArguments::List(arg_list) => {
                if arg_list.duplicate_treatment.is_some() {
                    unsupported_sql_err!("duplicate treatment");
                }
                if !arg_list.clauses.is_empty() {
                    unsupported_sql_err!("clauses in function arguments");
                }
                arg_list
                    .args
                    .iter()
                    .map(|arg| self.plan_function_arg(arg, current_rel))
                    .collect::<SQLPlannerResult<Vec<_>>>()?
            }
        };
        let func_name = func.name.to_string();
        let func = func_name.parse::<SQLFunctions>()?;

        match func {
            SQLFunctions::Abs => {
                if args.len() != 1 {
                    invalid_operation_err!("abs takes exactly one argument");
                }
                Ok(daft_dsl::functions::numeric::abs(args[0].clone()))
            }
            SQLFunctions::Sign => {
                if args.len() != 1 {
                    invalid_operation_err!("sign takes exactly one argument");
                }
                Ok(daft_dsl::functions::numeric::sign(args[0].clone()))
            }
            SQLFunctions::Round => {
                if args.len() != 2 {
                    invalid_operation_err!("round takes exactly one argument");
                }

                let precision = match args[1].as_ref() {
                    Expr::Literal(LiteralValue::Int32(i)) => *i,
                    Expr::Literal(LiteralValue::UInt32(u)) => *u as i32,
                    Expr::Literal(LiteralValue::Int64(i)) => *i as i32,
                    _ => invalid_operation_err!("round precision must be an integer"),
                };

                Ok(daft_dsl::functions::numeric::round(
                    args[0].clone(),
                    precision,
                ))
            }
            SQLFunctions::Max => {
                if args.len() != 1 {
                    invalid_operation_err!("max takes exactly one argument");
                }
                Ok(args[0].clone().max())
            }
        }
    }

    fn plan_function_arg(
        &self,
        function_arg: &FunctionArg,
        current_rel: &Relation,
    ) -> SQLPlannerResult<ExprRef> {
        match function_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => self.plan_expr(expr, current_rel),
            _ => unsupported_sql_err!("named function args not yet supported"),
        }
    }
}
