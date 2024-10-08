use daft_dsl::ExprRef;
use daft_functions::{
    hash::hash,
    minhash::{minhash, MinHashFunction},
};
use sqlparser::ast::FunctionArg;

use super::SQLModule;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments, SQLFunctions},
    unsupported_sql_err,
};

pub struct SQLModuleHashing;

impl SQLModule for SQLModuleHashing {
    fn register(parent: &mut SQLFunctions) {
        parent.add_fn("hash", SQLHash);
        parent.add_fn("minhash", SQLMinhash);
    }
}

pub struct SQLHash;

impl SQLFunction for SQLHash {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(hash(input, None))
            }
            [input, seed] => {
                let input = planner.plan_function_arg(input)?;
                match seed {
                    FunctionArg::Named { name, arg, .. } if name.value == "seed" => {
                        let seed = planner.try_unwrap_function_arg_expr(arg)?;
                        Ok(hash(input, Some(seed)))
                    }
                    arg @ FunctionArg::Unnamed(_) => {
                        let seed = planner.plan_function_arg(arg)?;
                        Ok(hash(input, Some(seed)))
                    }
                    _ => unsupported_sql_err!("Invalid arguments for hash: '{inputs:?}'"),
                }
            }
            _ => unsupported_sql_err!("Invalid arguments for hash: '{inputs:?}'"),
        }
    }
}

pub struct SQLMinhash;

impl TryFrom<SQLFunctionArguments> for MinHashFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let num_hashes = args
            .get_named("num_hashes")
            .ok_or_else(|| PlannerError::invalid_operation("num_hashes is required"))?
            .as_literal()
            .and_then(daft_dsl::LiteralValue::as_i64)
            .ok_or_else(|| PlannerError::invalid_operation("num_hashes must be an integer"))?
            as usize;

        let ngram_size = args
            .get_named("ngram_size")
            .ok_or_else(|| PlannerError::invalid_operation("ngram_size is required"))?
            .as_literal()
            .and_then(daft_dsl::LiteralValue::as_i64)
            .ok_or_else(|| PlannerError::invalid_operation("ngram_size must be an integer"))?
            as usize;
        let seed = args
            .get_named("seed")
            .map(|arg| {
                arg.as_literal()
                    .and_then(daft_dsl::LiteralValue::as_i64)
                    .ok_or_else(|| PlannerError::invalid_operation("num_hashes must be an integer"))
            })
            .transpose()?
            .unwrap_or(1) as u32;
        Ok(Self {
            num_hashes,
            ngram_size,
            seed,
        })
    }
}

impl SQLFunction for SQLMinhash {
    fn to_expr(
        &self,
        inputs: &[FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args: MinHashFunction =
                    planner.plan_function_args(args, &["num_hashes", "ngram_size", "seed"], 0)?;

                Ok(minhash(input, args.num_hashes, args.ngram_size, args.seed))
            }
            _ => unsupported_sql_err!("Invalid arguments for minhash: '{inputs:?}'"),
        }
    }
}
