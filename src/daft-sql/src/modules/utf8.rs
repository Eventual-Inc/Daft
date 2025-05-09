use daft_dsl::{binary_op, ExprRef, Operator};
use daft_functions::tokenize::{
    tokenize_decode, tokenize_encode, TokenizeDecodeFunction, TokenizeEncodeFunction,
};

use super::SQLModule;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    invalid_operation_err,
};

pub struct SQLModuleUtf8;

impl SQLModule for SQLModuleUtf8 {
    fn register(parent: &mut crate::functions::SQLFunctions) {
        parent.add_fn("tokenize_encode", SQLTokenizeEncode);
        parent.add_fn("tokenize_decode", SQLTokenizeDecode);
        parent.add_fn("concat", SQLConcat);
    }
}

pub struct SQLTokenizeEncode;
impl TryFrom<SQLFunctionArguments> for TokenizeEncodeFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        if args.get_named("io_config").is_some() {
            return Err(PlannerError::invalid_operation(
                "io_config argument is not yet supported for tokenize_encode",
            ));
        }

        let tokens_path = args.try_get_named("tokens_path")?.ok_or_else(|| {
            PlannerError::invalid_operation("tokens_path argument is required for tokenize_encode")
        })?;

        let pattern = args.try_get_named("pattern")?;
        let special_tokens = args.try_get_named("special_tokens")?;
        let use_special_tokens = args.try_get_named("use_special_tokens")?.unwrap_or(false);

        Ok(Self {
            tokens_path,
            pattern,
            special_tokens,
            use_special_tokens,
            io_config: None,
        })
    }
}

impl SQLFunction for SQLTokenizeEncode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, tokens_path] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let tokens_path = planner.plan_function_arg(tokens_path)?.into_inner();
                let tokens_path = tokens_path
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("tokens_path argument must be a string")
                    })?;
                Ok(tokenize_encode(input, tokens_path, None, None, None, false))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let args: TokenizeEncodeFunction = planner.plan_function_args(
                    args,
                    &[
                        "tokens_path",
                        "pattern",
                        "special_tokens",
                        "use_special_tokens",
                    ],
                    1, // tokens_path can be named or positional
                )?;
                Ok(tokenize_encode(
                    input,
                    &args.tokens_path,
                    None,
                    args.pattern.as_deref(),
                    args.special_tokens.as_deref(),
                    args.use_special_tokens,
                ))
            }
            _ => invalid_operation_err!("Invalid arguments for tokenize_encode"),
        }
    }

    fn docstrings(&self, _: &str) -> String {
        "Decodes each list of integer tokens into a string using a tokenizer.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "token_path",
            "io_config",
            "pattern",
            "special_tokens",
            "use_special_tokens",
        ]
    }
}

pub struct SQLTokenizeDecode;
impl TryFrom<SQLFunctionArguments> for TokenizeDecodeFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        if args.get_named("io_config").is_some() {
            return Err(PlannerError::invalid_operation(
                "io_config argument is not yet supported for tokenize_decode",
            ));
        }

        let tokens_path = args.try_get_named("tokens_path")?.ok_or_else(|| {
            PlannerError::invalid_operation("tokens_path argument is required for tokenize_encode")
        })?;

        let pattern = args.try_get_named("pattern")?;
        let special_tokens = args.try_get_named("special_tokens")?;

        Ok(Self {
            tokens_path,
            pattern,
            special_tokens,
            io_config: None,
        })
    }
}
impl SQLFunction for SQLTokenizeDecode {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, tokens_path] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let tokens_path = planner.plan_function_arg(tokens_path)?.into_inner();
                let tokens_path = tokens_path
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("tokens_path argument must be a string")
                    })?;
                Ok(tokenize_decode(input, tokens_path, None, None, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?.into_inner();
                let args: TokenizeDecodeFunction = planner.plan_function_args(
                    args,
                    &["tokens_path", "pattern", "special_tokens"],
                    1, // tokens_path can be named or positional
                )?;
                Ok(tokenize_decode(
                    input,
                    &args.tokens_path,
                    None,
                    args.pattern.as_deref(),
                    args.special_tokens.as_deref(),
                ))
            }
            _ => invalid_operation_err!("Invalid arguments for tokenize_decode"),
        }
    }

    fn docstrings(&self, _: &str) -> String {
        "Encodes each string as a list of integer tokens using a tokenizer.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "token_path",
            "io_config",
            "pattern",
            "special_tokens",
            "use_special_tokens",
        ]
    }
}

pub struct SQLConcat;

impl SQLFunction for SQLConcat {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let inputs = inputs
            .iter()
            .map(|input| planner.plan_function_arg(input).map(|arg| arg.into_inner()))
            .collect::<SQLPlannerResult<Vec<_>>>()?;
        let mut inputs = inputs.into_iter();

        let Some(mut first) = inputs.next() else {
            invalid_operation_err!("concat requires at least one argument")
        };
        for input in inputs {
            first = binary_op(Operator::Plus, first, input);
        }

        Ok(first)
    }

    fn docstrings(&self, _: &str) -> String {
        "Concatenate the inputs into a single string".to_string()
    }
}
