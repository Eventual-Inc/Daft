use daft_dsl::{binary_op, ExprRef, Operator};
use daft_functions::{
    count_matches::{utf8_count_matches, CountMatchesFunction},
    tokenize::{tokenize_decode, tokenize_encode, TokenizeDecodeFunction, TokenizeEncodeFunction},
};

use super::SQLModule;
use crate::{
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    invalid_operation_err,
};

fn utf8_unary(
    func: impl Fn(ExprRef) -> ExprRef,
    sql_name: &str,
    arg_name: &str,
    inputs: &[sqlparser::ast::FunctionArg],
    planner: &crate::planner::SQLPlanner,
) -> SQLPlannerResult<ExprRef> {
    match inputs {
        [input] => {
            let input = planner.plan_function_arg(input)?;
            Ok(func(input))
        }
        _ => invalid_operation_err!(
            "invalid arguments for {sql_name}. Expected {sql_name}({arg_name})",
        ),
    }
}

macro_rules! utf8_function {
    ($name:ident, $sql_name:expr, $func:expr, $doc:expr, $arg_name:expr) => {
        pub struct $name;
        impl SQLFunction for $name {
            fn to_expr(
                &self,
                inputs: &[sqlparser::ast::FunctionArg],
                planner: &crate::planner::SQLPlanner,
            ) -> SQLPlannerResult<ExprRef> {
                utf8_unary($func, $sql_name, $arg_name, inputs, planner)
            }

            fn docstrings(&self, _alias: &str) -> String {
                $doc.to_string()
            }

            fn arg_names(&self) -> &'static [&'static str] {
                &[$arg_name]
            }
        }
    };
    ($name:ident, $sql_name:expr, $func:expr, $doc:expr, $arg_name_1:expr, $arg_name_2:expr) => {
        pub struct $name;
        impl SQLFunction for $name {
            fn to_expr(
                &self,
                inputs: &[sqlparser::ast::FunctionArg],
                planner: &crate::planner::SQLPlanner,
            ) -> SQLPlannerResult<ExprRef> {
                utf8_binary($func, $sql_name, $arg_name_1, $arg_name_2, inputs, planner)
            }

            fn docstrings(&self, _alias: &str) -> String {
                $doc.to_string()
            }

            fn arg_names(&self) -> &'static [&'static str] {
                &[$arg_name_1, $arg_name_2]
            }
        }
    };
    ($name:ident, $sql_name:expr, $func:expr, $doc:expr, $arg_name_1:expr, $arg_name_2:expr, $arg_name_3:expr) => {
        pub struct $name;
        impl SQLFunction for $name {
            fn to_expr(
                &self,
                inputs: &[sqlparser::ast::FunctionArg],
                planner: &crate::planner::SQLPlanner,
            ) -> SQLPlannerResult<ExprRef> {
                utf8_ternary(
                    $func,
                    $sql_name,
                    $arg_name_1,
                    $arg_name_2,
                    $arg_name_3,
                    inputs,
                    planner,
                )
            }

            fn docstrings(&self, _alias: &str) -> String {
                $doc.to_string()
            }

            fn arg_names(&self) -> &'static [&'static str] {
                &[$arg_name_1, $arg_name_2, $arg_name_3]
            }
        }
    };
}

pub struct SQLModuleUtf8;

impl SQLModule for SQLModuleUtf8 {
    fn register(parent: &mut crate::functions::SQLFunctions) {
        // TODO add split variants
        // parent.add("split", f(Split(false)));
        // TODO add replace variants
        // parent.add("replace", f(Replace(false)));
        parent.add_fn("upper", SQLUtf8Upper);
        parent.add_fn("to_datetime", SQLUtf8ToDatetime);
        parent.add_fn("count_matches", SQLCountMatches);
        parent.add_fn("tokenize_encode", SQLTokenizeEncode);
        parent.add_fn("tokenize_decode", SQLTokenizeDecode);
        parent.add_fn("concat", SQLConcat);
    }
}

utf8_function!(
    SQLUtf8Upper,
    "upper",
    daft_functions::utf8::upper,
    "Converts the string to uppercase",
    "string_input"
);

pub struct SQLUtf8ToDatetime;

impl SQLFunction for SQLUtf8ToDatetime {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, fmt] => {
                let input = planner.plan_function_arg(input)?;
                let fmt = planner.plan_function_arg(fmt)?;
                let fmt = fmt
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("to_datetime format must be a string")
                    })?;
                Ok(daft_functions::utf8::to_datetime(input, fmt, None))
            }
            [input, fmt, tz] => {
                let input = planner.plan_function_arg(input)?;
                let fmt = planner.plan_function_arg(fmt)?;
                let fmt = fmt
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("to_datetime format must be a string")
                    })?;
                let tz = planner.plan_function_arg(tz)?;
                let tz = tz.as_literal().and_then(|lit| lit.as_str());
                Ok(daft_functions::utf8::to_datetime(input, fmt, tz))
            }
            _ => invalid_operation_err!("to_datetime takes either two or three arguments"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Parses the string as a datetime using the specified format.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["string_input", "format"]
    }
}

pub struct SQLCountMatches;

impl TryFrom<SQLFunctionArguments> for CountMatchesFunction {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let whole_words = args.try_get_named("whole_words")?.unwrap_or(false);
        let case_sensitive = args.try_get_named("case_sensitive")?.unwrap_or(true);

        Ok(Self {
            whole_words,
            case_sensitive,
        })
    }
}

impl SQLFunction for SQLCountMatches {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, pattern] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                Ok(utf8_count_matches(input, pattern, false, true))
            }
            [input, pattern, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                let args: CountMatchesFunction =
                    planner.plan_function_args(args, &["whole_words", "case_sensitive"], 0)?;

                Ok(utf8_count_matches(
                    input,
                    pattern,
                    args.whole_words,
                    args.case_sensitive,
                ))
            }
            _ => Err(PlannerError::invalid_operation(format!(
                "Invalid arguments for count_matches: '{inputs:?}'"
            ))),
        }
    }

    fn docstrings(&self, _: &str) -> String {
        "Counts the number of times a pattern, or multiple patterns, appears in the input."
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["input", "pattern", "whole_words", "case_sensitive"]
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
                let input = planner.plan_function_arg(input)?;
                let tokens_path = planner.plan_function_arg(tokens_path)?;
                let tokens_path = tokens_path
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("tokens_path argument must be a string")
                    })?;
                Ok(tokenize_encode(input, tokens_path, None, None, None, false))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
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
                let input = planner.plan_function_arg(input)?;
                let tokens_path = planner.plan_function_arg(tokens_path)?;
                let tokens_path = tokens_path
                    .as_literal()
                    .and_then(|lit| lit.as_str())
                    .ok_or_else(|| {
                        PlannerError::invalid_operation("tokens_path argument must be a string")
                    })?;
                Ok(tokenize_decode(input, tokens_path, None, None, None))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
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
            .map(|input| planner.plan_function_arg(input))
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
