use daft_core::array::ops::Utf8NormalizeOptions;
use daft_dsl::{binary_op, ExprRef, LiteralValue, Operator};
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

fn utf8_binary(
    func: impl Fn(ExprRef, ExprRef) -> ExprRef,
    sql_name: &str,
    arg_name_1: &str,
    arg_name_2: &str,
    inputs: &[sqlparser::ast::FunctionArg],
    planner: &crate::planner::SQLPlanner,
) -> SQLPlannerResult<ExprRef> {
    match inputs {
        [input1, input2] => {
            let input1 = planner.plan_function_arg(input1)?;
            let input2 = planner.plan_function_arg(input2)?;
            Ok(func(input1, input2))
        }
        _ => invalid_operation_err!(
            "invalid arguments for {sql_name}. Expected {sql_name}({arg_name_1}, {arg_name_2})",
        ),
    }
}

fn utf8_ternary(
    func: impl Fn(ExprRef, ExprRef, ExprRef) -> ExprRef,
    sql_name: &str,
    arg_name_1: &str,
    arg_name_2: &str,
    arg_name_3: &str,
    inputs: &[sqlparser::ast::FunctionArg],
    planner: &crate::planner::SQLPlanner,
) -> SQLPlannerResult<ExprRef> {
    match inputs {
        [input1, input2, input3] => {
            let input1 = planner.plan_function_arg(input1)?;
            let input2 = planner.plan_function_arg(input2)?;
            let input3 = planner.plan_function_arg(input3)?;
            Ok(func(input1, input2, input3))
        },
        _ => invalid_operation_err!(
            "invalid arguments for {sql_name}. Expected {sql_name}({arg_name_1}, {arg_name_2}, {arg_name_3})",
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
        parent.add_fn("ends_with", SQLUtf8EndsWith);
        parent.add_fn("starts_with", SQLUtf8StartsWith);
        parent.add_fn("contains", SQLUtf8Contains);
        parent.add_fn("split", SQLUtf8Split);
        // TODO add split variants
        // parent.add("split", f(Split(false)));
        parent.add_fn("regexp_match", SQLUtf8RegexpMatch);
        parent.add_fn("regexp_extract", SQLUtf8RegexpExtract);
        parent.add_fn("regexp_extract_all", SQLUtf8RegexpExtractAll);
        parent.add_fn("regexp_replace", SQLUtf8RegexpReplace);
        parent.add_fn("regexp_split", SQLUtf8RegexpSplit);
        // TODO add replace variants
        // parent.add("replace", f(Replace(false)));
        parent.add_fn("length", SQLUtf8Length);
        parent.add_fn("length_bytes", SQLUtf8LengthBytes);
        parent.add_fn("lower", SQLUtf8Lower);
        parent.add_fn("upper", SQLUtf8Upper);
        parent.add_fn("lstrip", SQLUtf8Lstrip);
        parent.add_fn("rstrip", SQLUtf8Rstrip);
        parent.add_fn("reverse", SQLUtf8Reverse);
        parent.add_fn("capitalize", SQLUtf8Capitalize);
        parent.add_fn("left", SQLUtf8Left);
        parent.add_fn("right", SQLUtf8Right);
        parent.add_fn("find", SQLUtf8Find);
        parent.add_fn("rpad", SQLUtf8Rpad);
        parent.add_fn("lpad", SQLUtf8Lpad);
        parent.add_fn("repeat", SQLUtf8Repeat);

        parent.add_fn("to_date", SQLUtf8ToDate);
        parent.add_fn("to_datetime", SQLUtf8ToDatetime);
        parent.add_fn("count_matches", SQLCountMatches);
        parent.add_fn("normalize", SQLNormalize);
        parent.add_fn("tokenize_encode", SQLTokenizeEncode);
        parent.add_fn("tokenize_decode", SQLTokenizeDecode);
        parent.add_fn("concat", SQLConcat);
    }
}

utf8_function!(
    SQLUtf8EndsWith,
    "ends_with",
    daft_functions::utf8::endswith,
    "Returns true if the string ends with the specified substring",
    "string_input",
    "substring"
);

utf8_function!(
    SQLUtf8StartsWith,
    "starts_with",
    daft_functions::utf8::startswith,
    "Returns true if the string starts with the specified substring",
    "string_input",
    "substring"
);

utf8_function!(
    SQLUtf8Contains,
    "contains",
    daft_functions::utf8::contains,
    "Returns true if the string contains the specified substring",
    "string_input",
    "substring"
);

utf8_function!(
    SQLUtf8Split,
    "split",
    |input, pattern| daft_functions::utf8::split(input, pattern, false),
    "Splits the string by the specified delimiter and returns an array of substrings",
    "string_input",
    "delimiter"
);

utf8_function!(
    SQLUtf8RegexpMatch,
    "regexp_match",
    daft_functions::utf8::match_,
    "Returns true if the string matches the specified regular expression pattern",
    "string_input",
    "pattern"
);

utf8_function!(
    SQLUtf8RegexpReplace,
    "regexp_replace",
    |input, pattern, replacement| daft_functions::utf8::replace(input, pattern, replacement, true),
    "Replaces all occurrences of a substring with a new string",
    "string_input",
    "pattern",
    "replacement"
);

utf8_function!(
    SQLUtf8RegexpSplit,
    "regexp_split",
    |input, pattern| daft_functions::utf8::split(input, pattern, true),
    "Splits the string by the specified delimiter and returns an array of substrings",
    "string_input",
    "delimiter"
);

utf8_function!(
    SQLUtf8Length,
    "length",
    daft_functions::utf8::length,
    "Returns the length of the string",
    "string_input"
);

utf8_function!(
    SQLUtf8LengthBytes,
    "length_bytes",
    daft_functions::utf8::length_bytes,
    "Returns the length of the string in bytes",
    "string_input"
);

utf8_function!(
    SQLUtf8Lower,
    "lower",
    daft_functions::utf8::lower,
    "Converts the string to lowercase",
    "string_input"
);

utf8_function!(
    SQLUtf8Upper,
    "upper",
    daft_functions::utf8::upper,
    "Converts the string to uppercase",
    "string_input"
);

utf8_function!(
    SQLUtf8Lstrip,
    "lstrip",
    daft_functions::utf8::lstrip,
    "Removes leading whitespace from the string",
    "string_input"
);

utf8_function!(
    SQLUtf8Rstrip,
    "rstrip",
    daft_functions::utf8::rstrip,
    "Removes trailing whitespace from the string",
    "string_input"
);

utf8_function!(
    SQLUtf8Reverse,
    "reverse",
    daft_functions::utf8::reverse,
    "Reverses the order of characters in the string",
    "string_input"
);

utf8_function!(
    SQLUtf8Capitalize,
    "capitalize",
    daft_functions::utf8::capitalize,
    "Capitalizes the first character of the string",
    "string_input"
);

utf8_function!(
    SQLUtf8Left,
    "left",
    daft_functions::utf8::left,
    "Returns the specified number of leftmost characters from the string",
    "string_input",
    "length"
);

utf8_function!(
    SQLUtf8Right,
    "right",
    daft_functions::utf8::right,
    "Returns the specified number of rightmost characters from the string",
    "string_input",
    "length"
);

utf8_function!(
    SQLUtf8Find,
    "find",
    daft_functions::utf8::find,
    "Returns the index of the first occurrence of a substring within the string",
    "string_input",
    "substring"
);

utf8_function!(
    SQLUtf8Rpad,
    "rpad",
    daft_functions::utf8::rpad,
    "Pads the string on the right side with the specified string until it reaches the specified length",
    "string_input", "length", "pad"
);

utf8_function!(
    SQLUtf8Lpad,
    "lpad",
    daft_functions::utf8::lpad,
    "Pads the string on the left side with the specified string until it reaches the specified length",
    "string_input", "length", "pad"
);

utf8_function!(
    SQLUtf8Repeat,
    "repeat",
    daft_functions::utf8::repeat,
    "Repeats the string the specified number of times",
    "string_input",
    "count"
);

pub struct SQLUtf8RegexpExtract;

impl SQLFunction for SQLUtf8RegexpExtract {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, pattern] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                Ok(daft_functions::utf8::extract(input, pattern, 0))
            }
            [input, pattern, idx] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                let idx = planner.plan_function_arg(idx)?.as_literal().and_then(LiteralValue::as_i64).ok_or_else(|| {
                    PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract, found {idx:?}"))
                })? as usize;
                Ok(daft_functions::utf8::extract(input, pattern, idx))
            }
            _ => invalid_operation_err!("regexp_extract takes exactly two or three arguments"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Extracts the first substring that matches the specified regular expression pattern"
            .to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["string_input", "pattern"]
    }
}

pub struct SQLUtf8RegexpExtractAll;

impl SQLFunction for SQLUtf8RegexpExtractAll {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input, pattern] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                Ok(daft_functions::utf8::extract_all(input, pattern, 0))
            }
            [input, pattern, idx] => {
                let input = planner.plan_function_arg(input)?;
                let pattern = planner.plan_function_arg(pattern)?;
                let idx = planner.plan_function_arg(idx)?.as_literal().and_then(LiteralValue::as_i64).ok_or_else(|| {
                    PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract_all, found {idx:?}"))
                })? as usize;
                Ok(daft_functions::utf8::extract_all(input, pattern, idx))
            }
            _ => invalid_operation_err!("regexp_extract_all takes exactly two or three arguments"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Extracts all substrings that match the specified regular expression pattern".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["string_input", "pattern"]
    }
}

pub struct SQLUtf8ToDate;

impl SQLFunction for SQLUtf8ToDate {
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
                        PlannerError::invalid_operation("to_date format must be a string")
                    })?;
                Ok(daft_functions::utf8::to_date(input, fmt))
            }
            _ => invalid_operation_err!("to_date takes exactly two arguments"),
        }
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Parses the string as a date using the specified format.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &["string_input", "format"]
    }
}

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
            _ => Err(PlannerError::invalid_operation(
                "Invalid arguments for count_matches: '{inputs:?}'",
            )),
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

pub struct SQLNormalize;

impl TryFrom<SQLFunctionArguments> for Utf8NormalizeOptions {
    type Error = PlannerError;

    fn try_from(args: SQLFunctionArguments) -> Result<Self, Self::Error> {
        let remove_punct = args.try_get_named("remove_punct")?.unwrap_or(false);
        let lowercase = args.try_get_named("lowercase")?.unwrap_or(false);
        let nfd_unicode = args.try_get_named("nfd_unicode")?.unwrap_or(false);
        let white_space = args.try_get_named("white_space")?.unwrap_or(false);

        Ok(Self {
            remove_punct,
            lowercase,
            nfd_unicode,
            white_space,
        })
    }
}

impl SQLFunction for SQLNormalize {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        match inputs {
            [input] => {
                let input = planner.plan_function_arg(input)?;
                Ok(daft_functions::utf8::normalize(
                    input,
                    Utf8NormalizeOptions::default(),
                ))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args: Utf8NormalizeOptions = planner.plan_function_args(
                    args,
                    &["remove_punct", "lowercase", "nfd_unicode", "white_space"],
                    0,
                )?;
                Ok(daft_functions::utf8::normalize(input, args))
            }
            _ => invalid_operation_err!("Invalid arguments for normalize"),
        }
    }
    fn docstrings(&self, _: &str) -> String {
        "Normalizes a string for more useful deduplication and data cleaning.".to_string()
    }

    fn arg_names(&self) -> &'static [&'static str] {
        &[
            "input",
            "remove_punct",
            "lowercase",
            "nfd_unicode",
            "white_space",
        ]
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
