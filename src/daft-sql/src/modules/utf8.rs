use daft_core::array::ops::Utf8NormalizeOptions;
use daft_dsl::{
    functions::{
        self,
        utf8::{normalize, Utf8Expr},
    },
    ExprRef, LiteralValue,
};
use daft_functions::{
    count_matches::{utf8_count_matches, CountMatchesFunction},
    tokenize::{tokenize_decode, tokenize_encode, TokenizeDecodeFunction, TokenizeEncodeFunction},
};

use super::SQLModule;
use crate::{
    ensure,
    error::{PlannerError, SQLPlannerResult},
    functions::{SQLFunction, SQLFunctionArguments},
    invalid_operation_err, unsupported_sql_err,
};

pub struct SQLModuleUtf8;

impl SQLModule for SQLModuleUtf8 {
    fn register(parent: &mut crate::functions::SQLFunctions) {
        use Utf8Expr::{
            Capitalize, Contains, EndsWith, Extract, ExtractAll, Find, Left, Length, LengthBytes,
            Lower, Lpad, Lstrip, Match, Repeat, Replace, Reverse, Right, Rpad, Rstrip, Split,
            StartsWith, ToDate, ToDatetime, Upper,
        };
        parent.add_fn("ends_with", EndsWith);
        parent.add_fn("starts_with", StartsWith);
        parent.add_fn("contains", Contains);
        parent.add_fn("split", Split(false));
        // TODO add split variants
        // parent.add("split", f(Split(false)));
        parent.add_fn("regexp_match", Match);
        parent.add_fn("regexp_extract", Extract(0));
        parent.add_fn("regexp_extract_all", ExtractAll(0));
        parent.add_fn("regexp_replace", Replace(true));
        parent.add_fn("regexp_split", Split(true));
        // TODO add replace variants
        // parent.add("replace", f(Replace(false)));
        parent.add_fn("length", Length);
        parent.add_fn("length_bytes", LengthBytes);
        parent.add_fn("lower", Lower);
        parent.add_fn("upper", Upper);
        parent.add_fn("lstrip", Lstrip);
        parent.add_fn("rstrip", Rstrip);
        parent.add_fn("reverse", Reverse);
        parent.add_fn("capitalize", Capitalize);
        parent.add_fn("left", Left);
        parent.add_fn("right", Right);
        parent.add_fn("find", Find);
        parent.add_fn("rpad", Rpad);
        parent.add_fn("lpad", Lpad);
        parent.add_fn("repeat", Repeat);

        parent.add_fn("to_date", ToDate(String::new()));
        parent.add_fn("to_datetime", ToDatetime(String::new(), None));
        parent.add_fn("count_matches", SQLCountMatches);
        parent.add_fn("normalize", SQLNormalize);
        parent.add_fn("tokenize_encode", SQLTokenizeEncode);
        parent.add_fn("tokenize_decode", SQLTokenizeDecode);
    }
}

impl SQLModuleUtf8 {}

impl SQLFunction for Utf8Expr {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        let inputs = self.args_to_expr_unnamed(inputs, planner)?;
        to_expr(self, &inputs)
    }

    fn docstrings(&self, _alias: &str) -> String {
        match self {
            Self::EndsWith => "Returns true if the string ends with the specified substring".to_string(),
            Self::StartsWith => "Returns true if the string starts with the specified substring".to_string(),
            Self::Contains => "Returns true if the string contains the specified substring".to_string(),
            Self::Split(_) => "Splits the string by the specified delimiter and returns an array of substrings".to_string(),
            Self::Match => "Returns true if the string matches the specified regular expression pattern".to_string(),
            Self::Extract(_) => "Extracts the first substring that matches the specified regular expression pattern".to_string(),
            Self::ExtractAll(_) => "Extracts all substrings that match the specified regular expression pattern".to_string(),
            Self::Replace(_) => "Replaces all occurrences of a substring with a new string".to_string(),
            Self::Like => "Returns true if the string matches the specified SQL LIKE pattern".to_string(),
            Self::Ilike => "Returns true if the string matches the specified SQL LIKE pattern (case-insensitive)".to_string(),
            Self::Length => "Returns the length of the string".to_string(),
            Self::Lower => "Converts the string to lowercase".to_string(),
            Self::Upper => "Converts the string to uppercase".to_string(),
            Self::Lstrip => "Removes leading whitespace from the string".to_string(),
            Self::Rstrip => "Removes trailing whitespace from the string".to_string(),
            Self::Reverse => "Reverses the order of characters in the string".to_string(),
            Self::Capitalize => "Capitalizes the first character of the string".to_string(),
            Self::Left => "Returns the specified number of leftmost characters from the string".to_string(),
            Self::Right => "Returns the specified number of rightmost characters from the string".to_string(),
            Self::Find => "Returns the index of the first occurrence of a substring within the string".to_string(),
            Self::Rpad => "Pads the string on the right side with the specified string until it reaches the specified length".to_string(),
            Self::Lpad => "Pads the string on the left side with the specified string until it reaches the specified length".to_string(),
            Self::Repeat => "Repeats the string the specified number of times".to_string(),
            Self::Substr => "Returns a substring of the string starting at the specified position and length".to_string(),
            Self::ToDate(_) => "Parses the string as a date using the specified format.".to_string(),
            Self::ToDatetime(_, _) => "Parses the string as a datetime using the specified format.".to_string(),
            Self::LengthBytes => "Returns the length of the string in bytes".to_string(),
            Self::Normalize(_) => unimplemented!("Normalize not implemented"),
        }
    }

    fn arg_names(&self) -> &'static [&'static str] {
        match self {
            Self::EndsWith => &["string_input", "substring"],
            Self::StartsWith => &["string_input", "substring"],
            Self::Contains => &["string_input", "substring"],
            Self::Split(_) => &["string_input", "delimiter"],
            Self::Match => &["string_input", "pattern"],
            Self::Extract(_) => &["string_input", "pattern"],
            Self::ExtractAll(_) => &["string_input", "pattern"],
            Self::Replace(_) => &["string_input", "pattern", "replacement"],
            Self::Like => &["string_input", "pattern"],
            Self::Ilike => &["string_input", "pattern"],
            Self::Length => &["string_input"],
            Self::Lower => &["string_input"],
            Self::Upper => &["string_input"],
            Self::Lstrip => &["string_input"],
            Self::Rstrip => &["string_input"],
            Self::Reverse => &["string_input"],
            Self::Capitalize => &["string_input"],
            Self::Left => &["string_input", "length"],
            Self::Right => &["string_input", "length"],
            Self::Find => &["string_input", "substring"],
            Self::Rpad => &["string_input", "length", "pad"],
            Self::Lpad => &["string_input", "length", "pad"],
            Self::Repeat => &["string_input", "count"],
            Self::Substr => &["string_input", "start", "length"],
            Self::ToDate(_) => &["string_input", "format"],
            Self::ToDatetime(_, _) => &["string_input", "format"],
            Self::LengthBytes => &["string_input"],
            Self::Normalize(_) => unimplemented!("Normalize not implemented"),
        }
    }
}

fn to_expr(expr: &Utf8Expr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
    use functions::utf8::{
        capitalize, contains, endswith, extract, extract_all, find, left, length, length_bytes,
        lower, lpad, lstrip, match_, repeat, replace, reverse, right, rpad, rstrip, split,
        startswith, to_date, to_datetime, upper, Utf8Expr,
    };
    use Utf8Expr::{
        Capitalize, Contains, EndsWith, Extract, ExtractAll, Find, Ilike, Left, Length,
        LengthBytes, Like, Lower, Lpad, Lstrip, Match, Normalize, Repeat, Replace, Reverse, Right,
        Rpad, Rstrip, Split, StartsWith, Substr, ToDate, ToDatetime, Upper,
    };
    match expr {
        EndsWith => {
            ensure!(args.len() == 2, "endswith takes exactly two arguments");
            Ok(endswith(args[0].clone(), args[1].clone()))
        }
        StartsWith => {
            ensure!(args.len() == 2, "startswith takes exactly two arguments");
            Ok(startswith(args[0].clone(), args[1].clone()))
        }
        Contains => {
            ensure!(args.len() == 2, "contains takes exactly two arguments");
            Ok(contains(args[0].clone(), args[1].clone()))
        }
        Split(true) => {
            ensure!(args.len() == 2, "split takes exactly two arguments");
            Ok(split(args[0].clone(), args[1].clone(), true))
        }
        Split(false) => {
            ensure!(args.len() == 2, "split takes exactly two arguments");
            Ok(split(args[0].clone(), args[1].clone(), false))
        }
        Match => {
            ensure!(args.len() == 2, "regexp_match takes exactly two arguments");
            Ok(match_(args[0].clone(), args[1].clone()))
        }
        Extract(_) => match args {
            [input, pattern] => Ok(extract(input.clone(), pattern.clone(), 0)),
            [input, pattern, idx] => {
                let idx = idx.as_literal().and_then(daft_dsl::LiteralValue::as_i64).ok_or_else(|| {
                   PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract, found {idx:?}"))
               })?;

                Ok(extract(input.clone(), pattern.clone(), idx as usize))
            }
            _ => {
                invalid_operation_err!("regexp_extract takes exactly two or three arguments")
            }
        },
        ExtractAll(_) => match args {
            [input, pattern] => Ok(extract_all(input.clone(), pattern.clone(), 0)),
            [input, pattern, idx] => {
                let idx = idx.as_literal().and_then(daft_dsl::LiteralValue::as_i64).ok_or_else(|| {
                   PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract, found {idx:?}"))
               })?;

                Ok(extract_all(input.clone(), pattern.clone(), idx as usize))
            }
            _ => {
                invalid_operation_err!("regexp_extract_all takes exactly two or three arguments")
            }
        },
        Replace(_) => {
            ensure!(args.len() == 3, "replace takes exactly three arguments");
            Ok(replace(
                args[0].clone(),
                args[1].clone(),
                args[2].clone(),
                false,
            ))
        }
        Like => {
            unreachable!("like should be handled by the parser")
        }
        Ilike => {
            unreachable!("ilike should be handled by the parser")
        }
        Length => {
            ensure!(args.len() == 1, "length takes exactly one argument");
            Ok(length(args[0].clone()))
        }
        LengthBytes => {
            ensure!(args.len() == 1, "length_bytes takes exactly one argument");
            Ok(length_bytes(args[0].clone()))
        }
        Lower => {
            ensure!(args.len() == 1, "lower takes exactly one argument");
            Ok(lower(args[0].clone()))
        }
        Upper => {
            ensure!(args.len() == 1, "upper takes exactly one argument");
            Ok(upper(args[0].clone()))
        }
        Lstrip => {
            ensure!(args.len() == 1, "lstrip takes exactly one argument");
            Ok(lstrip(args[0].clone()))
        }
        Rstrip => {
            ensure!(args.len() == 1, "rstrip takes exactly one argument");
            Ok(rstrip(args[0].clone()))
        }
        Reverse => {
            ensure!(args.len() == 1, "reverse takes exactly one argument");
            Ok(reverse(args[0].clone()))
        }
        Capitalize => {
            ensure!(args.len() == 1, "capitalize takes exactly one argument");
            Ok(capitalize(args[0].clone()))
        }
        Left => {
            ensure!(args.len() == 2, "left takes exactly two arguments");
            Ok(left(args[0].clone(), args[1].clone()))
        }
        Right => {
            ensure!(args.len() == 2, "right takes exactly two arguments");
            Ok(right(args[0].clone(), args[1].clone()))
        }
        Find => {
            ensure!(args.len() == 2, "find takes exactly two arguments");
            Ok(find(args[0].clone(), args[1].clone()))
        }
        Rpad => {
            ensure!(args.len() == 3, "rpad takes exactly three arguments");
            Ok(rpad(args[0].clone(), args[1].clone(), args[2].clone()))
        }
        Lpad => {
            ensure!(args.len() == 3, "lpad takes exactly three arguments");
            Ok(lpad(args[0].clone(), args[1].clone(), args[2].clone()))
        }
        Repeat => {
            ensure!(args.len() == 2, "repeat takes exactly two arguments");
            Ok(repeat(args[0].clone(), args[1].clone()))
        }
        Substr => {
            unreachable!("substr should be handled by the parser")
        }
        ToDate(_) => {
            ensure!(args.len() == 2, "to_date takes exactly two arguments");
            let fmt = match args[1].as_ref().as_literal() {
                Some(LiteralValue::Utf8(s)) => s,
                _ => invalid_operation_err!("to_date format must be a string"),
            };
            Ok(to_date(args[0].clone(), fmt))
        }
        ToDatetime(..) => {
            ensure!(
                args.len() >= 2,
                "to_datetime takes either two or three arguments"
            );
            let fmt = match args[1].as_ref().as_literal() {
                Some(LiteralValue::Utf8(s)) => s,
                _ => invalid_operation_err!("to_datetime format must be a string"),
            };
            let tz = match args.get(2).and_then(|e| e.as_ref().as_literal()) {
                Some(LiteralValue::Utf8(s)) => Some(s.as_str()),
                _ => invalid_operation_err!("to_datetime timezone must be a string"),
            };

            Ok(to_datetime(args[0].clone(), fmt, tz))
        }
        Normalize(_) => {
            unsupported_sql_err!("normalize")
        }
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
                Ok(normalize(input, Utf8NormalizeOptions::default()))
            }
            [input, args @ ..] => {
                let input = planner.plan_function_arg(input)?;
                let args: Utf8NormalizeOptions = planner.plan_function_args(
                    args,
                    &["remove_punct", "lowercase", "nfd_unicode", "white_space"],
                    0,
                )?;
                Ok(normalize(input, args))
            }
            _ => invalid_operation_err!("Invalid arguments for normalize"),
        }
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
}
