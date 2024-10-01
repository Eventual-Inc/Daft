use daft_dsl::{
    functions::{self, utf8::Utf8Expr},
    ExprRef, LiteralValue,
};

use super::SQLModule;
use crate::{
    ensure, error::SQLPlannerResult, functions::SQLFunction, invalid_operation_err,
    unsupported_sql_err,
};

pub struct SQLModuleUtf8;

impl SQLModule for SQLModuleUtf8 {
    fn register(parent: &mut crate::functions::SQLFunctions) {
        use Utf8Expr::*;
        parent.add_fn("ends_with", EndsWith);
        parent.add_fn("starts_with", StartsWith);
        parent.add_fn("contains", Contains);
        parent.add_fn("split", Split(true));
        // TODO add split variants
        // parent.add("split", f(Split(false)));
        parent.add_fn("match", Match);
        parent.add_fn("extract", Extract(0));
        parent.add_fn("extract_all", ExtractAll(0));
        parent.add_fn("replace", Replace(true));
        // TODO add replace variants
        // parent.add("replace", f(Replace(false)));
        parent.add_fn("length", Length);
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
        parent.add_fn("like", Like);
        parent.add_fn("ilike", Ilike);
        parent.add_fn("substr", Substr);
        parent.add_fn("to_date", ToDate("".to_string()));
        parent.add_fn("to_datetime", ToDatetime("".to_string(), None));
        // TODO add normalization variants.
        // parent.add("normalize", f(Normalize(Default::default())));
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
    use functions::utf8::*;
    use Utf8Expr::*;
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
        Split(_) => {
            ensure!(args.len() == 2, "split takes exactly two arguments");
            Ok(split(args[0].clone(), args[1].clone(), false))
        }
        Match => {
            unsupported_sql_err!("match")
        }
        Extract(_) => {
            unsupported_sql_err!("extract")
        }
        ExtractAll(_) => {
            unsupported_sql_err!("extract_all")
        }
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
            unsupported_sql_err!("like")
        }
        Ilike => {
            unsupported_sql_err!("ilike")
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
            ensure!(args.len() == 3, "substr takes exactly three arguments");
            Ok(substr(args[0].clone(), args[1].clone(), args[2].clone()))
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
