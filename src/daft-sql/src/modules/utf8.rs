use daft_dsl::{
    functions::{self, utf8::Utf8Expr, FunctionExpr},
    ExprRef, LiteralValue,
};

use crate::{ensure, error::SQLPlannerResult, invalid_operation_err, unsupported_sql_err};

use super::SQLModule;

pub struct SQLModuleUtf8;

impl SQLModule for SQLModuleUtf8 {
    fn register(parent: &mut crate::functions::SQLFunctions) {
        use FunctionExpr::Utf8 as f;
        use Utf8Expr::*;
        parent.add_fn("ends_with", f(EndsWith));
        parent.add_fn("starts_with", f(StartsWith));
        parent.add_fn("contains", f(Contains));
        parent.add_fn("split", f(Split(true)));
        // TODO add split variants
        // parent.add("split", f(Split(false)));
        parent.add_fn("match", f(Match));
        parent.add_fn("extract", f(Extract(0)));
        parent.add_fn("extract_all", f(ExtractAll(0)));
        parent.add_fn("replace", f(Replace(true)));
        // TODO add replace variants
        // parent.add("replace", f(Replace(false)));
        parent.add_fn("length", f(Length));
        parent.add_fn("lower", f(Lower));
        parent.add_fn("upper", f(Upper));
        parent.add_fn("lstrip", f(Lstrip));
        parent.add_fn("rstrip", f(Rstrip));
        parent.add_fn("reverse", f(Reverse));
        parent.add_fn("capitalize", f(Capitalize));
        parent.add_fn("left", f(Left));
        parent.add_fn("right", f(Right));
        parent.add_fn("find", f(Find));
        parent.add_fn("rpad", f(Rpad));
        parent.add_fn("lpad", f(Lpad));
        parent.add_fn("repeat", f(Repeat));
        parent.add_fn("like", f(Like));
        parent.add_fn("ilike", f(Ilike));
        parent.add_fn("substr", f(Substr));
        parent.add_fn("to_date", f(ToDate("".to_string())));
        parent.add_fn("to_datetime", f(ToDatetime("".to_string(), None)));
        // TODO add normalization variants.
        // parent.add("normalize", f(Normalize(Default::default())));
    }
}

impl SQLModuleUtf8 {}
pub(crate) fn to_expr(expr: &Utf8Expr, args: &[ExprRef]) -> SQLPlannerResult<ExprRef> {
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
