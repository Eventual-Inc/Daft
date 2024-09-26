use daft_dsl::{
    functions::{self, utf8::Utf8Expr},
    ExprRef, LiteralValue,
};
use daft_functions::count_matches::{utf8_count_matches, CountMatchesFunction};

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
        use Utf8Expr::*;
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

        parent.add_fn("to_date", ToDate("".to_string()));
        parent.add_fn("to_datetime", ToDatetime("".to_string(), None));
        parent.add_fn("count_matches", SQLCountMatches);
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
                let idx = idx.as_literal().and_then(|lit| lit.as_i64()).ok_or_else(|| {
                   PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract, found {:?}", idx))
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
                let idx = idx.as_literal().and_then(|lit| lit.as_i64()).ok_or_else(|| {
                   PlannerError::invalid_operation(format!("Expected a literal integer for the third argument of regexp_extract, found {:?}", idx))
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
        let whole_words = args
            .get_named("whole_words")
            .map(|v| {
                v.as_literal().and_then(|lit| lit.as_bool()).ok_or_else(|| {
                    PlannerError::invalid_operation(format!(
                        "whole_words argument must be a boolean, got {:?}",
                        v
                    ))
                })
            })
            .transpose()?
            .unwrap_or(false);

        let case_sensitive = args
            .get_named("case_sensitive")
            .map(|v| {
                v.as_literal().and_then(|lit| lit.as_bool()).ok_or_else(|| {
                    PlannerError::invalid_operation(format!(
                        "case_sensitive argument must be a boolean, got {:?}",
                        v
                    ))
                })
            })
            .transpose()?
            .unwrap_or(true);

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
