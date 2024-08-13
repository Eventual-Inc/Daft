use daft_dsl::{ExprRef, LiteralValue};
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};
use strum::EnumString;

use crate::{
    ensure,
    error::{PlannerError, SQLPlannerResult},
    invalid_operation_err,
    planner::{Relation, SQLPlanner},
    unsupported_sql_err,
};

// TODO: expand this to support more functions
#[derive(EnumString)]
#[strum(serialize_all = "snake_case")]
#[strum(ascii_case_insensitive)]
pub enum SQLFunctions {
    // ------------------------------------------------
    // Numeric Functions
    // ------------------------------------------------
    /// SQL `abs()` function
    /// # Example
    /// ```sql
    /// SELECT abs(-1);
    /// ```
    Abs,
    /// SQL `ceil()` function
    /// # Example
    /// ```sql
    /// SELECT ceil(1.1);
    /// ```
    Ceil,
    /// SQL `floor()` function
    /// # Example
    /// ```sql
    /// SELECT floor(1.1);
    /// ```
    Floor,
    /// SQL `sign()` function
    /// # Example
    /// ```sql
    /// SELECT sign(-1);
    /// ```
    Sign,
    /// SQL `round()` function
    /// # Example
    /// ```sql
    /// SELECT round(1.1);
    /// ```
    Round,
    /// SQL `sqrt()` function
    /// # Example
    /// ```sql
    /// SELECT sqrt(4);
    /// ```
    Sqrt,
    /// SQL `sin()` function
    /// # Example
    /// ```sql
    /// SELECT sin(0);
    /// ```
    Sin,
    /// SQL `cos()` function
    /// # Example
    /// ```sql
    /// SELECT cos(0);
    /// ```
    Cos,
    /// SQL `tan()` function
    /// # Example
    /// ```sql
    /// SELECT tan(0);
    /// ```
    Tan,
    /// SQL `cot()` function
    /// # Example
    /// ```sql
    /// SELECT cot(0);
    /// ```
    Cot,
    /// SQL `asin()` function
    /// # Example
    /// ```sql
    /// SELECT asin(0);
    /// ```
    #[strum(serialize = "asin", serialize = "arcsin")]
    ArcSin,
    /// SQL `acos()` function
    /// # Example
    /// ```sql
    /// SELECT acos(0);
    /// ```
    #[strum(serialize = "acos", serialize = "arccos")]
    ArcCos,
    /// SQL `atan()` function
    /// # Example
    /// ```sql
    /// SELECT atan(0);
    /// ```
    #[strum(serialize = "atan", serialize = "arctan")]
    ArcTan,
    /// SQL `atan2()` function
    /// # Example
    /// ```sql
    /// SELECT atan2(0);
    /// ```
    #[strum(serialize = "atan2", serialize = "arctan2")]
    ArcTan2,
    /// SQL `radians()` function
    /// # Example
    /// ```sql
    /// SELECT radians(0);
    /// ```
    Radians,
    /// SQL `degrees()` function
    /// # Example
    /// ```sql
    /// SELECT degrees(0);
    /// ```
    Degrees,
    /// SQL `log2()` function
    /// # Example
    /// ```sql
    /// SELECT log2(1);
    /// ```
    Log2,
    /// SQL `log10()` function
    /// # Example
    /// ```sql
    /// SELECT log10(1);
    /// ```
    Log10,
    /// SQL `ln()` function
    /// # Example
    /// ```sql
    /// SELECT ln(1);
    /// ```
    Ln,
    /// SQL `log()` function
    /// # Example
    /// ```sql
    /// SELECT log(1, 10);
    /// ```
    Log,
    /// SQL `exp()` function
    /// # Example
    /// ```sql
    /// SELECT exp(1);
    /// ```
    Exp,
    /// SQL `atanh()` function
    /// # Example
    /// ```sql
    /// SELECT atanh(0);
    /// ```
    #[strum(serialize = "atanh", serialize = "arctanh")]
    ArcTanh,
    /// SQL `acosh()` function
    /// # Example
    /// ```sql
    /// SELECT acosh(1);
    /// ```
    #[strum(serialize = "acosh", serialize = "arccosh")]
    ArcCosh,
    /// SQL `asinh()` function
    /// # Example
    /// ```sql
    /// SELECT asinh(0);
    /// ```
    #[strum(serialize = "asinh", serialize = "arcsinh")]
    ArcSinh,

    // ------------------------------------------------
    // String Functions
    // ------------------------------------------------
    /// SQL `ends_with()` function
    /// # Example
    /// ```sql
    /// SELECT ends_with('hello', 'lo');
    /// ```
    EndsWith,
    /// SQL `starts_with()` function
    /// # Example
    /// ```sql
    /// SELECT starts_with('hello', 'he');
    /// ```
    StartsWith,
    /// SQL `contains()` function
    /// # Example
    /// ```sql
    /// SELECT contains('hello', 'el');
    /// ```
    Contains,
    /// SQL `split()` function
    /// # Example
    /// ```sql
    /// SELECT split('hello,world', ',');
    /// ```
    Split,

    Extract,
    ExtractAll,
    /// SQL 'replace()' function
    /// # Example
    /// ```sql
    /// SELECT replace('hello', 'l', 'r');
    /// ```
    Replace,
    /// SQL 'length()' function
    /// # Example
    /// ```sql
    /// SELECT length('hello');
    /// ```
    Length,
    /// SQL 'lower()' function
    /// # Example
    /// ```sql
    /// SELECT lower('HELLO');
    /// ```
    Lower,
    /// SQL 'upper()' function
    /// # Example
    /// ```sql
    /// SELECT upper('hello');
    /// ```
    Upper,
    /// SQL 'lstrip()' function
    /// # Example
    /// ```sql
    /// SELECT lstrip(' hello');
    /// ```
    Lstrip,
    /// SQL 'rstrip()' function
    /// # Example
    /// ```sql
    /// SELECT rstrip('hello ');
    /// ```
    Rstrip,
    /// SQL 'reverse()' function
    /// # Example
    /// ```sql
    /// SELECT reverse('olleh');
    /// ```
    Reverse,
    /// SQL 'capitalize()' function
    /// # Example
    /// ```sql
    /// SELECT capitalize('hello');
    /// ```
    Capitalize,
    /// SQL 'left()' function
    /// # Example
    /// ```sql
    /// SELECT left('hello', 2);
    /// ```
    Left,

    /// SQL 'right()' function
    /// # Example
    /// ```sql
    /// SELECT right('hello', 2);
    /// ```
    Right,
    /// SQL 'find()' function
    /// # Example
    /// ```sql
    /// SELECT find('hello', 'l');
    /// ```
    Find,
    /// SQL 'rpad()' function
    /// # Example
    /// ```sql
    /// SELECT rpad('hello', 10, ' ');
    /// ```
    Rpad,
    /// SQL 'lpad()' function
    /// # Example
    /// ```sql
    /// SELECT lpad('hello', 10, ' ');
    /// ```
    Lpad,
    /// SQL 'repeat()' function
    /// # Example
    /// ```sql
    /// SELECT repeat('X', 2);
    /// ```
    Repeat,
    // Like,
    // Ilike,
    /// SQL 'substring()' function
    /// # Example
    /// ```sql
    /// SELECT substring('hello', 1, 2);
    /// ```
    Substr,
    /// SQL 'to_date()` function
    /// # Example
    /// ```sql
    /// SELECT to_date('2021-01-01', 'YYYY-MM-DD');
    /// ```
    ToDate,
    /// SQL 'to_datetime()' function
    /// # Example
    /// ```sql
    /// SELECT to_datetime('2021-01-01 00:00:00', 'YYYY-MM-DD HH:MM:SS');
    /// ```
    ToDatetime,

    // ------------------------------------------------
    // Aggregate Functions
    // ------------------------------------------------
    Max,
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
                ensure!(args.len() == 1, "abs takes exactly one argument");
                Ok(daft_dsl::functions::numeric::abs(args[0].clone()))
            }
            SQLFunctions::Ceil => {
                ensure!(args.len() == 1, "ceil takes exactly one argument");
                Ok(daft_dsl::functions::numeric::ceil(args[0].clone()))
            }
            SQLFunctions::Floor => {
                ensure!(args.len() == 1, "floor takes exactly one argument");
                Ok(daft_dsl::functions::numeric::floor(args[0].clone()))
            }
            SQLFunctions::Sign => {
                ensure!(args.len() == 1, "sign takes exactly one argument");
                Ok(daft_dsl::functions::numeric::sign(args[0].clone()))
            }
            SQLFunctions::Round => {
                ensure!(args.len() == 2, "round takes exactly two arguments");
                let precision = match args[1].as_ref().as_literal() {
                    Some(LiteralValue::Int32(i)) => *i,
                    Some(LiteralValue::UInt32(u)) => *u as i32,
                    Some(LiteralValue::Int64(i)) => *i as i32,
                    _ => invalid_operation_err!("round precision must be an integer"),
                };
                Ok(daft_dsl::functions::numeric::round(
                    args[0].clone(),
                    precision,
                ))
            }
            SQLFunctions::Sqrt => {
                ensure!(args.len() == 1, "sqrt takes exactly one argument");
                Ok(daft_dsl::functions::numeric::sqrt(args[0].clone()))
            }
            SQLFunctions::Sin => {
                ensure!(args.len() == 1, "sin takes exactly one argument");
                Ok(daft_dsl::functions::numeric::sin(args[0].clone()))
            }
            SQLFunctions::Cos => {
                ensure!(args.len() == 1, "cos takes exactly one argument");
                Ok(daft_dsl::functions::numeric::cos(args[0].clone()))
            }
            SQLFunctions::Tan => {
                ensure!(args.len() == 1, "tan takes exactly one argument");
                Ok(daft_dsl::functions::numeric::tan(args[0].clone()))
            }
            SQLFunctions::Cot => {
                ensure!(args.len() == 1, "cot takes exactly one argument");
                Ok(daft_dsl::functions::numeric::cot(args[0].clone()))
            }
            SQLFunctions::ArcSin => {
                ensure!(args.len() == 1, "asin takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arcsin(args[0].clone()))
            }
            SQLFunctions::ArcCos => {
                ensure!(args.len() == 1, "acos takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arccos(args[0].clone()))
            }
            SQLFunctions::ArcTan => {
                ensure!(args.len() == 1, "atan takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arctan(args[0].clone()))
            }
            SQLFunctions::ArcTan2 => {
                ensure!(args.len() == 2, "atan2 takes exactly two arguments");
                Ok(daft_dsl::functions::numeric::arctan2(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Degrees => {
                ensure!(args.len() == 1, "degrees takes exactly one argument");
                Ok(daft_dsl::functions::numeric::degrees(args[0].clone()))
            }
            SQLFunctions::Radians => {
                ensure!(args.len() == 1, "radians takes exactly one argument");
                Ok(daft_dsl::functions::numeric::radians(args[0].clone()))
            }
            SQLFunctions::Log2 => {
                ensure!(args.len() == 1, "log2 takes exactly one argument");
                Ok(daft_dsl::functions::numeric::log2(args[0].clone()))
            }
            SQLFunctions::Log10 => {
                ensure!(args.len() == 1, "log10 takes exactly one argument");
                Ok(daft_dsl::functions::numeric::log10(args[0].clone()))
            }
            SQLFunctions::Ln => {
                ensure!(args.len() == 1, "ln takes exactly one argument");
                Ok(daft_dsl::functions::numeric::ln(args[0].clone()))
            }
            SQLFunctions::Log => {
                ensure!(args.len() == 2, "log takes exactly two arguments");
                let base = args[1]
                    .as_literal()
                    .and_then(|lit| match lit {
                        LiteralValue::Float64(f) => Some(*f),
                        LiteralValue::Int32(i) => Some(*i as f64),
                        LiteralValue::UInt32(u) => Some(*u as f64),
                        LiteralValue::Int64(i) => Some(*i as f64),
                        LiteralValue::UInt64(u) => Some(*u as f64),
                        _ => None,
                    })
                    .ok_or_else(|| PlannerError::InvalidOperation {
                        message: "log base must be a float or a number".to_string(),
                    })?;

                Ok(daft_dsl::functions::numeric::log(args[0].clone(), base))
            }
            SQLFunctions::Exp => {
                ensure!(args.len() == 1, "exp takes exactly one argument");
                Ok(daft_dsl::functions::numeric::exp(args[0].clone()))
            }
            SQLFunctions::ArcTanh => {
                ensure!(args.len() == 1, "atanh takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arctanh(args[0].clone()))
            }
            SQLFunctions::ArcCosh => {
                ensure!(args.len() == 1, "acosh takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arccosh(args[0].clone()))
            }
            SQLFunctions::ArcSinh => {
                ensure!(args.len() == 1, "asinh takes exactly one argument");
                Ok(daft_dsl::functions::numeric::arcsinh(args[0].clone()))
            }
            SQLFunctions::EndsWith => {
                ensure!(args.len() == 2, "endswith takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::endswith(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::StartsWith => {
                ensure!(args.len() == 2, "startswith takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::startswith(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Contains => {
                ensure!(args.len() == 2, "contains takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::contains(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Split => {
                ensure!(args.len() == 2, "split takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::split(
                    args[0].clone(),
                    args[1].clone(),
                    false,
                ))
            }

            SQLFunctions::Extract => {
                unsupported_sql_err!("extract")
            }
            SQLFunctions::ExtractAll => {
                unsupported_sql_err!("extract_all")
            }
            SQLFunctions::Replace => {
                ensure!(args.len() == 3, "replace takes exactly three arguments");
                Ok(daft_dsl::functions::utf8::replace(
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                    false,
                ))
            }
            SQLFunctions::Length => {
                ensure!(args.len() == 1, "length takes exactly one argument");
                Ok(daft_dsl::functions::utf8::length(args[0].clone()))
            }
            SQLFunctions::Lower => {
                ensure!(args.len() == 1, "lower takes exactly one argument");
                Ok(daft_dsl::functions::utf8::lower(args[0].clone()))
            }
            SQLFunctions::Upper => {
                ensure!(args.len() == 1, "upper takes exactly one argument");
                Ok(daft_dsl::functions::utf8::upper(args[0].clone()))
            }
            SQLFunctions::Lstrip => {
                ensure!(args.len() == 1, "lstrip takes exactly one argument");
                Ok(daft_dsl::functions::utf8::lstrip(args[0].clone()))
            }
            SQLFunctions::Rstrip => {
                ensure!(args.len() == 1, "rstrip takes exactly one argument");
                Ok(daft_dsl::functions::utf8::rstrip(args[0].clone()))
            }
            SQLFunctions::Reverse => {
                ensure!(args.len() == 1, "reverse takes exactly one argument");
                Ok(daft_dsl::functions::utf8::reverse(args[0].clone()))
            }
            SQLFunctions::Capitalize => {
                ensure!(args.len() == 1, "capitalize takes exactly one argument");
                Ok(daft_dsl::functions::utf8::capitalize(args[0].clone()))
            }
            SQLFunctions::Left => {
                ensure!(args.len() == 2, "left takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::left(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Right => {
                ensure!(args.len() == 2, "right takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::right(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Find => {
                ensure!(args.len() == 2, "find takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::find(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Rpad => {
                ensure!(args.len() == 3, "rpad takes exactly three arguments");
                Ok(daft_dsl::functions::utf8::rpad(
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                ))
            }
            SQLFunctions::Lpad => {
                ensure!(args.len() == 3, "lpad takes exactly three arguments");
                Ok(daft_dsl::functions::utf8::lpad(
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                ))
            }
            SQLFunctions::Repeat => {
                ensure!(args.len() == 2, "repeat takes exactly two arguments");
                Ok(daft_dsl::functions::utf8::repeat(
                    args[0].clone(),
                    args[1].clone(),
                ))
            }
            SQLFunctions::Substr => {
                ensure!(args.len() == 3, "substr takes exactly three arguments");
                Ok(daft_dsl::functions::utf8::substr(
                    args[0].clone(),
                    args[1].clone(),
                    args[2].clone(),
                ))
            }
            SQLFunctions::ToDate => {
                ensure!(args.len() == 2, "to_date takes exactly two arguments");
                let fmt = match args[1].as_ref().as_literal() {
                    Some(LiteralValue::Utf8(s)) => s,
                    _ => invalid_operation_err!("to_date format must be a string"),
                };
                Ok(daft_dsl::functions::utf8::to_date(args[0].clone(), fmt))
            }
            SQLFunctions::ToDatetime => {
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

                Ok(daft_dsl::functions::utf8::to_datetime(
                    args[0].clone(),
                    fmt,
                    tz,
                ))
            }

            SQLFunctions::Max => {
                ensure!(args.len() == 1, "max takes exactly one argument");
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
