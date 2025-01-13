use daft_core::count_mode::CountMode;
use eyre::{bail, Context};
use spark_connect::expression::UnresolvedFunction;

use crate::translation::to_daft_expr;

pub fn unresolved_to_daft_expr(f: &UnresolvedFunction) -> eyre::Result<daft_dsl::ExprRef> {
    let UnresolvedFunction {
        function_name,
        arguments,
        is_distinct,
        is_user_defined_function,
    } = f;

    let arguments: Vec<_> = arguments.iter().map(to_daft_expr).try_collect()?;

    if *is_distinct {
        bail!("Distinct not yet supported");
    }

    if *is_user_defined_function {
        bail!("User-defined functions not yet supported");
    }

    match function_name.as_str() {
        "%" => handle_binary_op(arguments, daft_dsl::Operator::Modulus),
        "<" => handle_binary_op(arguments, daft_dsl::Operator::Lt),
        "<=" => handle_binary_op(arguments, daft_dsl::Operator::LtEq),
        "==" => handle_binary_op(arguments, daft_dsl::Operator::Eq),
        ">" => handle_binary_op(arguments, daft_dsl::Operator::Gt),
        ">=" => handle_binary_op(arguments, daft_dsl::Operator::GtEq),
        "count" => handle_count(arguments),
        "isnotnull" => handle_isnotnull(arguments),
        "isnull" => handle_isnull(arguments),
        "not" => not(arguments),
        "sum" => handle_sum(arguments),
        n => bail!("Unresolved function {n:?} not yet supported"),
    }
    .wrap_err_with(|| format!("Failed to handle function {function_name:?}"))
}

pub fn handle_sum(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;
    Ok(arg.sum())
}

/// If the arguments are exactly one, return it. Otherwise, return an error.
pub fn to_single(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;

    Ok(arg)
}

pub fn not(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arg = to_single(arguments)?;
    Ok(arg.not())
}

pub fn handle_binary_op(
    arguments: Vec<daft_dsl::ExprRef>,
    op: daft_dsl::Operator,
) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 2] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly two arguments; got {arguments:?}");
        }
    };

    let [left, right] = arguments;

    Ok(daft_dsl::binary_op(op, left, right))
}

pub fn handle_count(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;

    let count = arg.count(CountMode::All);

    Ok(count)
}

pub fn handle_isnull(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;

    Ok(arg.is_null())
}

pub fn handle_isnotnull(arguments: Vec<daft_dsl::ExprRef>) -> eyre::Result<daft_dsl::ExprRef> {
    let arguments: [daft_dsl::ExprRef; 1] = match arguments.try_into() {
        Ok(arguments) => arguments,
        Err(arguments) => {
            bail!("requires exactly one argument; got {arguments:?}");
        }
    };

    let [arg] = arguments;

    Ok(arg.not_null())
}
