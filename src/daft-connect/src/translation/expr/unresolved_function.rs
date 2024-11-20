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
        "count" => handle_count(arguments).wrap_err("Failed to handle count function"),
        "isnotnull" => handle_isnotnull(arguments).wrap_err("Failed to handle isnotnull function"),
        "isnull" => handle_isnull(arguments).wrap_err("Failed to handle isnull function"),
        n => bail!("Unresolved function {n} not yet supported"),
    }
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
