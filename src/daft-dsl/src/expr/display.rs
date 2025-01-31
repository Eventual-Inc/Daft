use std::fmt::Write;

use itertools::Itertools;

use super::{Expr, ExprRef, Operator};

/// Display for Expr::BinaryOp
pub fn expr_binary_op_display_without_formatter(
    op: &Operator,
    left: &ExprRef,
    right: &ExprRef,
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    let write_out_expr = |f: &mut String, input: &Expr| match input {
        Expr::Alias(e, _) => write!(f, "{e}"),
        Expr::BinaryOp { .. } => write!(f, "[{input}]"),
        _ => write!(f, "{input}"),
    };
    write_out_expr(&mut f, left)?;
    write!(&mut f, " {op} ")?;
    write_out_expr(&mut f, right)?;
    Ok(f)
}

/// Display for Expr::IsIn
pub fn expr_is_in_display_without_formatter(
    expr: &ExprRef,
    inputs: &[ExprRef],
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    write!(&mut f, "{expr} IN (")?;
    for (i, input) in inputs.iter().enumerate() {
        if i != 0 {
            write!(&mut f, ", ")?;
        }
        write!(&mut f, "{input}")?;
    }
    write!(&mut f, ")")?;
    Ok(f)
}

/// Display for Expr::List
pub fn expr_list_display_without_formatter(
    items: &[ExprRef],
) -> std::result::Result<String, std::fmt::Error> {
    let mut f = String::default();
    write!(
        &mut f,
        "list({})",
        items.iter().map(|x| x.to_string()).join(", ")
    )?;
    Ok(f)
}
