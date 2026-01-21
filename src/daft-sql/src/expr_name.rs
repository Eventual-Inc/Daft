use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, UnaryOperator, Value, ValueWithSpan,
};

fn ident_to_string(ident: &Ident) -> String {
    ident.value.clone()
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident_to_string(ident),
            ObjectNamePart::Function(func) => func.name.value.clone(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Number(n, _) => n.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => format!("\"{s}\""),
        other => format!("{other}"),
    }
}

fn value_with_span_to_string(v: &ValueWithSpan) -> String {
    value_to_string(&v.value)
}

fn binary_op_to_string(op: &BinaryOperator) -> String {
    match op {
        BinaryOperator::Plus => "+".to_string(),
        BinaryOperator::Minus => "-".to_string(),
        BinaryOperator::Multiply => "*".to_string(),
        BinaryOperator::Divide => "/".to_string(),
        BinaryOperator::Modulo => "%".to_string(),
        BinaryOperator::StringConcat => "||".to_string(),
        other => format!("{other}"),
    }
}

fn unary_op_to_string(op: &UnaryOperator) -> String {
    match op {
        UnaryOperator::Plus => "+".to_string(),
        UnaryOperator::Minus => "-".to_string(),
        UnaryOperator::Not => "not".to_string(),
        other => format!("{other}"),
    }
}

fn expr_to_string(e: &SQLExpr) -> String {
    match e {
        SQLExpr::Identifier(ident) => ident_to_string(ident),
        SQLExpr::CompoundIdentifier(idents) => idents
            .iter()
            .map(ident_to_string)
            .collect::<Vec<_>>()
            .join("."),
        SQLExpr::Value(v) => value_with_span_to_string(v),

        SQLExpr::UnaryOp { op, expr } => {
            let op = unary_op_to_string(op);
            format!("({op} {})", expr_to_string(expr))
        }
        SQLExpr::BinaryOp { left, op, right } => {
            let op = binary_op_to_string(op);
            format!("({} {op} {})", expr_to_string(left), expr_to_string(right))
        }

        SQLExpr::Nested(inner) => expr_to_string(inner),
        SQLExpr::Cast {
            expr, data_type, ..
        } => {
            format!("cast({} as {data_type})", expr_to_string(expr))
        }

        SQLExpr::Function(func) => {
            let fn_name = object_name_to_string(&func.name).to_lowercase();

            // Special-case COUNT(*) to preserve the star.
            if fn_name == "count"
                && matches!(
                    &func.args,
                    FunctionArguments::List(args)
                        if args.args.len() == 1
                            && matches!(&args.args[0], FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
                )
            {
                return "count(*)".to_string();
            }

            let args: Vec<String> = match &func.args {
                FunctionArguments::None => vec![],
                FunctionArguments::Subquery(_) => vec!["<subquery>".to_string()],
                FunctionArguments::List(args) => args
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            match arg {
                                FunctionArgExpr::Expr(e) => expr_to_string(e),
                                FunctionArgExpr::QualifiedWildcard(obj) => {
                                    format!("{}.*", object_name_to_string(obj))
                                }
                                FunctionArgExpr::Wildcard => "*".to_string(),
                            }
                        }
                        FunctionArg::Unnamed(arg) => match arg {
                            FunctionArgExpr::Expr(e) => expr_to_string(e),
                            FunctionArgExpr::QualifiedWildcard(obj) => {
                                format!("{}.*", object_name_to_string(obj))
                            }
                            FunctionArgExpr::Wildcard => "*".to_string(),
                        },
                    })
                    .collect(),
            };

            format!("{fn_name}({})", args.join(", "))
        }

        // If we haven't normalized a specific expression yet, fall back to sqlparser's Display
        // to avoid changing the set of supported queries.
        other => format!("{other}"),
    }
}

/// Returns a stable, user-facing name for an unnamed SQL projection expression.
///
/// This is intended to be shared across SQL (AST -> name) and, in the future,
/// DataFrame (DSL Expr -> name) so that default column names follow consistent rules.
pub fn normalized_sql_expr_name(expr: &SQLExpr) -> String {
    expr_to_string(expr)
}
