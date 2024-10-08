#[cfg(test)]
mod tests;

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TransformedResult, TreeNode};
use daft_core::prelude::*;

use crate::{col, expr::has_agg, has_stateful_udf, AggExpr, ApproxPercentileParams, Expr, ExprRef};

// Calculates all the possible struct get expressions in a schema.
// For each sugared string, calculates all possible corresponding expressions, in order of priority.
fn calculate_struct_expr_map(schema: &Schema) -> HashMap<String, Vec<ExprRef>> {
    #[derive(PartialEq, Eq)]
    struct BfsState<'a> {
        name: String,
        expr: ExprRef,
        field: &'a Field,
    }

    impl Ord for BfsState<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.name.cmp(&other.name)
        }
    }

    impl PartialOrd for BfsState<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut pq: BinaryHeap<BfsState> = BinaryHeap::new();

    for field in schema.fields.values() {
        pq.push(BfsState {
            name: field.name.clone(),
            expr: Arc::new(Expr::Column(field.name.clone().into())),
            field,
        });
    }

    let mut str_to_get_expr: HashMap<String, Vec<ExprRef>> = HashMap::new();

    while let Some(BfsState { name, expr, field }) = pq.pop() {
        if let Some(expr_vec) = str_to_get_expr.get_mut(&name) {
            expr_vec.push(expr.clone());
        } else {
            str_to_get_expr.insert(name.clone(), vec![expr.clone()]);
        }

        if let DataType::Struct(children) = &field.dtype {
            for child in children {
                pq.push(BfsState {
                    name: format!("{}.{}", name, child.name),
                    expr: crate::functions::struct_::get(expr.clone(), &child.name),
                    field: child,
                });
            }
        }
    }

    str_to_get_expr
}

/// Converts an expression with syntactic sugar into struct gets.
/// Does left-associative parsing to to resolve ambiguity.
///
/// For example, if col("a.b.c") could be interpreted as either col("a.b").struct.get("c")
/// or col("a").struct.get("b.c"), this function will resolve it to col("a.b").struct.get("c").
fn transform_struct_gets(
    expr: ExprRef,
    struct_expr_map: &HashMap<String, Vec<ExprRef>>,
) -> DaftResult<ExprRef> {
    expr.transform(|e| match e.as_ref() {
        Expr::Column(name) => struct_expr_map
            .get(name.as_ref())
            .ok_or(DaftError::ValueError(format!(
                "Column not found in schema: {name}"
            )))
            .map(|expr_vec| {
                let get_expr = expr_vec.first().unwrap();
                if expr_vec.len() > 1 {
                    log::warn!("Warning: Multiple matches found for col({name}), choosing left-associatively");
                }
                match get_expr.as_ref() {
                    Expr::Column(_) => Transformed::no(e.clone()),
                    _ => Transformed::yes(get_expr.clone()),
                }
            }),
        _ => Ok(Transformed::no(e)),
    })
        .data()
}

// Finds the names of all the wildcard expressions in an expression tree.
// Needs the schema because column names with stars must not count as wildcards
fn find_wildcards(expr: ExprRef, struct_expr_map: &HashMap<String, Vec<ExprRef>>) -> Vec<Arc<str>> {
    match expr.as_ref() {
        Expr::Column(name) => {
            if name.contains('*') {
                if struct_expr_map.contains_key(name.as_ref()) {
                    log::warn!(
                        "Warning: Column '{name}' contains *, preventing potential wildcard match"
                    );
                    Vec::new()
                } else {
                    vec![name.clone()]
                }
            } else {
                Vec::new()
            }
        }
        _ => expr
            .children()
            .into_iter()
            .flat_map(|e| find_wildcards(e, struct_expr_map))
            .collect(),
    }
}

// Calculates a list of all wildcard matches against a schema.
fn get_wildcard_matches(
    pattern: &str,
    schema: &Schema,
    struct_expr_map: &HashMap<String, Vec<ExprRef>>,
) -> DaftResult<Vec<String>> {
    if pattern == "*" {
        // return all top-level columns
        return Ok(schema.fields.keys().cloned().collect());
    }

    if !pattern.ends_with(".*") {
        return Err(DaftError::ValueError(format!(
            "Unsupported wildcard format: {pattern}"
        )));
    }

    // remove last two characters (should always be ".*")
    let struct_name = &pattern[..pattern.len() - 2];

    let Some(struct_expr_vec) = struct_expr_map.get(struct_name) else {
        return Err(DaftError::ValueError(format!(
            "Error matching wildcard {pattern}: struct {struct_name} not found"
        )));
    };

    // find any field that is a struct
    let mut possible_structs =
        struct_expr_vec
            .iter()
            .filter_map(|e| match e.to_field(schema).map(|f| f.dtype) {
                Ok(DataType::Struct(subfields)) => Some(subfields),
                _ => None,
            });
    let Some(subfields) = possible_structs.next() else {
        return Err(DaftError::ValueError(format!(
            "Error matching wildcard {pattern}: no column matching {struct_name} is a struct"
        )));
    };

    if possible_structs.next().is_some() {
        log::warn!(
            "Warning: Multiple matches found for col({pattern}), choosing left-associatively"
        );
    }

    Ok(subfields
        .into_iter()
        .map(|f| format!("{}.{}", struct_name, f.name))
        .collect())
}

fn replace_column_name(expr: ExprRef, old_name: &str, new_name: &str) -> DaftResult<ExprRef> {
    expr.transform(|e| match e.as_ref() {
        Expr::Column(name) if name.as_ref() == old_name => Ok(Transformed::yes(col(new_name))),
        _ => Ok(Transformed::no(e)),
    })
    .data()
}

// Duplicate an expression tree for each wildcard match.
fn expand_wildcards(
    expr: ExprRef,
    schema: &Schema,
    struct_expr_map: &HashMap<String, Vec<ExprRef>>,
) -> DaftResult<Vec<ExprRef>> {
    let wildcards = find_wildcards(expr.clone(), struct_expr_map);
    match wildcards.as_slice() {
        [] => Ok(vec![expr]),
        [pattern] => {
            get_wildcard_matches(pattern, schema, struct_expr_map)?
                .into_iter()
                .map(|s| replace_column_name(expr.clone(), pattern, &s))
                .collect()
        }
        _ => Err(DaftError::ValueError(format!(
            "Error resolving expression {expr}: cannot have multiple wildcard columns in one expression tree (found {wildcards:?})")))
    }
}

fn extract_agg_expr(expr: &Expr) -> DaftResult<AggExpr> {
    match expr {
        Expr::Agg(agg_expr) => Ok(agg_expr.clone()),
        Expr::Function { func, inputs } => Ok(AggExpr::MapGroups {
            func: func.clone(),
            inputs: inputs.clone(),
        }),
        Expr::Alias(e, name) => extract_agg_expr(e).map(|agg_expr| {
            // reorder expressions so that alias goes before agg
            match agg_expr {
                AggExpr::Count(e, count_mode) => {
                    AggExpr::Count(Expr::Alias(e, name.clone()).into(), count_mode)
                }
                AggExpr::Sum(e) => AggExpr::Sum(Expr::Alias(e, name.clone()).into()),
                AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: e,
                    percentiles,
                    force_list_output,
                }) => AggExpr::ApproxPercentile(ApproxPercentileParams {
                    child: Expr::Alias(e, name.clone()).into(),
                    percentiles,
                    force_list_output,
                }),
                AggExpr::ApproxCountDistinct(e) => {
                    AggExpr::ApproxCountDistinct(Expr::Alias(e, name.clone()).into())
                }
                AggExpr::ApproxSketch(e, sketch_type) => {
                    AggExpr::ApproxSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::MergeSketch(e, sketch_type) => {
                    AggExpr::MergeSketch(Expr::Alias(e, name.clone()).into(), sketch_type)
                }
                AggExpr::Mean(e) => AggExpr::Mean(Expr::Alias(e, name.clone()).into()),
                AggExpr::Stddev(e) => AggExpr::Stddev(Expr::Alias(e, name.clone()).into()),
                AggExpr::Min(e) => AggExpr::Min(Expr::Alias(e, name.clone()).into()),
                AggExpr::Max(e) => AggExpr::Max(Expr::Alias(e, name.clone()).into()),
                AggExpr::AnyValue(e, ignore_nulls) => {
                    AggExpr::AnyValue(Expr::Alias(e, name.clone()).into(), ignore_nulls)
                }
                AggExpr::List(e) => AggExpr::List(Expr::Alias(e, name.clone()).into()),
                AggExpr::Concat(e) => AggExpr::Concat(Expr::Alias(e, name.clone()).into()),
                AggExpr::MapGroups { func, inputs } => AggExpr::MapGroups {
                    func,
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.alias(name.clone()))
                        .collect(),
                },
            }
        }),
        // TODO(Kevin): Support a mix of aggregation and non-aggregation expressions
        // as long as the final value always has a cardinality of 1.
        _ => Err(DaftError::ValueError(format!(
            "Expected aggregation expression, but got: {expr}"
        ))),
    }
}

/// Resolves and validates the expression with a schema, returning the new expression and its field.
/// Specifically, makes sure the expression does not contain aggregations or stateful UDFs when they are not allowed,
/// and resolves struct accessors and wildcards.
/// May return multiple expressions if the expr contains a wildcard.
///
/// TODO: Use a builder pattern for this functionality
fn resolve_expr(
    expr: ExprRef,
    schema: &Schema,
    allow_stateful_udf: bool,
) -> DaftResult<Vec<ExprRef>> {
    // TODO(Kevin): Support aggregation expressions everywhere
    if has_agg(&expr) {
        return Err(DaftError::ValueError(format!(
            "Aggregation expressions are currently only allowed in agg and pivot: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
        )));
    }

    if !allow_stateful_udf && has_stateful_udf(&expr) {
        return Err(DaftError::ValueError(format!(
            "Stateful UDFs are only allowed in projections: {expr}"
        )));
    }

    let struct_expr_map = calculate_struct_expr_map(schema);
    expand_wildcards(expr, schema, &struct_expr_map)?
        .into_iter()
        .map(|e| transform_struct_gets(e, &struct_expr_map))
        .collect()
}

// Resolve a single expression, erroring if any kind of expansion happens.
pub fn resolve_single_expr(
    expr: ExprRef,
    schema: &Schema,
    allow_stateful_udf: bool,
) -> DaftResult<(ExprRef, Field)> {
    let resolved_exprs = resolve_expr(expr.clone(), schema, allow_stateful_udf)?;
    match resolved_exprs.as_slice() {
        [resolved_expr] => Ok((resolved_expr.clone(), resolved_expr.to_field(schema)?)),
        _ => Err(DaftError::ValueError(format!(
            "Error resolving expression {}: expanded into {} expressions when 1 was expected",
            expr,
            resolved_exprs.len()
        ))),
    }
}

pub fn resolve_exprs(
    exprs: Vec<ExprRef>,
    schema: &Schema,
    allow_stateful_udf: bool,
) -> DaftResult<(Vec<ExprRef>, Vec<Field>)> {
    // can't flat map because we need to deal with errors
    let resolved_exprs: DaftResult<Vec<Vec<ExprRef>>> = exprs
        .into_iter()
        .map(|e| resolve_expr(e, schema, allow_stateful_udf))
        .collect();
    let resolved_exprs: Vec<ExprRef> = resolved_exprs?.into_iter().flatten().collect();
    let resolved_fields: DaftResult<Vec<Field>> =
        resolved_exprs.iter().map(|e| e.to_field(schema)).collect();
    Ok((resolved_exprs, resolved_fields?))
}

/// Resolves and validates the expression with a schema, returning the extracted aggregation expression and its field.
/// Specifically, makes sure the expression does not contain aggregationsnested  or stateful UDFs,
/// and resolves struct accessors and wildcards.
/// May return multiple expressions if the expr contains a wildcard.
///
/// TODO: Use a builder pattern for this functionality
fn resolve_aggexpr(expr: ExprRef, schema: &Schema) -> DaftResult<Vec<AggExpr>> {
    let has_nested_agg = extract_agg_expr(&expr)?.children().iter().any(has_agg);

    if has_nested_agg {
        return Err(DaftError::ValueError(format!(
            "Nested aggregation expressions are not supported: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383"
        )));
    }

    if has_stateful_udf(&expr) {
        return Err(DaftError::ValueError(format!(
            "Stateful UDFs are only allowed in projections: {expr}"
        )));
    }

    let struct_expr_map = calculate_struct_expr_map(schema);
    expand_wildcards(expr, schema, &struct_expr_map)?
        .into_iter()
        .map(|expr| {
            let agg_expr = extract_agg_expr(&expr)?;

            let resolved_children = agg_expr
                .children()
                .into_iter()
                .map(|e| transform_struct_gets(e, &struct_expr_map))
                .collect::<DaftResult<Vec<_>>>()?;
            Ok(agg_expr.with_new_children(resolved_children))
        })
        .collect()
}

pub fn resolve_single_aggexpr(expr: ExprRef, schema: &Schema) -> DaftResult<(AggExpr, Field)> {
    let resolved_exprs = resolve_aggexpr(expr.clone(), schema)?;
    match resolved_exprs.as_slice() {
        [resolved_expr] => Ok((resolved_expr.clone(), resolved_expr.to_field(schema)?)),
        _ => Err(DaftError::ValueError(format!(
            "Error resolving expression {}: expanded into {} expressions when 1 was expected",
            expr,
            resolved_exprs.len()
        ))),
    }
}

pub fn resolve_aggexprs(
    exprs: Vec<ExprRef>,
    schema: &Schema,
) -> DaftResult<(Vec<AggExpr>, Vec<Field>)> {
    // can't flat map because we need to deal with errors
    let resolved_exprs: DaftResult<Vec<Vec<AggExpr>>> = exprs
        .into_iter()
        .map(|e| resolve_aggexpr(e, schema))
        .collect();
    let resolved_exprs: Vec<AggExpr> = resolved_exprs?.into_iter().flatten().collect();
    let resolved_fields: DaftResult<Vec<Field>> =
        resolved_exprs.iter().map(|e| e.to_field(schema)).collect();
    Ok((resolved_exprs, resolved_fields?))
}

pub fn check_column_name_validity(name: &str, schema: &Schema) -> DaftResult<()> {
    let struct_expr_map = calculate_struct_expr_map(schema);

    let names = if name == "*" || name.ends_with(".*") {
        if let Ok(names) = get_wildcard_matches(name, schema, &struct_expr_map) {
            names
        } else {
            return Err(DaftError::ValueError(format!(
                "Error matching wildcard `{name}` in schema: {schema}"
            )));
        }
    } else {
        vec![name.into()]
    };

    for n in names {
        if !struct_expr_map.contains_key(&n) {
            return Err(DaftError::ValueError(format!(
                "Column `{n}` not found in schema: {schema}"
            )));
        }
    }

    Ok(())
}
