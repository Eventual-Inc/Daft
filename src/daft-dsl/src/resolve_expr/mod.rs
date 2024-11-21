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
use typed_builder::TypedBuilder;

use crate::{
    col, expr::has_agg, functions::FunctionExpr, has_stateful_udf, AggExpr, Expr, ExprRef,
};

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

/// Checks if an expression used in an aggregation is well formed.
/// Expressions for aggregations must be in the form (optional) non-agg expr <- agg exprs or literals <- non-agg exprs
///
/// # Examples
///
/// Allowed:
/// - lit("x")
/// - sum(col("a"))
/// - sum(col("a")) > 0
/// - sum(col("a")) - sum(col("b")) > sum(col("c"))
///
/// Not allowed:
/// - col("a")
///     - not an aggregation
/// - sum(col("a")) + col("b")
///     - not all branches are aggregations
fn has_single_agg_layer(expr: &ExprRef) -> bool {
    match expr.as_ref() {
        Expr::Agg(agg_expr) => !agg_expr.children().iter().any(has_agg),
        Expr::Column(_) => false,
        Expr::Literal(_) => true,
        _ => expr.children().iter().all(has_single_agg_layer),
    }
}

fn convert_udfs_to_map_groups(expr: &ExprRef) -> ExprRef {
    expr.clone()
        .transform(|e| match e.as_ref() {
            Expr::Function { func, inputs } if matches!(func, FunctionExpr::Python(_)) => {
                Ok(Transformed::yes(Arc::new(Expr::Agg(AggExpr::MapGroups {
                    func: func.clone(),
                    inputs: inputs.clone(),
                }))))
            }
            _ => Ok(Transformed::no(e)),
        })
        .unwrap()
        .data
}

fn validate_expr(expr: ExprRef) -> DaftResult<ExprRef> {
    if has_agg(&expr) {
        return Err(DaftError::ValueError(format!(
            "Aggregation expressions are currently only allowed in agg and pivot: {expr}\nIf you would like to have this feature, please see https://github.com/Eventual-Inc/Daft/issues/1979#issue-2170913383",
        )));
    }

    Ok(expr)
}

fn validate_expr_in_agg(expr: ExprRef) -> DaftResult<ExprRef> {
    let converted_expr = convert_udfs_to_map_groups(&expr);

    if !has_single_agg_layer(&converted_expr) {
        return Err(DaftError::ValueError(format!(
            "Expressions in aggregations must be composed of non-nested aggregation expressions, got {expr}"
        )));
    }

    Ok(converted_expr)
}

/// Used for resolving and validating expressions.
/// Specifically, makes sure the expression does not contain aggregations or stateful UDFs
/// where they are not allowed, and resolves struct accessors and wildcards.
#[derive(Default, TypedBuilder)]
pub struct ExprResolver {
    #[builder(default)]
    allow_stateful_udf: bool,
    #[builder(default)]
    in_agg_context: bool,
}

impl ExprResolver {
    fn resolve_helper(&self, expr: ExprRef, schema: &Schema) -> DaftResult<Vec<ExprRef>> {
        if !self.allow_stateful_udf && has_stateful_udf(&expr) {
            return Err(DaftError::ValueError(format!(
                "Stateful UDFs are only allowed in projections: {expr}"
            )));
        }

        let validated_expr = if self.in_agg_context {
            validate_expr_in_agg(expr)
        } else {
            validate_expr(expr)
        }?;

        let struct_expr_map = calculate_struct_expr_map(schema);
        expand_wildcards(validated_expr, schema, &struct_expr_map)?
            .into_iter()
            .map(|e| transform_struct_gets(e, &struct_expr_map))
            .collect()
    }

    /// Resolve multiple expressions. Due to wildcards, output vec may contain more expressions than input.
    pub fn resolve(
        &self,
        exprs: Vec<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<(Vec<ExprRef>, Vec<Field>)> {
        // can't flat map because we need to deal with errors
        let resolved_exprs: DaftResult<Vec<Vec<ExprRef>>> = exprs
            .into_iter()
            .map(|e| self.resolve_helper(e, schema))
            .collect();
        let resolved_exprs: Vec<ExprRef> = resolved_exprs?.into_iter().flatten().collect();
        let resolved_fields: DaftResult<Vec<Field>> =
            resolved_exprs.iter().map(|e| e.to_field(schema)).collect();
        Ok((resolved_exprs, resolved_fields?))
    }

    /// Resolve a single expression, ensuring that the output is also a single expression.
    pub fn resolve_single(&self, expr: ExprRef, schema: &Schema) -> DaftResult<(ExprRef, Field)> {
        let resolved_exprs = self.resolve_helper(expr.clone(), schema)?;
        match resolved_exprs.as_slice() {
            [resolved_expr] => Ok((resolved_expr.clone(), resolved_expr.to_field(schema)?)),
            _ => Err(DaftError::ValueError(format!(
                "Error resolving expression {}: expanded into {} expressions when 1 was expected",
                expr,
                resolved_exprs.len()
            ))),
        }
    }
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
