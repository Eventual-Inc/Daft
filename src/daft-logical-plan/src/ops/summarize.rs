use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{Expr, ExprRef, lit};
use daft_schema::dtype::DataType;

use crate::LogicalPlanBuilder;

/// Creates a DataFrame summary by aggregating column stats into lists then exploding.
pub fn summarize(input: &LogicalPlanBuilder) -> DaftResult<LogicalPlanBuilder> {
    // create the agg lists (avg is blocked on try_cast)
    let mut cols: Vec<ExprRef> = vec![]; // column             :: utf8
    let mut typs: Vec<ExprRef> = vec![]; // type               :: utf8
    let mut mins: Vec<ExprRef> = vec![]; // min                :: utf8
    let mut maxs: Vec<ExprRef> = vec![]; // max                :: utf8
    let mut cnts: Vec<ExprRef> = vec![]; // count              :: int64
    let mut nuls: Vec<ExprRef> = vec![]; // nulls              :: int64
    let mut unqs: Vec<ExprRef> = vec![]; // approx_distinct    :: int64
    for field in input.schema().as_ref() {
        let col = daft_dsl::resolved_col(field.name.as_str());
        cols.push(lit(field.name.clone()));
        typs.push(lit(field.dtype.to_string()));
        mins.push(col.clone().min().cast(&DataType::Utf8));
        maxs.push(col.clone().max().cast(&DataType::Utf8));
        cnts.push(col.clone().count(CountMode::Valid));
        nuls.push(col.clone().count(CountMode::Null));
        unqs.push(col.clone().approx_count_distinct());
    }
    // apply aggregations lists
    let input = input.aggregate(
        vec![
            list_(cols, "column"),
            list_(typs, "type"),
            list_(mins, "min"),
            list_(maxs, "max"),
            list_(cnts, "count"),
            list_(nuls, "count_nulls"),
            list_(unqs, "approx_count_distinct"),
        ],
        vec![],
    )?;
    // apply explode for all columns
    input.explode(input.columns())
}

/// Creates a list constructor for the given items.
fn list_(items: Vec<ExprRef>, alias: &str) -> ExprRef {
    Expr::List(items).arced().alias(alias)
}
