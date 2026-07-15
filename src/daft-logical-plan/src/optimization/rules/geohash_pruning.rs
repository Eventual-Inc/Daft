use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_algebra::boolean::combine_conjunction;
use daft_dsl::{
    Expr,
    ExprRef,
    expr::{Column, ResolvedColumn},
};
use daft_geo::StGeohashCovers;

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimizer rule for automatic geohash-based partition pruning.
///
/// When a spatial predicate (`st_intersects`, `st_contains`, `st_within`, `st_distance`)
/// is applied on a geometry column `col`, and the schema also contains a string column
/// named `{col}_geohash`, this rule rewrites the filter to add a geohash pre-filter:
///
/// ```sql
/// -- Original
/// WHERE st_intersects(geom, @query)
///
/// -- Rewritten
/// WHERE st_geohash_covers(geom_geohash, @precision, @cells) AND st_intersects(geom, @query)
/// ```
///
/// The geohash pre-filter is much cheaper to evaluate because it operates on short strings
/// rather than parsing WKB bytes and doing spatial computations.
///
/// ## Convention
/// Add a `{column_name}_geohash` column to your table to enable this optimization:
///
/// ```python
/// df = df.with_column("geom_geohash", st_geohash(df["geom"], precision=5))
/// ```
#[derive(Debug, Default)]
pub struct GeohashPruning;

impl OptimizerRule for GeohashPruning {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| self.try_optimize_node(node))
    }
}

impl GeohashPruning {
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let filter = match plan.as_ref() {
            LogicalPlan::Filter(f) => f,
            _ => return Ok(Transformed::no(plan)),
        };

        let schema = filter.input.schema();
        let predicate = &filter.predicate;

        // Collect additional geohash predicates to AND in.
        let mut extra_preds: Vec<ExprRef> = vec![];
        collect_geohash_preds(predicate, &schema, &mut extra_preds);

        if extra_preds.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Build combined predicate: AND(geohash_preds..., original_predicate)
        let mut all_preds = extra_preds;
        all_preds.push(predicate.clone());
        let new_predicate = combine_conjunction(all_preds).unwrap();

        if new_predicate == *predicate {
            return Ok(Transformed::no(plan));
        }

        let new_filter = LogicalPlan::from(crate::ops::Filter::try_new(
            filter.input.clone(),
            new_predicate,
        )?)
        .into();
        Ok(Transformed::yes(new_filter))
    }
}

// ── Expression walking ──────────────────────────────────────────────────────

const SPATIAL_PRED_FNS: &[&str] = &["st_intersects", "st_contains", "st_within", "st_distance"];
const DEFAULT_GEOHASH_PRECISION: u8 = 5;

fn collect_geohash_preds(
    expr: &ExprRef,
    schema: &daft_schema::schema::Schema,
    out: &mut Vec<ExprRef>,
) {
    match expr.as_ref() {
        Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) => {
            if SPATIAL_PRED_FNS.contains(&sf.name()) {
                if let Some(pred) =
                    maybe_geohash_pred_for_spatial_fn(sf, schema)
                {
                    out.push(pred);
                }
            }
        }
        Expr::BinaryOp { op: daft_core::prelude::Operator::And, left, right } => {
            collect_geohash_preds(left, schema, out);
            collect_geohash_preds(right, schema, out);
        }
        _ => {}
    }
}

fn maybe_geohash_pred_for_spatial_fn(
    sf: &daft_dsl::functions::BuiltinScalarFn,
    schema: &daft_schema::schema::Schema,
) -> Option<ExprRef> {
    // First input must be a column reference
    let first_arg = sf.inputs.required(0).ok()?.clone();
    let col_name: Arc<str> = match first_arg.as_ref() {
        Expr::Alias(inner, _) => match inner.as_ref() {
            Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => name.clone(),
            _ => return None,
        },
        Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => name.clone(),
        _ => return None,
    };

    // Check if the schema has a `{col_name}_geohash` string column
    let geohash_col_name = format!("{col_name}_geohash");
    let geohash_field = schema.get_field(&geohash_col_name).ok()?;
    if !matches!(geohash_field.dtype, daft_schema::dtype::DataType::Utf8) {
        return None;
    }

    // For distance-based predicates, get the precision from an existing geohash column
    // For other spatial predicates, compute covering cells from literal second arg
    let second_arg = sf.inputs.required(1).ok()?;

    // Extract WKB literal bytes from the second argument
    let wkb_bytes: Vec<u8> = match second_arg.as_ref() {
        Expr::Literal(lit) => match lit {
            daft_core::lit::Literal::Binary(b) => b.clone(),
            _ => return None,
        },
        _ => return None,
    };

    // Compute covering cells from the query geometry's WKB
    let geom = daft_geo::st_geohash::geohash_covers_geometry(
        &daft_geo::utils::parse_wkb(&wkb_bytes).ok()?,
        DEFAULT_GEOHASH_PRECISION as usize,
    );
    if geom.is_empty() {
        return None;
    }

    let covering_cells = geom.join("\n");
    let geohash_expr = daft_dsl::resolved_col(geohash_col_name);
    let builtin = daft_dsl::functions::BuiltinScalarFn::new(
        StGeohashCovers {
            precision: DEFAULT_GEOHASH_PRECISION,
            covering_cells,
        },
        vec![geohash_expr],
    );
    Some(builtin.into())
}
