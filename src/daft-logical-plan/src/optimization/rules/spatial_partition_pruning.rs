/// Partition-level spatial pruning using a per-directory H3 spatial index.
///
/// When a spatial predicate (`st_intersects`, `st_contains`, `st_within`) filters a
/// geometry column and a `_spatial_index.json` sidecar exists in the same directory
/// as the parquet files, this rule skips every partition whose H3 coverage does not
/// overlap the query geometry.
///
/// # Index format — version 1 (H3 hex index)
///
/// ```json
/// {
///   "version": 1,
///   "geom_col": "geom",
///   "h3_resolution": 10,
///   "files": {
///     "part-0.parquet": ["8a283473fffffff", "8a2834b3fffffff"],
///     "part-1.parquet": ["8a283477fffffff"]
///   }
/// }
/// ```
///
/// Build the index from Python:
///
/// ```python
/// from daft.functions.spatial_index import build_spatial_index
/// build_spatial_index("output/", geom_col="geom")   # requires h3
/// ```
use std::{collections::HashMap, path::Path, sync::Arc};

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::lit::Literal;
use daft_dsl::{
    Expr, ExprRef,
    expr::{Column, ResolvedColumn},
};
use daft_geo::h3_index::{h3_cells_intersect, parse_h3_cells, wkb_to_h3_cells};
use daft_scan::{ScanState, ScanTask, ScanTaskRef};

use super::OptimizerRule;
use crate::{LogicalPlan, source_info::SourceInfo};

const SPATIAL_FNS: &[&str] = &["st_intersects", "st_contains", "st_within"];
const INDEX_FILENAME: &str = "_spatial_index.json";

/// Optimizer rule: skip scan tasks whose spatial coverage does not intersect
/// the query geometry's bounding area.
#[derive(Debug, Default)]
pub struct SpatialPartitionPruning;

impl OptimizerRule for SpatialPartitionPruning {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| self.try_optimize_node(node))
    }
}

// ── Index loading ─────────────────────────────────────────────────────────

/// In-memory representation of a loaded `_spatial_index.json` (H3, version 1).
struct LoadedIndex {
    geom_col: String,
    h3_resolution: u8,
    file_cells: HashMap<String, Vec<u64>>,
}

fn load_index_for_dir(dir: &str) -> Option<LoadedIndex> {
    let index_path = Path::new(dir).join(INDEX_FILENAME);
    let contents = std::fs::read_to_string(index_path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;

    if value["version"].as_u64()? != 1 {
        return None;
    }
    let geom_col = value["geom_col"].as_str()?.to_string();
    let h3_resolution = value["h3_resolution"].as_u64()? as u8;
    let files_map = value["files"].as_object()?;

    let mut file_cells: HashMap<String, Vec<u64>> = HashMap::new();
    for (fname, cells_val) in files_map {
        if let Ok(cell_strs) = serde_json::from_value::<Vec<String>>(cells_val.clone()) {
            file_cells.insert(fname.clone(), parse_h3_cells(&cell_strs));
        }
    }
    Some(LoadedIndex { geom_col, h3_resolution, file_cells })
}

// ── Rule implementation ───────────────────────────────────────────────────

impl SpatialPartitionPruning {
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        // Match: Filter → Source(ScanState::Tasks)
        let filter = match plan.as_ref() {
            LogicalPlan::Filter(f) => f,
            _ => return Ok(Transformed::no(plan)),
        };

        let (geom_col, query_wkb) = match extract_spatial_pred(&filter.predicate) {
            Some(v) => v,
            None => return Ok(Transformed::no(plan)),
        };

        let source = match filter.input.as_ref() {
            LogicalPlan::Source(s) => s,
            _ => return Ok(Transformed::no(plan)),
        };
        let scan_info = match source.source_info.as_ref() {
            SourceInfo::Physical(p) => p,
            _ => return Ok(Transformed::no(plan)),
        };
        let tasks = match &scan_info.scan_state {
            ScanState::Tasks(t) => t,
            _ => return Ok(Transformed::no(plan)),
        };

        let dir = match first_file_dir(tasks.as_ref()) {
            Some(d) => d,
            None => return Ok(Transformed::no(plan)),
        };
        let index = match load_index_for_dir(&dir) {
            Some(idx) => idx,
            None => return Ok(Transformed::no(plan)),
        };

        // Prune tasks using the H3 index.
        if index.geom_col != geom_col {
            return Ok(Transformed::no(plan));
        }
        let query_cells = match wkb_to_h3_cells(&query_wkb, index.h3_resolution) {
            Some(c) => c.into_iter().collect::<Vec<u64>>(),
            None => return Ok(Transformed::no(plan)),
        };
        let pruned: Vec<ScanTaskRef> = tasks
            .iter()
            .filter(|t| task_passes_h3(t, &index.file_cells, &query_cells))
            .cloned()
            .collect();

        let skipped = tasks.len() - pruned.len();
        if skipped == 0 {
            return Ok(Transformed::no(plan));
        }

        let mut new_scan_info = scan_info.clone();
        new_scan_info.scan_state = ScanState::Tasks(Arc::new(pruned));
        let new_source = source
            .clone()
            .with_source_info(Arc::new(SourceInfo::Physical(new_scan_info)));
        let new_filter = crate::ops::Filter::try_new(
            Arc::new(LogicalPlan::Source(new_source)),
            filter.predicate.clone(),
        )?;
        Ok(Transformed::yes(Arc::new(LogicalPlan::Filter(new_filter))))
    }
}

// ── Per-task filter helpers ───────────────────────────────────────────────

/// Keep the task when at least one source file's H3 cell set shares a
/// cell with the query geometry's covering cells.
fn task_passes_h3(
    task: &ScanTask,
    file_cells: &HashMap<String, Vec<u64>>,
    query_cells: &[u64],
) -> bool {
    for source in &task.sources {
        let fname = Path::new(source.get_path())
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        match fname.and_then(|f| file_cells.get(&f)) {
            Some(cells) => {
                if h3_cells_intersect(cells, query_cells) {
                    return true;
                }
            }
            None => return true, // not in index → keep conservatively
        }
    }
    false
}

// ── Predicate extraction ──────────────────────────────────────────────────

fn extract_spatial_pred(expr: &ExprRef) -> Option<(String, Vec<u8>)> {
    let sf = match expr.as_ref() {
        Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) => sf,
        Expr::BinaryOp {
            op: daft_core::prelude::Operator::And,
            left,
            right,
        } => {
            return extract_spatial_pred(left).or_else(|| extract_spatial_pred(right));
        }
        _ => return None,
    };

    if !SPATIAL_FNS.contains(&sf.name()) {
        return None;
    }

    let col_name = match sf.inputs.required(0).ok()?.as_ref() {
        Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => name.to_string(),
        Expr::Alias(inner, _) => match inner.as_ref() {
            Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => name.to_string(),
            _ => return None,
        },
        _ => return None,
    };

    let wkb = match sf.inputs.required(1).ok()?.as_ref() {
        Expr::Literal(Literal::Binary(b)) => b.clone(),
        _ => return None,
    };

    Some((col_name, wkb))
}

fn first_file_dir(tasks: &[ScanTaskRef]) -> Option<String> {
    for task in tasks {
        for source in &task.sources {
            let path = source.get_path();
            if let Some(parent) = Path::new(path).parent() {
                return Some(parent.to_string_lossy().into_owned());
            }
        }
    }
    None
}

