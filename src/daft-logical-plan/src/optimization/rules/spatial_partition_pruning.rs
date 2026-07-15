/// Partition-level spatial pruning using a per-directory H3 spatial index.
///
/// When a spatial predicate (`st_intersects`, `st_contains`, `st_within`) filters a
/// geometry column and a `_spatial_index.idx` sidecar exists in a source file's own
/// directory, this rule skips every scan task whose H3 coverage does not overlap the
/// query geometry.
///
/// # Index format — version 1 (H3 inverted Parquet index)
///
/// An inverted Parquet file with two dictionary-encoded string columns:
///
/// | h3_cell         | filename       |
/// |-----------------|----------------|
/// | 8a283473fffffff | part-0.parquet |
/// | 8a2834b3fffffff | part-0.parquet |
/// | 8a283477fffffff | part-1.parquet |
///
/// plus schema-level key-value metadata: `geom_col` (geometry column name) and
/// `h3_resolution` (decimal string). This matches what
/// `daft.functions.spatial_index.build_spatial_index()` writes. The rule loads each
/// directory's whole (small) sidecar into memory once, cached per directory.
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
const INDEX_FILENAME: &str = "_spatial_index.idx";

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

/// In-memory representation of a loaded `_spatial_index.idx` (H3, version 1).
struct LoadedIndex {
    geom_col: String,
    h3_resolution: u8,
    file_cells: HashMap<String, Vec<u64>>,
}

fn load_index_for_dir(dir: &str) -> Option<LoadedIndex> {
    use parquet::{
        file::reader::{FileReader, SerializedFileReader},
        record::RowAccessor,
    };

    let index_path = Path::new(dir).join(INDEX_FILENAME);
    let file = std::fs::File::open(index_path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;

    let kv_meta = reader.metadata().file_metadata().key_value_metadata()?;
    let geom_col = kv_meta
        .iter()
        .find(|kv| kv.key == "geom_col")
        .and_then(|kv| kv.value.clone())?;
    let h3_resolution: u8 = kv_meta
        .iter()
        .find(|kv| kv.key == "h3_resolution")
        .and_then(|kv| kv.value.clone())?
        .parse()
        .ok()?;

    // Column 0 = h3_cell, column 1 = filename (the schema `_build_h3_index` in
    // daft/functions/spatial_index.py writes). Dictionary encoding is a physical
    // storage detail — the row-based reader yields plain logical strings.
    let mut raw: HashMap<String, Vec<String>> = HashMap::new();
    let row_iter = reader.get_row_iter(None).ok()?;
    for row in row_iter {
        let row = row.ok()?;
        let h3_cell = row.get_string(0).ok()?.clone();
        let filename = row.get_string(1).ok()?.clone();
        raw.entry(filename).or_default().push(h3_cell);
    }

    // `parse_h3_cells` silently drops any string it can't parse. A shortfall
    // between the recorded cell strings and the successfully parsed cells
    // means this file's entry is CORRUPT, not merely sparse — and a
    // corrupt/partial cell list must never be used to prune, since
    // `h3_cells_intersect` over a shrunken set can wrongly report "no
    // overlap". Drop the file's entry entirely so it is ABSENT from
    // `file_cells`; the lookup in `task_passes_h3` already treats an absent
    // basename as "not in this directory's index" and keeps the task
    // conservatively. This is safer than dropping just the unparsed cells
    // and keeping the valid ones, which would silently shrink coverage and
    // could turn a real intersection into a false "no overlap".
    let file_cells: HashMap<String, Vec<u64>> = raw
        .into_iter()
        .filter_map(|(fname, cell_strs)| {
            let parsed = parse_h3_cells(&cell_strs);
            if parsed.len() == cell_strs.len() {
                Some((fname, parsed))
            } else {
                None
            }
        })
        .collect();
    Some(LoadedIndex {
        geom_col,
        h3_resolution,
        file_cells,
    })
}

/// One loaded index per directory, so a scan spanning many partition directories
/// reads each sidecar once, not once per task.
struct IndexCache {
    by_dir: HashMap<String, Option<Arc<LoadedIndex>>>,
}

impl IndexCache {
    fn new() -> Self {
        Self {
            by_dir: HashMap::new(),
        }
    }

    fn get(&mut self, dir: &str) -> Option<Arc<LoadedIndex>> {
        self.by_dir
            .entry(dir.to_string())
            .or_insert_with(|| load_index_for_dir(dir).map(Arc::new))
            .clone()
    }
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

        // Nothing to prune when there is only one scan task.
        if tasks.len() <= 1 {
            return Ok(Transformed::no(plan));
        }

        let mut cache = IndexCache::new();
        // Different directories may use different H3 resolutions; resolve the
        // query's covering cells once per resolution.
        let mut query_cells_by_res: HashMap<u8, Option<Vec<u64>>> = HashMap::new();

        let pruned: Vec<ScanTaskRef> = tasks
            .iter()
            .filter(|t| {
                task_passes_h3(
                    t,
                    &geom_col,
                    &query_wkb,
                    &mut cache,
                    &mut query_cells_by_res,
                )
            })
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

/// On Windows, strips the leading "/" from paths like "/C:/Users/..." to produce "C:/Users/...".
///
/// Mirrors `daft_io::strip_leading_slash_before_drive`; duplicated locally (instead of
/// depending on `daft-io`) since this is the only URI-handling this crate needs.
#[cfg(windows)]
fn strip_leading_slash_before_drive(path: &str) -> &str {
    let bytes = path.as_bytes();
    if bytes.len() >= 3 && bytes[0] == b'/' && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
        &path[1..]
    } else {
        path
    }
}

/// Strip a `file://` URI scheme prefix, as carried by local scan task paths
/// (e.g. `file:///tmp/data/part-0.parquet`), to obtain a real filesystem path
/// that `std::fs`/`Path` can resolve. `std::fs::File::open` cannot open a URI
/// directly, so without this, the index sidecar is never found for any local
/// scan and the rule silently falls back to conservative-keep for every task.
///
/// Paths without a `file://` prefix (already-bare local paths, or remote
/// object-store URIs such as `s3://`) pass through unchanged — the index
/// sidecar is only ever read via local filesystem I/O, so remote paths
/// correctly continue to miss the index and keep conservatively.
fn local_fs_path(path: &str) -> &str {
    match path.strip_prefix("file://") {
        Some(p) => {
            #[cfg(windows)]
            let p = strip_leading_slash_before_drive(p);
            p
        }
        None => path,
    }
}

/// Keep the task when any of its source files' H3 cells (from that file's OWN
/// directory's index) intersect the query cells. Every unresolvable situation
/// (no parent dir, no index, wrong geom_col, no query cells, file not indexed
/// — including a file whose recorded cell strings failed to parse, which
/// `load_index_for_dir` drops from the index entirely) keeps the task
/// conservatively — pruning must never be based on a different directory's
/// index.
fn task_passes_h3(
    task: &ScanTask,
    geom_col: &str,
    query_wkb: &[u8],
    cache: &mut IndexCache,
    query_cells_by_res: &mut HashMap<u8, Option<Vec<u64>>>,
) -> bool {
    if task.sources.is_empty() {
        // No sources to resolve against an index -> keep conservatively.
        return true;
    }
    for source in &task.sources {
        let path = local_fs_path(source.get_path());
        let Some(dir) = Path::new(path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
        else {
            return true;
        };
        let Some(index) = cache.get(&dir) else {
            return true;
        };
        if index.geom_col != geom_col {
            return true;
        }
        let query_cells = query_cells_by_res
            .entry(index.h3_resolution)
            .or_insert_with(|| {
                wkb_to_h3_cells(query_wkb, index.h3_resolution).map(|c| c.into_iter().collect())
            });
        let Some(query_cells) = query_cells.as_ref() else {
            return true;
        };
        let fname = Path::new(path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        match fname.and_then(|f| index.file_cells.get(&f)) {
            Some(cells) => {
                if h3_cells_intersect(cells, query_cells) {
                    return true;
                }
            }
            None => return true, // not in this directory's index → keep conservatively
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

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use super::*;

    /// Write a `.idx` file with the exact schema `_build_h3_index` in
    /// `daft/functions/spatial_index.py` produces: two dictionary-encoded string
    /// columns (`h3_cell`, `filename`) plus `geom_col`/`h3_resolution` metadata.
    fn write_test_idx_file(
        path: &std::path::Path,
        geom_col: &str,
        h3_resolution: u8,
        rows: &[(&str, &str)],
    ) {
        use arrow::{
            array::StringDictionaryBuilder,
            datatypes::{DataType, Field, Int32Type, Schema},
            record_batch::RecordBatch,
        };
        use parquet::{arrow::ArrowWriter, file::metadata::KeyValue};

        let mut h3_builder: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
        let mut fname_builder: StringDictionaryBuilder<Int32Type> = StringDictionaryBuilder::new();
        for (cell, fname) in rows {
            h3_builder.append_value(*cell);
            fname_builder.append_value(*fname);
        }
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("geom_col".to_string(), geom_col.to_string());
        metadata.insert("h3_resolution".to_string(), h3_resolution.to_string());
        let schema = StdArc::new(
            Schema::new(vec![
                Field::new(
                    "h3_cell",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new(
                    "filename",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
            ])
            .with_metadata(metadata),
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(h3_builder.finish()),
                StdArc::new(fname_builder.finish()),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        // `ArrowWriter` only round-trips schema metadata inside the opaque
        // "ARROW:schema" IPC blob; it does NOT surface it as top-level Parquet
        // key-value metadata the way pyarrow's `pq.write_table` does (verified
        // empirically: pyarrow copies `schema.metadata` entries into the raw
        // Parquet footer KV pairs in addition to "ARROW:schema"). Replicate that
        // here so this fixture matches what the real Python writer produces and
        // `load_index_for_dir`'s `key_value_metadata()` lookup finds them.
        writer
            .append_key_value_metadata(KeyValue::new("geom_col".to_string(), geom_col.to_string()));
        writer.append_key_value_metadata(KeyValue::new(
            "h3_resolution".to_string(),
            h3_resolution.to_string(),
        ));
        writer.close().unwrap();
    }

    #[test]
    fn task_passes_h3_keeps_task_with_empty_sources() {
        use daft_scan::{
            FileFormatConfig, ParquetSourceConfig, Pushdowns, ScanTask, SourceConfig,
            storage_config::StorageConfig,
        };
        use daft_schema::{field::Field, schema::Schema, time_unit::TimeUnit};

        // `ScanTask::new` asserts `!sources.is_empty()`, so an empty-sources
        // task can never arise from the public constructor. Build one via a
        // direct struct literal (every field is `pub`) purely to exercise the
        // defensive early-return in `task_passes_h3` directly.
        let task = ScanTask {
            sources: vec![],
            schema: StdArc::new(Schema::new(Vec::<Field>::new())),
            source_config: StdArc::new(SourceConfig::File(FileFormatConfig::Parquet(
                ParquetSourceConfig {
                    coerce_int96_timestamp_unit: TimeUnit::Seconds,
                    field_id_mapping: None,
                    row_groups: None,
                    chunk_size: None,
                    ignore_corrupt_files: false,
                    geometry: true,
                },
            ))),
            storage_config: StdArc::new(StorageConfig::new_internal(false, None)),
            pushdowns: Pushdowns::default(),
            size_bytes_on_disk: None,
            metadata: None,
            statistics: None,
            generated_fields: None,
        };

        let mut cache = IndexCache::new();
        let mut query_cells_by_res = HashMap::new();
        assert!(
            task_passes_h3(&task, "geom", &[], &mut cache, &mut query_cells_by_res),
            "a task with empty sources must be kept conservatively, not pruned"
        );
    }

    #[test]
    fn load_index_for_dir_parses_real_idx_parquet_format() {
        let dir = tempfile::tempdir().unwrap();
        write_test_idx_file(
            &dir.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[
                ("851fb467fffffff", "part-0.parquet"),
                ("851fb467fffffff", "part-1.parquet"),
            ],
        );
        let index = load_index_for_dir(&dir.path().to_string_lossy()).expect("index must parse");
        assert_eq!(index.geom_col, "geom");
        assert_eq!(index.h3_resolution, 5);
        assert!(index.file_cells.contains_key("part-0.parquet"));
        assert!(index.file_cells.contains_key("part-1.parquet"));
    }

    #[test]
    fn corrupt_cell_string_makes_file_entry_unresolvable_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_test_idx_file(
            &dir.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[
                // part-0.parquet's sidecar entry is CORRUPT: one valid H3 hex
                // cell plus one unparsable string, as would result from a
                // hand-edited or otherwise corrupted sidecar file.
                ("851fb467fffffff", "part-0.parquet"),
                ("not_a_cell", "part-0.parquet"),
                // part-1.parquet's entry is fully valid and must be unaffected
                // by the corruption in a different file's entry.
                ("85be0e37fffffff", "part-1.parquet"),
            ],
        );
        let index =
            load_index_for_dir(&dir.path().to_string_lossy()).expect("index must still load");

        // Design A: a file whose recorded cell strings partially fail to parse
        // must be OMITTED from `file_cells` entirely (unresolvable -> keep at
        // the `task_passes_h3` lookup site), never present with a
        // shrunken/partial cell list — a partial list would let
        // `h3_cells_intersect` silently under-report coverage and prune rows
        // that should have been kept.
        assert!(
            !index.file_cells.contains_key("part-0.parquet"),
            "a file with any unparsable recorded cell string must be absent \
             from file_cells, not present with a shrunken cell list"
        );
        // The directory's other, fully-valid file entry must still load
        // normally — corruption in one file's entry must not poison the rest
        // of the directory's index.
        assert_eq!(
            index.file_cells.get("part-1.parquet"),
            Some(&vec![u64::from(
                "85be0e37fffffff".parse::<h3o::CellIndex>().unwrap()
            )])
        );
    }

    #[test]
    fn index_cache_isolates_directories() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        write_test_idx_file(
            &dir_a.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[("85be0e37fffffff", "part-0.parquet")],
        );
        write_test_idx_file(
            &dir_b.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[("851fb467fffffff", "part-0.parquet")],
        );
        let mut cache = IndexCache::new();
        let a = cache.get(&dir_a.path().to_string_lossy()).unwrap();
        let b = cache.get(&dir_b.path().to_string_lossy()).unwrap();
        // Same basename, different directories, different cells: per-directory
        // isolation, never a shared global basename map.
        assert_ne!(
            a.file_cells.get("part-0.parquet"),
            b.file_cells.get("part-0.parquet")
        );
        // Missing directory -> None (conservative keep at the call site).
        let dir_c = tempfile::tempdir().unwrap();
        assert!(cache.get(&dir_c.path().to_string_lossy()).is_none());
    }

    #[test]
    fn end_to_end_prunes_task_whose_own_directory_index_does_not_cover_query() {
        use daft_scan::{
            FileFormatConfig, ParquetSourceConfig, PhysicalScanInfo, Pushdowns, ScanOperatorRef,
            ScanSource, ScanSourceKind, ScanTask, SourceConfig, storage_config::StorageConfig,
            test_utils::DummyScanOperator,
        };
        use daft_schema::{
            dtype::DataType as DaftDataType, field::Field, schema::Schema, time_unit::TimeUnit,
        };

        fn make_task(path: &str, schema: daft_schema::schema::SchemaRef) -> ScanTaskRef {
            StdArc::new(ScanTask::new(
                vec![ScanSource {
                    size_bytes: None,
                    metadata: None,
                    statistics: None,
                    partition_spec: None,
                    kind: ScanSourceKind::File {
                        path: path.to_string(),
                        chunk_spec: None,
                        iceberg_delete_files: None,
                        parquet_metadata: None,
                    },
                }],
                StdArc::new(SourceConfig::File(FileFormatConfig::Parquet(
                    ParquetSourceConfig {
                        coerce_int96_timestamp_unit: TimeUnit::Seconds,
                        field_id_mapping: None,
                        row_groups: None,
                        chunk_size: None,
                        ignore_corrupt_files: false,
                        geometry: true,
                    },
                ))),
                schema,
                StdArc::new(StorageConfig::new_internal(false, None)),
                Pushdowns::default(),
                None,
            ))
        }

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        // Compute the TRUE H3 cell for the query point (Paris) at resolution 5,
        // rather than trusting a hardcoded string — H3 cell IDs are sensitive to
        // exact coordinates/resolution and a wrong guess would make this test
        // pass or fail for the wrong reason.
        let query_point = geo::Geometry::Point(geo::Point::new(2.35, 48.85));
        let query_wkb = daft_geo::utils::geom_to_wkb(&query_point).unwrap();
        let query_cells =
            wkb_to_h3_cells(&query_wkb, 5).expect("query point must resolve to H3 cells");
        let covering_cell_str = h3o::CellIndex::try_from(*query_cells.iter().next().unwrap())
            .unwrap()
            .to_string();

        // A different point (Sydney), far enough away that its resolution-5 cell
        // set is guaranteed disjoint from Paris's.
        let other_point = geo::Geometry::Point(geo::Point::new(151.2, -33.87));
        let other_wkb = daft_geo::utils::geom_to_wkb(&other_point).unwrap();
        let other_cells =
            wkb_to_h3_cells(&other_wkb, 5).expect("other point must resolve to H3 cells");
        let non_covering_cell_str = h3o::CellIndex::try_from(*other_cells.iter().next().unwrap())
            .unwrap()
            .to_string();
        assert_ne!(covering_cell_str, non_covering_cell_str);

        // dir_a covers the non-covering (Sydney) cell; dir_b covers the true
        // Paris query cell.
        write_test_idx_file(
            &dir_a.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[(non_covering_cell_str.as_str(), "part-0.parquet")],
        );
        write_test_idx_file(
            &dir_b.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[(covering_cell_str.as_str(), "part-0.parquet")],
        );

        let schema = StdArc::new(Schema::new(vec![Field::new("geom", DaftDataType::Binary)]));
        let task_a = make_task(
            &dir_a.path().join("part-0.parquet").to_string_lossy(),
            schema.clone(),
        );
        let task_b = make_task(
            &dir_b.path().join("part-0.parquet").to_string_lossy(),
            schema.clone(),
        );

        let scan_op = StdArc::new(DummyScanOperator {
            schema: schema.clone(),
            ..Default::default()
        });
        let mut psi = PhysicalScanInfo::new(
            ScanOperatorRef(scan_op),
            schema.clone(),
            vec![],
            Pushdowns::default(),
            None,
        );
        psi.scan_state = ScanState::Tasks(StdArc::new(vec![task_a, task_b]));
        let source =
            crate::ops::Source::new(schema.clone(), StdArc::new(SourceInfo::Physical(psi)));

        let predicate = daft_geo::st_intersects::st_intersects(
            daft_dsl::resolved_col("geom"),
            daft_dsl::lit(query_wkb.as_slice()),
        );
        let filter =
            crate::ops::Filter::try_new(StdArc::new(LogicalPlan::Source(source)), predicate)
                .unwrap();
        let plan = StdArc::new(LogicalPlan::Filter(filter));

        let result = SpatialPartitionPruning.try_optimize(plan).unwrap();
        assert!(result.transformed, "rule must prune the non-covering task");
        let LogicalPlan::Filter(f) = result.data.as_ref() else {
            panic!("expected Filter")
        };
        let LogicalPlan::Source(s) = f.input.as_ref() else {
            panic!("expected Source")
        };
        let SourceInfo::Physical(p) = s.source_info.as_ref() else {
            panic!()
        };
        let ScanState::Tasks(remaining) = &p.scan_state else {
            panic!()
        };
        assert_eq!(remaining.len(), 1);
        assert!(
            remaining[0].sources[0]
                .get_path()
                .starts_with(&*dir_b.path().to_string_lossy())
        );
    }

    #[test]
    fn end_to_end_keeps_task_when_own_directory_index_has_corrupt_cell_string() {
        use daft_scan::{
            FileFormatConfig, ParquetSourceConfig, PhysicalScanInfo, Pushdowns, ScanOperatorRef,
            ScanSource, ScanSourceKind, ScanTask, SourceConfig, storage_config::StorageConfig,
            test_utils::DummyScanOperator,
        };
        use daft_schema::{
            dtype::DataType as DaftDataType, field::Field, schema::Schema, time_unit::TimeUnit,
        };

        fn make_task(path: &str, schema: daft_schema::schema::SchemaRef) -> ScanTaskRef {
            StdArc::new(ScanTask::new(
                vec![ScanSource {
                    size_bytes: None,
                    metadata: None,
                    statistics: None,
                    partition_spec: None,
                    kind: ScanSourceKind::File {
                        path: path.to_string(),
                        chunk_spec: None,
                        iceberg_delete_files: None,
                        parquet_metadata: None,
                    },
                }],
                StdArc::new(SourceConfig::File(FileFormatConfig::Parquet(
                    ParquetSourceConfig {
                        coerce_int96_timestamp_unit: TimeUnit::Seconds,
                        field_id_mapping: None,
                        row_groups: None,
                        chunk_size: None,
                        ignore_corrupt_files: false,
                        geometry: true,
                    },
                ))),
                schema,
                StdArc::new(StorageConfig::new_internal(false, None)),
                Pushdowns::default(),
                None,
            ))
        }

        // dir_a's part-0.parquet entry is CORRUPT (one valid non-covering cell
        // + one unparsable string) and must therefore be UNRESOLVABLE, not
        // "confirmed non-covering" — its task must be KEPT even though the one
        // cell that *did* parse is the same non-covering (Sydney) cell used in
        // dir_b, whose task (clean, fully-valid, non-covering) IS pruned. This
        // proves the difference in outcome is caused by the corruption, not by
        // some other confound.
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        let query_point = geo::Geometry::Point(geo::Point::new(2.35, 48.85)); // Paris
        let query_wkb = daft_geo::utils::geom_to_wkb(&query_point).unwrap();

        let other_point = geo::Geometry::Point(geo::Point::new(151.2, -33.87)); // Sydney
        let other_wkb = daft_geo::utils::geom_to_wkb(&other_point).unwrap();
        let other_cells =
            wkb_to_h3_cells(&other_wkb, 5).expect("other point must resolve to H3 cells");
        let non_covering_cell_str = h3o::CellIndex::try_from(*other_cells.iter().next().unwrap())
            .unwrap()
            .to_string();

        write_test_idx_file(
            &dir_a.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[
                (non_covering_cell_str.as_str(), "part-0.parquet"),
                ("not_a_cell", "part-0.parquet"),
            ],
        );
        write_test_idx_file(
            &dir_b.path().join(INDEX_FILENAME),
            "geom",
            5,
            &[(non_covering_cell_str.as_str(), "part-0.parquet")],
        );

        let schema = StdArc::new(Schema::new(vec![Field::new("geom", DaftDataType::Binary)]));
        let task_a = make_task(
            &dir_a.path().join("part-0.parquet").to_string_lossy(),
            schema.clone(),
        );
        let task_b = make_task(
            &dir_b.path().join("part-0.parquet").to_string_lossy(),
            schema.clone(),
        );

        let scan_op = StdArc::new(DummyScanOperator {
            schema: schema.clone(),
            ..Default::default()
        });
        let mut psi = PhysicalScanInfo::new(
            ScanOperatorRef(scan_op),
            schema.clone(),
            vec![],
            Pushdowns::default(),
            None,
        );
        psi.scan_state = ScanState::Tasks(StdArc::new(vec![task_a, task_b]));
        let source =
            crate::ops::Source::new(schema.clone(), StdArc::new(SourceInfo::Physical(psi)));

        let predicate = daft_geo::st_intersects::st_intersects(
            daft_dsl::resolved_col("geom"),
            daft_dsl::lit(query_wkb.as_slice()),
        );
        let filter =
            crate::ops::Filter::try_new(StdArc::new(LogicalPlan::Source(source)), predicate)
                .unwrap();
        let plan = StdArc::new(LogicalPlan::Filter(filter));

        let result = SpatialPartitionPruning.try_optimize(plan).unwrap();
        assert!(
            result.transformed,
            "rule must still prune dir_b's clean, confirmed non-covering task"
        );
        let LogicalPlan::Filter(f) = result.data.as_ref() else {
            panic!("expected Filter")
        };
        let LogicalPlan::Source(s) = f.input.as_ref() else {
            panic!("expected Source")
        };
        let SourceInfo::Physical(p) = s.source_info.as_ref() else {
            panic!()
        };
        let ScanState::Tasks(remaining) = &p.scan_state else {
            panic!()
        };
        assert_eq!(
            remaining.len(),
            1,
            "only dir_a's task (corrupt index entry -> unresolvable -> keep) must remain"
        );
        assert!(
            remaining[0].sources[0]
                .get_path()
                .starts_with(&*dir_a.path().to_string_lossy()),
            "dir_a's task must be KEPT despite its one parseable cell being \
             non-covering, because the corrupt entry makes it unresolvable"
        );
    }
}
