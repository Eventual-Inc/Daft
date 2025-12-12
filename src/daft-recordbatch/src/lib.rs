#![feature(iterator_try_collect)]

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter, Result},
    hash::{Hash, Hasher},
    io::Cursor,
    sync::Arc,
};

use common_display::table_display::{StrValue, make_comfy_table};
use common_error::{DaftError, DaftResult};
use common_runtime::get_compute_runtime;
use daft_arrow::{array::Array, chunk::Chunk};
use daft_core::{
    array::ops::{
        DaftApproxCountDistinctAggable, DaftHllSketchAggable, GroupIndices, full::FullNull,
    },
    prelude::*,
};
use daft_dsl::{
    AggExpr, ApproxPercentileParams, Column, Expr, ExprRef, SketchType,
    expr::{
        BoundColumn,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
    functions::{
        BuiltinScalarFn, BuiltinScalarFnVariant, FunctionArg, FunctionArgs, FunctionEvaluator,
        scalar::ScalarFn,
    },
    null_lit,
    operator_metrics::{MetricsCollector, NoopMetricsCollector},
    resolved_col,
};
use daft_functions_list::SeriesListExtension;
use file_info::FileInfos;
use futures::{StreamExt, TryStreamExt, future::try_join_all};
use num_traits::ToPrimitive;
#[cfg(feature = "python")]
pub mod ffi;
mod file_info;
mod growable;
mod ops;
mod preview;
mod probeable;
mod repr_html;

pub use growable::GrowableRecordBatch;
pub use ops::{get_column_by_name, get_columns_by_name};
pub use probeable::{ProbeState, Probeable, ProbeableBuilder, make_probeable_builder};

#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub use python::register_modules;
use rand::seq::index::sample;
pub use repr_html::html_value;

#[macro_export]
macro_rules! value_err {
    ($($arg:tt)*) => {
        return Err(common_error::DaftError::ValueError(format!($($arg)*)))
    };
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordBatch {
    pub schema: SchemaRef,
    columns: Arc<Vec<Series>>,
    num_rows: usize,
}

impl Hash for RecordBatch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        for col in &*self.columns {
            let hashes = col.hash(None).expect("Failed to hash column");
            hashes.into_iter().for_each(|h| h.hash(state));
        }
        self.num_rows.hash(state);
    }
}

#[inline]
fn validate_schema(schema: &Schema, columns: &[Series]) -> DaftResult<()> {
    if schema.len() != columns.len() {
        return Err(DaftError::SchemaMismatch(format!(
            "While building a RecordBatch, we found that the number of fields did not match between the schema and the input columns.\n {:?}\n vs\n {:?}",
            schema.len(),
            columns.len()
        )));
    }
    for (field, series) in schema.into_iter().zip(columns.iter()) {
        if field != series.field() {
            return Err(DaftError::SchemaMismatch(format!(
                "While building a RecordBatch, we found that the Schema Field and the Series Field  did not match. schema field: {field} vs series field: {}",
                series.field()
            )));
        }
    }
    Ok(())
}

impl RecordBatch {
    /// Create a new [`RecordBatch`] and handle broadcasting of any unit-length columns
    ///
    /// Note that this function is slow. You might instead be looking for [`RecordBatch::new_with_size`] which does not perform broadcasting
    /// or [`RecordBatch::new_unchecked`] if you've already performed your own validation logic.
    ///
    /// # Arguments
    ///
    /// * `schema` - Expected [`Schema`] of the new [`RecordBatch`], used for validation
    /// * `columns` - Columns to crate a table from as [`Series`] objects
    /// * `num_rows` - Expected number of rows in the [`RecordBatch`], passed explicitly to handle cases where `columns` is empty
    pub fn new_with_broadcast<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> DaftResult<Self> {
        let schema: SchemaRef = schema.into();
        validate_schema(schema.as_ref(), columns.as_slice())?;

        // Validate Series lengths against provided num_rows
        for (field, series) in schema.into_iter().zip(columns.iter()) {
            if (series.len() != 1) && (series.len() != num_rows) {
                return Err(DaftError::ValueError(format!(
                    "While building a RecordBatch with RecordBatch::new_with_broadcast, we found that the Series lengths did not match and could not be broadcasted. Series named: {} had length: {} vs the specified RecordBatch length: {}",
                    field.name,
                    series.len(),
                    num_rows
                )));
            }
        }

        // Broadcast any unit-length Series
        let columns: DaftResult<Vec<Series>> = columns
            .into_iter()
            .map(|s| {
                if s.len() == num_rows {
                    Ok(s)
                } else {
                    s.broadcast(num_rows)
                }
            })
            .collect();

        Ok(Self::new_unchecked(schema, columns?, num_rows))
    }

    #[deprecated(note = "arrow2 migration")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn get_inner_arrow_arrays(
        &self,
    ) -> impl Iterator<Item = Box<dyn daft_arrow::array::Array>> + '_ {
        self.columns.iter().map(|s| s.to_arrow2())
    }

    /// Create a new [`RecordBatch`] and validate against `num_rows`
    ///
    /// Note that this function is slow. You might instead be looking for [`RecordBatch::new_unchecked`] if you've already performed your own validation logic.
    ///
    /// # Arguments
    ///
    /// * `schema` - Expected [`Schema`] of the new [`RecordBatch`], used for validation
    /// * `columns` - Columns to crate a table from as [`Series`] objects
    /// * `num_rows` - Expected number of rows in the [`RecordBatch`], passed explicitly to handle cases where `columns` is empty
    pub fn new_with_size<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> DaftResult<Self> {
        let schema: SchemaRef = schema.into();
        validate_schema(schema.as_ref(), columns.as_slice())?;

        // Validate Series lengths against provided num_rows
        for (field, series) in schema.into_iter().zip(columns.iter()) {
            if series.len() != num_rows {
                return Err(DaftError::ValueError(format!(
                    "While building a RecordBatch with RecordBatch::new_with_size, we found that the Series lengths did not match. Series named: {} had length: {} vs the specified RecordBatch length: {}",
                    field.name,
                    series.len(),
                    num_rows
                )));
            }
        }

        Ok(Self::new_unchecked(schema, columns, num_rows))
    }

    /// Create a new [`RecordBatch`] without any validations
    ///
    /// WARNING: be sure that your data is valid, or this might cause downstream problems such as segfaults!
    ///
    /// This constructor is meant to be used from code that needs very fast performance (e.g. I/O code) and
    /// already does its own validations.
    pub fn new_unchecked<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> Self {
        Self {
            schema: schema.into(),
            columns: Arc::new(columns),
            num_rows,
        }
    }

    pub fn empty(schema: Option<SchemaRef>) -> Self {
        let schema = schema.unwrap_or_else(|| Schema::empty().into());
        let mut columns: Vec<Series> = Vec::with_capacity(schema.len());
        for field in schema.as_ref() {
            let series = Series::empty(&field.name, &field.dtype);
            columns.push(series);
        }
        Self::new_unchecked(schema, columns, 0)
    }

    /// Create a RecordBatch from a set of columns.
    ///
    /// Note: `columns` cannot be empty (will panic if so.) and must all have the same length.
    ///
    /// # Arguments
    ///
    /// * `columns` - Columns to create a table from as [`Series`] objects
    pub fn from_nonempty_columns(columns: impl Into<Arc<Vec<Series>>>) -> DaftResult<Self> {
        let columns = columns.into();
        assert!(
            !columns.is_empty(),
            "Cannot call RecordBatch::new() with empty columns. This indicates an internal error, please file an issue."
        );

        let schema = Schema::new(columns.iter().map(|s| s.field().clone()));
        let schema: SchemaRef = schema.into();
        validate_schema(schema.as_ref(), columns.as_slice())?;

        // Infer the num_rows, assume no broadcasting
        let mut num_rows = 1;
        for (field, series) in schema.into_iter().zip(columns.iter()) {
            if num_rows == 1 {
                num_rows = series.len();
            }
            if series.len() != num_rows {
                return Err(DaftError::ValueError(format!(
                    "While building a RecordBatch with RecordBatch::new_with_nonempty_columns, we found that the Series lengths did not match. Series named: {} had length: {} vs inferred RecordBatch length: {}",
                    field.name,
                    series.len(),
                    num_rows
                )));
            }
        }

        Ok(Self {
            schema,
            columns,
            num_rows,
        })
    }

    pub fn from_arrow<S: Into<SchemaRef>>(
        schema: S,
        arrays: Vec<Box<dyn daft_arrow::array::Array>>,
    ) -> DaftResult<Self> {
        // validate we have at least one array
        if arrays.is_empty() {
            value_err!("Cannot call RecordBatch::from_arrow() with no arrow arrays.")
        }
        // validate that we have a field for each array
        let schema: SchemaRef = schema.into();
        if schema.len() != arrays.len() {
            value_err!(
                "While building a RecordBatch with RecordBatch::from_arrow(), we found that the number of fields in the schema `{}` did not match the number of arrays `{}`",
                schema.len(),
                arrays.len()
            );
        }
        // convert arrays to series and validate lengths
        let mut columns = vec![];
        let mut num_rows = 1;
        for (field, array) in schema.into_iter().zip(arrays.into_iter()) {
            if num_rows == 1 {
                num_rows = array.len();
            }
            if array.len() != num_rows {
                return Err(DaftError::ValueError(format!(
                    "While building a RecordBatch with RecordBatch::from_arrow(), we found that the array for field `{}` had length `{}` whereas the expected length is {}",
                    &field.name,
                    array.len(),
                    num_rows
                )));
            }
            let field = Arc::new(field.clone());
            let column = Series::from_arrow(field, array)?;
            columns.push(column);
        }

        Ok(Self {
            schema,
            columns: Arc::new(columns),
            num_rows,
        })
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> =
            self.columns.iter().map(|s| s.slice(start, end)).collect();
        let new_num_rows = self.len().min(end - start);
        Self::new_with_size(self.schema.clone(), new_series?, new_num_rows)
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            return Ok(Self {
                schema: self.schema.clone(),
                columns: self.columns.clone(),
                num_rows: self.len(),
            });
        }
        self.slice(0, num)
    }

    pub fn sample_by_fraction(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let num = (fraction * self.len() as f64).ceil() as usize;
        self.sample(num, with_replacement, seed)
    }

    pub fn sample(
        &self,
        num: usize,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let len = self.len();

        // Handle size == 0: return empty dataframe
        if num == 0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        // Handle empty dataframe
        if len == 0 {
            if !with_replacement {
                return Err(DaftError::ValueError(
                    "Cannot take a sample larger than the population when 'replace=False'"
                        .to_string(),
                ));
            }
            // For with_replacement=True, we can't sample from empty, so return empty
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        // Handle size > total_rows
        if num > len {
            if !with_replacement {
                return Err(DaftError::ValueError(format!(
                    "Cannot take a sample larger than the population when 'replace=False'. Population size: {}, sample size: {}",
                    len, num
                )));
            }
            // For with_replacement=True, we can sample more than the population
            // Continue to sampling logic below
        } else if num == len && !with_replacement {
            // size == total_rows and no replacement: return all rows
            return Ok(self.clone());
        }

        use rand::{Rng, SeedableRng, distributions::Uniform, rngs::StdRng};
        let mut rng = match seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_rng(rand::thread_rng()).unwrap(),
        };
        let values: Vec<u64> = if with_replacement {
            let range = Uniform::from(0..len as u64);
            rng.sample_iter(&range).take(num).collect()
        } else {
            // https://docs.rs/rand/latest/rand/seq/index/fn.sample.html
            // Randomly sample exactly amount distinct indices from 0..length, and return them in random order (fully shuffled).
            sample(&mut rng, len, num)
                .into_iter()
                .map(|i| i as u64)
                .collect()
        };
        let indices: daft_core::array::DataArray<daft_core::datatypes::UInt64Type> =
            UInt64Array::from(("idx", values));
        self.take(&indices)
    }

    pub fn add_monotonically_increasing_id(
        &self,
        partition_num: u64,
        offset: u64,
        column_name: &str,
    ) -> DaftResult<Self> {
        // Use the leftmost 28 bits for the partition number and the rightmost 36 bits for the row number
        let start = (partition_num << 36) + offset;
        let end = start + self.len() as u64;
        let ids = (start..end).step_by(1).collect::<Vec<_>>();
        let id_series = UInt64Array::from((column_name, ids)).into_series();
        Self::from_nonempty_columns([&[id_series], &self.columns[..]].concat())
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        if self.is_empty() {
            return Ok(self.clone());
        }

        if num == 0 {
            let indices = UInt64Array::empty("idx", &DataType::UInt64);
            return self.take(&indices);
        }

        let self_len = self.len();

        let sample_points: Vec<u64> = (1..num)
            .map(|i| {
                ((i as f64 / num as f64) * self_len as f64)
                    .floor()
                    .to_u64()
                    .unwrap()
                    .min((self.len() - 1) as u64)
            })
            .collect();
        let indices = UInt64Array::from(("idx", sample_points));
        self.take(&indices)
    }

    pub fn size_bytes(&self) -> usize {
        self.columns.iter().map(|s| s.size_bytes()).sum()
    }

    pub fn filter(&self, predicate: &[BoundExpr]) -> DaftResult<Self> {
        if predicate.is_empty() {
            Ok(self.clone())
        } else if predicate.len() == 1 {
            let mask = self.eval_expression(predicate.first().unwrap())?;
            self.mask_filter(&mask)
        } else {
            let mut expr = predicate
                .first()
                .unwrap()
                .inner()
                .clone()
                .and(predicate.get(1).unwrap().inner().clone());
            for i in 2..predicate.len() {
                let next = predicate.get(i).unwrap();
                expr = expr.and(next.inner().clone());
            }
            let mask = self.eval_expression(&BoundExpr::new_unchecked(expr))?;
            self.mask_filter(&mask)
        }
    }

    pub fn mask_filter(&self, mask: &Series) -> DaftResult<Self> {
        if *mask.data_type() != DataType::Boolean {
            return Err(DaftError::ValueError(format!(
                "We can only mask a RecordBatch with a Boolean Series, but we got {}",
                mask.data_type()
            )));
        }

        let mask = mask.downcast::<BooleanArray>().unwrap();
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.filter(mask)).collect();

        // The number of rows post-filter should be the number of 'true' values in the mask
        let num_rows = if mask.len() == 1 {
            // account for broadcasting of mask
            if mask.get(0).is_some_and(|b| b) {
                self.len()
            } else {
                0
            }
        } else {
            // num_filtered is the number of 'false' or null values in the mask
            let num_filtered = mask
                .validity()
                .map(|validity| {
                    daft_arrow::bitmap::and(
                        &daft_arrow::buffer::from_null_buffer(validity.clone()),
                        mask.as_bitmap(),
                    )
                    .unset_bits()
                })
                .unwrap_or_else(|| mask.as_bitmap().unset_bits());
            mask.len() - num_filtered
        };

        Self::new_with_size(self.schema.clone(), new_series?, num_rows)
    }

    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.take(idx)).collect();
        Self::new_with_size(self.schema.clone(), new_series?, idx.len())
    }

    pub fn concat_or_empty<T: AsRef<Self>>(
        tables: &[T],
        schema: Option<SchemaRef>,
    ) -> DaftResult<Self> {
        if tables.is_empty() {
            return Ok(Self::empty(schema));
        }
        Self::concat(tables)
    }

    pub fn concat<T: AsRef<Self>>(tables: &[T]) -> DaftResult<Self> {
        if tables.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 RecordBatch to perform concat".to_string(),
            ));
        }
        if tables.len() == 1 {
            return Ok((*tables.first().unwrap().as_ref()).clone());
        }
        let first_table = tables.first().unwrap().as_ref();

        let first_schema = first_table.schema.as_ref();
        for tab in tables.iter().skip(1).map(|t| t.as_ref()) {
            if tab.schema.as_ref() != first_schema {
                return Err(DaftError::SchemaMismatch(format!(
                    "RecordBatch concat requires all schemas to match, {} vs {}",
                    first_schema, tab.schema
                )));
            }
        }
        let num_columns = first_table.num_columns();
        let mut new_series = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let series_to_cat: Vec<&Series> =
                tables.iter().map(|s| s.as_ref().get_column(i)).collect();
            new_series.push(Series::concat(series_to_cat.as_slice())?);
        }

        Self::new_with_size(
            first_table.schema.clone(),
            new_series,
            tables.iter().map(|t| t.as_ref().len()).sum(),
        )
    }

    pub fn union(&self, other: &Self) -> DaftResult<Self> {
        if self.num_rows != other.num_rows {
            return Err(DaftError::ValueError(format!(
                "Cannot union tables of length {} and {}",
                self.num_rows, other.num_rows
            )));
        }
        let unioned = self
            .columns
            .iter()
            .chain(other.columns.iter())
            .cloned()
            .collect::<Vec<_>>();
        Self::from_nonempty_columns(unioned)
    }

    pub fn get_column(&self, idx: usize) -> &Series {
        &self.columns[idx]
    }

    pub fn get_columns(&self, indices: &[usize]) -> Self {
        let new_columns = indices
            .iter()
            .map(|i| self.columns[*i].clone())
            .collect::<Vec<_>>();

        let new_schema = Schema::new(indices.iter().map(|i| self.schema[*i].clone()));

        Self::new_unchecked(new_schema, new_columns, self.num_rows)
    }

    pub fn columns(&self) -> &[Series] {
        &self.columns
    }

    pub fn append_column(&self, new_schema: SchemaRef, series: Series) -> DaftResult<Self> {
        if self.num_rows != series.len() {
            return Err(DaftError::ValueError(format!(
                "Cannot append column to RecordBatch of length {} with column of length {}",
                self.num_rows,
                series.len()
            )));
        }

        let mut new_columns = self.columns.as_ref().clone();
        new_columns.push(series);

        Ok(Self::new_unchecked(new_schema, new_columns, self.num_rows))
    }

    fn eval_agg_expression(
        &self,
        agg_expr: &BoundAggExpr,
        groups: Option<&GroupIndices>,
    ) -> DaftResult<Series> {
        match agg_expr.as_ref() {
            &AggExpr::Count(ref expr, mode) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .count(groups, mode),
            AggExpr::CountDistinct(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .count_distinct(groups),
            AggExpr::Sum(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .sum(groups),
            AggExpr::Product(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .product(groups),
            &AggExpr::ApproxPercentile(ApproxPercentileParams {
                child: ref expr,
                ref percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                self.eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                    .approx_sketch(groups)?
                    .sketch_percentile(&percentiles, force_list_output)
            }
            AggExpr::ApproxCountDistinct(expr) => {
                let hashed = self
                    .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                    .hash_with_validity(None)?;
                let series = groups
                    .map_or_else(
                        || hashed.approx_count_distinct(),
                        |groups| hashed.grouped_approx_count_distinct(groups),
                    )?
                    .into_series();
                Ok(series)
            }
            &AggExpr::ApproxSketch(ref expr, sketch_type) => {
                let evaled = self.eval_expression(&BoundExpr::new_unchecked(expr.clone()))?;
                match sketch_type {
                    SketchType::DDSketch => evaled.approx_sketch(groups),
                    SketchType::HyperLogLog => {
                        let hashed = self
                            .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                            .hash_with_validity(None)?;
                        let series = groups
                            .map_or_else(
                                || hashed.hll_sketch(),
                                |groups| hashed.grouped_hll_sketch(groups),
                            )?
                            .into_series();
                        Ok(series)
                    }
                }
            }
            &AggExpr::MergeSketch(ref expr, sketch_type) => {
                let evaled = self.eval_expression(&BoundExpr::new_unchecked(expr.clone()))?;
                match sketch_type {
                    SketchType::DDSketch => evaled.merge_sketch(groups),
                    SketchType::HyperLogLog => evaled.hll_merge(groups),
                }
            }
            AggExpr::Mean(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .mean(groups),
            AggExpr::Stddev(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .stddev(groups),
            AggExpr::Min(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .min(groups),
            AggExpr::Max(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .max(groups),
            AggExpr::BoolAnd(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .bool_and(groups),
            AggExpr::BoolOr(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .bool_or(groups),
            &AggExpr::AnyValue(ref expr, ignore_nulls) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .any_value(groups, ignore_nulls),
            AggExpr::List(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .agg_list(groups),
            AggExpr::Set(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .agg_set(groups),
            AggExpr::Concat(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .agg_concat(groups),
            AggExpr::Skew(expr) => self
                .eval_expression(&BoundExpr::new_unchecked(expr.clone()))?
                .skew(groups),
            AggExpr::MapGroups { .. } => Err(DaftError::ValueError(
                "MapGroups not supported via aggregation, use map_groups instead".to_string(),
            )),
        }
    }

    pub async fn eval_expression_async(&self, expr: BoundExpr) -> DaftResult<Series> {
        let mut sink = NoopMetricsCollector;
        self.eval_expression_async_with_metrics(expr, &mut sink)
            .await
    }

    #[async_recursion::async_recursion]
    pub async fn eval_expression_async_with_metrics(
        &self,
        expr: BoundExpr,
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        let expected_field = expr.inner().to_field(self.schema.as_ref())?;
        let series = match expr.as_ref() {
            Expr::Alias(child, name) => Ok(
                self.eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .rename(name),
            ),
            Expr::Agg(agg_expr) => {
                self.eval_agg_expression(&BoundAggExpr::new_unchecked(agg_expr.clone()), None)
            }
            Expr::Over(..) => Err(DaftError::ComputeError(
                "Window expressions should be evaluated via the window operator.".to_string(),
            )),
            Expr::WindowFunction(..) => Err(DaftError::ComputeError(
                "Window expressions cannot be directly evaluated. Please specify a window using \"over\".".to_string(),
            )),
            Expr::Cast(child, dtype) => self
                .eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .cast(dtype),
            Expr::Column(Column::Bound(BoundColumn { index, .. })) => {
                Ok(self.columns[*index].clone())
            }
            Expr::Not(child) => !(self
                .eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?),
            Expr::IsNull(child) => self
                .eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .is_null(),
            Expr::NotNull(child) => self
                .eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .not_null(),
            Expr::FillNull(child, fill_value) => {
                let fill_value = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(fill_value.clone()),
                        metrics,
                    )
                    .await?;
                self.eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .fill_null(&fill_value)
            }
            Expr::IsIn(child, items) => {
                if items.is_empty() {
                    return BooleanArray::from_iter(
                        &child.get_name(&self.schema)?,
                        std::iter::once(Some(false)),
                    )
                    .into_series()
                    .broadcast(self.len());
                }
                let mut evaluated_items = Vec::with_capacity(items.len());
                for item in items {
                    evaluated_items.push(
                        self.eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(item.clone()),
                            metrics,
                        )
                        .await?,
                    );
                }
                let items = evaluated_items.iter().collect::<Vec<&Series>>();
                let s = Series::concat(items.as_slice())?;
                self.eval_expression_async_with_metrics(
                    BoundExpr::new_unchecked(child.clone()),
                    metrics,
                )
                .await?
                .is_in(&s)
            }
            Expr::List(items) => {
                let field = expr.inner().to_field(&self.schema)?;
                let dtype = if let DataType::List(dtype) = &field.dtype {
                    dtype
                } else {
                    return Err(DaftError::ComputeError(
                        "List expression must be of type List(T)".to_string(),
                    ));
                };
                let cast_items = items
                    .iter()
                    .map(|i| i.clone().cast(dtype))
                    .collect::<Vec<_>>();
                let mut evaluated = Vec::with_capacity(cast_items.len());
                for item in cast_items {
                    evaluated.push(
                        self.eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(item),
                            metrics,
                        )
                        .await?,
                    );
                }
                let items = evaluated.iter().collect::<Vec<&Series>>();
                Series::zip(field, items.as_slice())
            }
            Expr::Between(child, lower, upper) => {
                let child_series = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(child.clone()),
                        metrics,
                    )
                    .await?;
                let lower_series = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(lower.clone()),
                        metrics,
                    )
                    .await?;
                let upper_series = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(upper.clone()),
                        metrics,
                    )
                    .await?;
                child_series.between(&lower_series, &upper_series)
            }
            Expr::BinaryOp { op, left, right } => {
                let lhs = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(left.clone()),
                        metrics,
                    )
                    .await?;
                let rhs = self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(right.clone()),
                        metrics,
                    )
                    .await?;
                use daft_core::array::ops::{DaftCompare, DaftLogical};
                use daft_dsl::Operator::*;
                match op {
                    Plus => lhs + rhs,
                    Minus => lhs - rhs,
                    TrueDivide => lhs / rhs,
                    FloorDivide => lhs.floor_div(&rhs),
                    Multiply => lhs * rhs,
                    Modulus => lhs % rhs,
                    Lt => Ok(lhs.lt(&rhs)?.into_series()),
                    LtEq => Ok(lhs.lte(&rhs)?.into_series()),
                    Eq => Ok(lhs.equal(&rhs)?.into_series()),
                    EqNullSafe => Ok(lhs.eq_null_safe(&rhs)?.into_series()),
                    NotEq => Ok(lhs.not_equal(&rhs)?.into_series()),
                    GtEq => Ok(lhs.gte(&rhs)?.into_series()),
                    Gt => Ok(lhs.gt(&rhs)?.into_series()),
                    And => lhs.and(&rhs),
                    Or => lhs.or(&rhs),
                    Xor => lhs.xor(&rhs),
                    ShiftLeft => lhs.shift_left(&rhs),
                    ShiftRight => lhs.shift_right(&rhs),
                }
            }
            Expr::Function { func, inputs } => {
                let mut evaluated_inputs = Vec::with_capacity(inputs.len());
                for e in inputs {
                    evaluated_inputs.push(
                        self.eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(e.clone()),
                            metrics,
                        )
                        .await?,
                    );
                }
                func.evaluate(evaluated_inputs.as_slice(), func)
            }
            Expr::ScalarFn(ScalarFn::Builtin(func)) => {
                let mut evaluated_args = Vec::new();
                for arg in func.inputs.iter() {
                    let evaluated = match arg {
                        FunctionArg::Named { name, arg: e } => {
                            let result = self
                                .eval_expression_async_with_metrics(
                                    BoundExpr::new_unchecked(e.clone()),
                                    metrics,
                                )
                                .await?;
                            FunctionArg::Named {
                                name: name.clone(),
                                arg: result,
                            }
                        }
                        FunctionArg::Unnamed(e) => {
                            let result = self
                                .eval_expression_async_with_metrics(
                                    BoundExpr::new_unchecked(e.clone()),
                                    metrics,
                                )
                                .await?;
                            FunctionArg::Unnamed(result)
                        }
                    };
                    evaluated_args.push(evaluated);
                }
                let args = FunctionArgs::new_unchecked(evaluated_args);
                match &func.func {
                    BuiltinScalarFnVariant::Sync(func) => func.call(args),
                    BuiltinScalarFnVariant::Async(func) => func.call(args).await,
                }
            }
            Expr::Literal(lit_value) => Ok(lit_value.clone().into()),
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => match predicate.as_ref() {
                Expr::Literal(Literal::Boolean(true)) => self
                    .eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(if_true.clone()),
                        metrics,
                    )
                    .await,
                Expr::Literal(Literal::Boolean(false)) => Ok(
                    self.eval_expression_async_with_metrics(
                        BoundExpr::new_unchecked(if_false.clone()),
                        metrics,
                    )
                    .await?
                    .rename(if_true.get_name(&self.schema)?),
                ),
                _ => {
                    let if_true_series = self
                        .eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(if_true.clone()),
                            metrics,
                        )
                        .await?;
                    let if_false_series = self
                        .eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(if_false.clone()),
                            metrics,
                        )
                        .await?;
                    let predicate_series = self
                        .eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(predicate.clone()),
                            metrics,
                        )
                        .await?;
                    Ok(if_true_series.if_else(&if_false_series, &predicate_series)?)
                }
            },
            Expr::ScalarFn(ScalarFn::Python(python_udf)) => {
                let mut args = Vec::with_capacity(python_udf.args().len());
                for expr in python_udf.args() {
                    args.push(
                        self.eval_expression_async_with_metrics(
                            BoundExpr::new_unchecked(expr),
                            metrics,
                        )
                        .await?,
                    );
                }
                if python_udf.is_async() {
                    python_udf.call_async(args.as_slice(), metrics).await
                } else {
                    python_udf.call(args.as_slice(), metrics)
                }
            }
            Expr::Subquery(_subquery) => Err(DaftError::ComputeError(
                "Subquery should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::InSubquery(_expr, _subquery) => Err(DaftError::ComputeError(
                "IN <SUBQUERY> should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::Exists(_subquery) => Err(DaftError::ComputeError(
                "EXISTS <SUBQUERY> should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::Column(_) => {
                unreachable!("bound expressions should not have unbound columns")
            }
            Expr::VLLM(..) => unreachable!(
                "VLLM expressions should not be evaluated directly. This indicates a bug in the query optimizer."
            ),
        }?;

        if expected_field.name != series.field().name {
            return Err(DaftError::ComputeError(format!(
                "Mismatch of expected expression name and name from computed series ({} vs {}) for expression: {expr}",
                expected_field.name,
                series.field().name
            )));
        }

        assert!(
            !(expected_field.dtype != series.field().dtype),
            "Data type mismatch in expression evaluation:\n\
                    Expected type: {}\n\
                    Computed type: {}\n\
                    Expression: {}\n\
                    This likely indicates an internal error in type inference or computation.",
            expected_field.dtype,
            series.field().dtype,
            expr
        );
        Ok(series)
    }

    pub fn eval_expression(&self, expr: &BoundExpr) -> DaftResult<Series> {
        let mut sink = NoopMetricsCollector;
        self.eval_expression_internal(expr, &mut sink)
    }

    pub fn eval_expression_with_metrics(
        &self,
        expr: &BoundExpr,
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        self.eval_expression_internal(expr, metrics)
    }

    fn eval_expression_internal(
        &self,
        expr: &BoundExpr,
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Series> {
        let expected_field = expr.inner().to_field(self.schema.as_ref())?;
        let series = match expr.as_ref() {
            Expr::Alias(child, name) => self
                .eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)
                .map(|s| s.rename(name)),
            Expr::Agg(agg_expr) => {
                self.eval_agg_expression(&BoundAggExpr::new_unchecked(agg_expr.clone()), None)
            }
            Expr::Over(..) => Err(DaftError::ComputeError(
                "Window expressions should be evaluated via the window operator.".to_string(),
            )),
            Expr::WindowFunction(..) => Err(DaftError::ComputeError(
                "Window expressions cannot be directly evaluated. Please specify a window using \"over\".".to_string(),
            )),
            Expr::Cast(child, dtype) => self
                .eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                .cast(dtype),
            Expr::Column(Column::Bound(BoundColumn { index, .. })) => {
                Ok(self.columns[*index].clone())
            }
            Expr::Not(child) => !(self.eval_expression_internal(
                &BoundExpr::new_unchecked(child.clone()),
                metrics,
            )?),
            Expr::IsNull(child) => self
                .eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                .is_null(),
            Expr::NotNull(child) => self
                .eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                .not_null(),
            Expr::FillNull(child, fill_value) => {
                let fill_value =
                    self.eval_expression_internal(&BoundExpr::new_unchecked(fill_value.clone()), metrics)?;
                self.eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                    .fill_null(&fill_value)
            }
            Expr::IsIn(child, items) => {
                if items.is_empty() {
                    return BooleanArray::from_iter(
                        &child.get_name(&self.schema)?,
                        std::iter::once(Some(false)),
                    )
                    .into_series()
                    .broadcast(self.len());
                }
                let mut evaluated_items = Vec::with_capacity(items.len());
                for i in items {
                    evaluated_items.push(self.eval_expression_internal(
                        &BoundExpr::new_unchecked(i.clone()),
                        metrics,
                    )?);
                }
                let items_refs = evaluated_items.iter().collect::<Vec<&Series>>();
                let s = Series::concat(items_refs.as_slice())?;
                self.eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                    .is_in(&s)
            }
            Expr::List(items) => {
                let field = expr.inner().to_field(&self.schema)?;
                let dtype = if let DataType::List(dtype) = &field.dtype {
                    dtype
                } else {
                    return Err(DaftError::ComputeError(
                        "List expression must be of type List(T)".to_string(),
                    ));
                };
                let items = items.iter().map(|i| i.clone().cast(dtype)).collect::<Vec<_>>();
                let mut evaluated = Vec::with_capacity(items.len());
                for i in items {
                    evaluated.push(self.eval_expression_internal(
                        &BoundExpr::new_unchecked(i.clone()),
                        metrics,
                    )?);
                }
                let items = evaluated.iter().collect::<Vec<&Series>>();
                Series::zip(field, items.as_slice())
            }
            Expr::Between(child, lower, upper) => self
                .eval_expression_internal(&BoundExpr::new_unchecked(child.clone()), metrics)?
                .between(
                    &self.eval_expression_internal(&BoundExpr::new_unchecked(lower.clone()), metrics)?,
                    &self.eval_expression_internal(&BoundExpr::new_unchecked(upper.clone()), metrics)?,
                ),
            Expr::BinaryOp { op, left, right } => {
                let lhs = self.eval_expression_internal(&BoundExpr::new_unchecked(left.clone()), metrics)?;
                let rhs = self.eval_expression_internal(&BoundExpr::new_unchecked(right.clone()), metrics)?;
                use daft_core::array::ops::{DaftCompare, DaftLogical};
                use daft_dsl::Operator::*;
                match op {
                    Plus => lhs + rhs,
                    Minus => lhs - rhs,
                    TrueDivide => lhs / rhs,
                    FloorDivide => lhs.floor_div(&rhs),
                    Multiply => lhs * rhs,
                    Modulus => lhs % rhs,
                    Lt => Ok(lhs.lt(&rhs)?.into_series()),
                    LtEq => Ok(lhs.lte(&rhs)?.into_series()),
                    Eq => Ok(lhs.equal(&rhs)?.into_series()),
                    EqNullSafe => Ok(lhs.eq_null_safe(&rhs)?.into_series()),
                    NotEq => Ok(lhs.not_equal(&rhs)?.into_series()),
                    GtEq => Ok(lhs.gte(&rhs)?.into_series()),
                    Gt => Ok(lhs.gt(&rhs)?.into_series()),
                    And => lhs.and(&rhs),
                    Or => lhs.or(&rhs),
                    Xor => lhs.xor(&rhs),
                    ShiftLeft => lhs.shift_left(&rhs),
                    ShiftRight => lhs.shift_right(&rhs),
                }
            }
            Expr::Function { func, inputs } => {
                let mut evaluated_inputs = Vec::with_capacity(inputs.len());
                for e in inputs {
                    evaluated_inputs.push(self.eval_expression_internal(
                        &BoundExpr::new_unchecked(e.clone()),
                        metrics,
                    )?);
                }
                func.evaluate(evaluated_inputs.as_slice(), func)
            }
            Expr::ScalarFn(ScalarFn::Builtin(BuiltinScalarFn { func, inputs })) => {
                let mut evaluated_args: Vec<FunctionArg<Series>> = Vec::with_capacity(inputs.len());
                for arg in inputs.iter() {
                    let evaluated = match arg {
                        FunctionArg::Named { name, arg: expr } => {
                            let result = self.eval_expression_internal(
                                &BoundExpr::new_unchecked(expr.clone()),
                                metrics,
                            )?;
                            FunctionArg::Named {
                                name: name.clone(),
                                arg: result,
                            }
                        }
                        FunctionArg::Unnamed(expr) => {
                            let result = self.eval_expression_internal(
                                &BoundExpr::new_unchecked(expr.clone()),
                                metrics,
                            )?;
                            FunctionArg::Unnamed(result)
                        }
                    };
                    evaluated_args.push(evaluated);
                }
                let args = FunctionArgs::new_unchecked(evaluated_args);
                match func {
                    BuiltinScalarFnVariant::Sync(f) => f.call(args),
                    BuiltinScalarFnVariant::Async(f) => {
                        get_compute_runtime().block_on_current_thread(f.call(args))
                    }
                }
            }
            Expr::Literal(lit_value) => Ok(lit_value.clone().into()),
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => match predicate.as_ref() {
                Expr::Literal(Literal::Boolean(true)) => self
                    .eval_expression_internal(&BoundExpr::new_unchecked(if_true.clone()), metrics),
                Expr::Literal(Literal::Boolean(false)) => {
                    let false_name = if_true.get_name(&self.schema)?;
                    self.eval_expression_internal(
                        &BoundExpr::new_unchecked(if_false.clone()),
                        metrics,
                    )
                    .map(|s| s.rename(false_name))
                }
                _ => {
                    let if_true_series =
                        self.eval_expression_internal(&BoundExpr::new_unchecked(if_true.clone()), metrics)?;
                    let if_false_series =
                        self.eval_expression_internal(&BoundExpr::new_unchecked(if_false.clone()), metrics)?;
                    let predicate_series =
                        self.eval_expression_internal(&BoundExpr::new_unchecked(predicate.clone()), metrics)?;
                    if_true_series.if_else(&if_false_series, &predicate_series)
                }
            },
            Expr::ScalarFn(ScalarFn::Python(python_udf)) => {
                let mut args = Vec::with_capacity(python_udf.args().len());
                for expr in python_udf.args() {
                    args.push(self.eval_expression_internal(
                        &BoundExpr::new_unchecked(expr.clone()),
                        metrics,
                    )?);
                }
                #[cfg(feature = "python")]
                {
                    if python_udf.is_async() {
                            get_compute_runtime()
                                .block_on_current_thread(python_udf.call_async(args.as_slice(), metrics))
                    } else {
                        python_udf.call(args.as_slice(), metrics)
                    }
                }
                #[cfg(not(feature = "python"))]
                {
                    let _ = (args, metrics);
                    panic!(
                        "Cannot evaluate a Python UDF without compiling for Python"
                    );
                }
            }
            Expr::Subquery(_subquery) => Err(DaftError::ComputeError(
                "Subquery should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::InSubquery(_expr, _subquery) => Err(DaftError::ComputeError(
                "IN <SUBQUERY> should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::Exists(_subquery) => Err(DaftError::ComputeError(
                "EXISTS <SUBQUERY> should be optimized away before evaluation. This indicates a bug in the query optimizer.".to_string(),
            )),
            Expr::Column(_) => unreachable!("bound expressions should not have unbound columns"),
            Expr::VLLM(..) => unreachable!(
                "VLLM expressions should not be evaluated directly. This indicates a bug in the query optimizer."
            ),
        }?;

        if expected_field.name != series.field().name {
            return Err(DaftError::ComputeError(format!(
                "Mismatch of expected expression name and name from computed series ({} vs {}) for expression: {expr}",
                expected_field.name,
                series.field().name
            )));
        }

        assert!(
            expected_field.dtype == series.field().dtype,
            "Data type mismatch in expression evaluation:\n\
                    Expected type: {}\n\
                    Computed type: {}\n\
                    Expression: {}\n\
                    This likely indicates an internal error in type inference or computation.",
            expected_field.dtype,
            series.field().dtype,
            expr
        );
        Ok(series)
    }

    // TODO(universalmind303): since we now have async expressions, the entire evaluation should happen async
    // Refactor all eval_expression's to async and remove the sync version.
    pub fn eval_expression_list(&self, exprs: &[BoundExpr]) -> DaftResult<Self> {
        let result_series: Vec<_> = exprs
            .iter()
            .map(|e| self.eval_expression(e))
            .try_collect()?;

        self.process_eval_results(exprs, result_series)
    }

    pub fn eval_expression_list_with_metrics(
        &self,
        exprs: &[BoundExpr],
        metrics: &mut dyn MetricsCollector,
    ) -> DaftResult<Self> {
        let result_series: Vec<_> = exprs
            .iter()
            .map(|e| self.eval_expression_with_metrics(e, metrics))
            .try_collect()?;

        self.process_eval_results(exprs, result_series)
    }

    pub async fn eval_expression_list_async(&self, exprs: Vec<BoundExpr>) -> DaftResult<Self> {
        let futs = exprs
            .clone()
            .into_iter()
            .map(|e| self.eval_expression_async(e));

        let result_series = try_join_all(futs).await?;

        self.process_eval_results(exprs.as_ref(), result_series)
    }

    pub async fn par_eval_expression_list(
        &self,
        exprs: &[BoundExpr],
        num_parallel_tasks: usize,
    ) -> DaftResult<Self> {
        // Partition the expressions into compute and non-compute
        let (compute_exprs, non_compute_exprs): (Vec<_>, Vec<_>) = exprs
            .iter()
            .cloned()
            .enumerate()
            .partition(|(_, e)| e.inner().has_compute());

        // Evaluate non-compute expressions
        let non_compute_results = non_compute_exprs
            .into_iter()
            .map(|(i, e)| (i, self.eval_expression(&e)))
            .collect::<Vec<_>>();

        let compute_runtime = get_compute_runtime();

        let compute_futures = compute_exprs.into_iter().map(|(i, e)| {
            let table = self.clone();
            compute_runtime.spawn(async move { (i, table.eval_expression_async(e).await) })
        });

        // Collect the results of the compute expressions
        let compute_results = futures::stream::iter(compute_futures)
            .buffered(num_parallel_tasks)
            .try_collect::<Vec<_>>()
            .await?;

        // Combine and sort by original index
        let mut all_results = non_compute_results;
        all_results.extend(compute_results);
        all_results.sort_by_key(|(i, _)| *i);

        // Extract just the results in order
        let result_series = all_results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<DaftResult<Vec<_>>>()?;

        self.process_eval_results(exprs, result_series)
    }

    fn process_eval_results(
        &self,
        exprs: &[BoundExpr],
        result_series: Vec<Series>,
    ) -> DaftResult<Self> {
        let fields = result_series.iter().map(|s| s.field().clone());

        let new_schema = Schema::new(fields);

        let has_agg_expr = exprs.iter().any(|e| matches!(e.as_ref(), Expr::Agg(..)));
        let num_rows = match (has_agg_expr, self.len()) {
            // "Normal" case: the final cardinality is the max(*results_lens, self.len())
            // This correctly accounts for broadcasting of literals, which can have unit length
            (false, self_len) if self_len > 0 => result_series
                .iter()
                .map(|s| s.len())
                .chain(std::iter::once(self.len()))
                .max()
                .unwrap(),
            // "Empty" case: when no aggregation is applied, the expected result should also be empty
            (false, _) => 0,
            // "Aggregation" case: the final cardinality is the max(results' lens)
            // We discard the original self.len() because we expect aggregations to change
            // the final cardinality. Aggregations on empty tables are expected to produce unit length results.
            (true, _) => result_series.iter().map(|s| s.len()).max().unwrap(),
        };

        Self::new_with_broadcast(new_schema, result_series, num_rows)
    }

    pub fn as_physical(&self) -> DaftResult<Self> {
        let new_series: Vec<Series> = self
            .columns
            .iter()
            .map(|s| s.as_physical())
            .collect::<DaftResult<Vec<_>>>()?;
        let new_schema = Schema::new(new_series.iter().map(|s| s.field().clone()));
        Self::new_with_size(new_schema, new_series, self.len())
    }

    #[deprecated(note = "name-referenced columns")]
    /// Casts a `RecordBatch` to a schema.
    ///
    /// Note: this method is deprecated because it maps fields by name, which will not work for schemas with duplicate field names.
    /// It should only be used for scans, and once we support reading files with duplicate column names, we should remove this function.
    pub fn cast_to_schema(&self, schema: &Schema) -> DaftResult<Self> {
        #[allow(deprecated)]
        self.cast_to_schema_with_fill(schema, None)
    }

    #[deprecated(note = "name-referenced columns")]
    /// Casts a `RecordBatch` to a schema, using `fill_map` to specify the default expression for a column that doesn't exist.
    ///
    /// Note: this method is deprecated because it maps fields by name, which will not work for schemas with duplicate field names.
    /// It should only be used for scans, and once we support reading files with duplicate column names, we should remove this function.
    pub fn cast_to_schema_with_fill(
        &self,
        schema: &Schema,
        fill_map: Option<&HashMap<&str, ExprRef>>,
    ) -> DaftResult<Self> {
        let current_col_names = HashSet::<_>::from_iter(self.schema.field_names());
        let null_lit = null_lit();
        let exprs: Vec<_> = schema
            .into_iter()
            .map(|field| {
                if current_col_names.contains(field.name.as_str()) {
                    // For any fields already in the table, perform a cast
                    resolved_col(field.name.clone()).cast(&field.dtype)
                } else {
                    // For any fields in schema that are not in self.schema, use fill map to fill with an expression.
                    // If no entry for column name, fall back to null literal (i.e. create a null array for that column).
                    fill_map
                        .as_ref()
                        .and_then(|m| m.get(field.name.as_str()))
                        .unwrap_or(&null_lit)
                        .clone()
                        .alias(field.name.clone())
                        .cast(&field.dtype)
                }
            })
            .map(|expr| BoundExpr::try_new(expr, &self.schema))
            .try_collect()?;
        self.eval_expression_list(&exprs)
    }

    pub fn repr_html(&self) -> String {
        // Produces a <table> HTML element.

        let num_columns = self.columns.len();

        let mut res =
            "<table class=\"dataframe\" style=\"table-layout: fixed; min-width: 100%\">\n"
                .to_string();

        // Begin the header.
        res.push_str("<thead><tr>");

        let header_style = format!(
            "text-wrap: nowrap; width: calc(100vw / {}); min-width: 192px; overflow: hidden; text-overflow: ellipsis; text-align:left",
            num_columns
        );

        for field in self.schema.as_ref() {
            #[allow(clippy::format_push_string)]
            res.push_str(&format!("<th style=\"{}\">", header_style));
            res.push_str(&html_escape::encode_text(&field.name));
            res.push_str("<br />");
            res.push_str(&html_escape::encode_text(&format!("{}", field.dtype)));
            res.push_str("</th>");
        }

        // End the header.
        res.push_str("</tr></thead>\n");

        // Begin the body.
        res.push_str("<tbody>\n");

        let body_style = format!(
            "text-align:left; width: calc(100vw / {}); min-width: 192px; max-height: 100px; overflow: hidden; text-overflow: ellipsis; word-wrap: break-word; overflow-y: auto",
            num_columns
        );

        let total_rows = self.len();

        for i in 0..total_rows {
            // Begin row.
            res.push_str("<tr>");

            for (col_idx, col) in self.columns.iter().enumerate() {
                #[allow(clippy::format_push_string)]
                res.push_str(&format!(
                    "<td data-row=\"{}\" data-col=\"{}\"><div style=\"{}\">",
                    i, col_idx, body_style
                ));
                res.push_str(&html_value(col, i, true));
                res.push_str("</div></td>");
            }

            // End row.
            res.push_str("</tr>\n");
        }

        // End the body and the table.
        res.push_str("</tbody>\n</table>");

        res
    }

    pub fn to_comfy_table(&self, max_col_width: Option<usize>) -> comfy_table::Table {
        let str_values = self
            .columns
            .iter()
            .map(|s| s as &dyn StrValue)
            .collect::<Vec<_>>();

        make_comfy_table(
            self.schema
                .into_iter()
                .map(|field| format!("{}\n---\n{}", field.name, field.dtype))
                .collect::<Vec<_>>()
                .as_slice(),
            Some(str_values.as_slice()),
            Some(self.len()),
            max_col_width,
        )
    }

    #[deprecated(note = "arrow2 migration")]
    #[allow(deprecated, reason = "arrow2 migration")]
    pub fn to_chunk(&self) -> Chunk<Box<dyn Array>> {
        Chunk::new(self.columns.iter().map(|s| s.to_arrow2()).collect())
    }

    pub fn to_ipc_stream(&self) -> DaftResult<Vec<u8>> {
        let buffer = Vec::with_capacity(self.size_bytes());
        #[allow(deprecated, reason = "arrow2 migration")]
        let schema = self.schema.to_arrow2()?;
        let options = daft_arrow::io::ipc::write::WriteOptions { compression: None };
        let mut writer = daft_arrow::io::ipc::write::StreamWriter::new(buffer, options);
        writer.start(&schema, None)?;

        #[allow(deprecated, reason = "arrow2 migration")]
        let chunk = self.to_chunk();
        writer.write(&chunk, None)?;

        writer.finish()?;
        let mut finished_buffer = writer.into_inner();
        finished_buffer.shrink_to_fit();
        Ok(finished_buffer)
    }

    pub fn from_ipc_stream(buffer: &[u8]) -> DaftResult<Self> {
        let mut cursor = Cursor::new(buffer);
        let stream_metadata = daft_arrow::io::ipc::read::read_stream_metadata(&mut cursor)?;
        let schema = Arc::new(Schema::from(stream_metadata.schema.clone()));
        let reader = daft_arrow::io::ipc::read::StreamReader::new(cursor, stream_metadata, None);

        let mut tables = reader
            .into_iter()
            .map(|state| {
                let state = state?;
                let arrow_chunk = match state {
                    daft_arrow::io::ipc::read::StreamState::Some(chunk) => chunk,
                    _ => panic!("State should not be waiting when reading from IPC buffer"),
                };
                Self::from_arrow(schema.clone(), arrow_chunk.into_arrays())
            })
            .collect::<DaftResult<Vec<_>>>()?;

        assert_eq!(tables.len(), 1);
        Ok(tables.pop().expect("Expected exactly one table"))
    }
}

impl TryFrom<RecordBatch> for arrow_array::RecordBatch {
    type Error = DaftError;

    #[allow(deprecated, reason = "arrow2 migration")]
    fn try_from(record_batch: RecordBatch) -> DaftResult<Self> {
        let schema = Arc::new(record_batch.schema.to_arrow2()?.into());
        let columns = record_batch
            .columns
            .iter()
            .map(|s| s.to_arrow2().into())
            .collect::<Vec<_>>();
        Self::try_new(schema, columns).map_err(DaftError::ArrowRsError)
    }
}

impl TryFrom<RecordBatch> for FileInfos {
    type Error = DaftError;

    fn try_from(record_batch: RecordBatch) -> DaftResult<Self> {
        let get_column_by_name = |name| {
            if let [(idx, _)] = record_batch.schema.get_fields_with_name(name)[..] {
                Ok(record_batch.get_column(idx))
            } else {
                Err(DaftError::SchemaMismatch(format!(
                    "RecordBatch requires columns \"path\", \"size\", and \"num_rows\" to convert to FileInfos, found: {}",
                    record_batch.schema
                )))
            }
        };

        let file_paths = get_column_by_name("path")?
            .utf8()?
            .data()
            .as_any()
            .downcast_ref::<daft_arrow::array::Utf8Array<i64>>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect::<Vec<_>>();
        let file_sizes = get_column_by_name("size")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<daft_arrow::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.copied())
            .collect::<Vec<_>>();
        let num_rows = get_column_by_name("num_rows")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<daft_arrow::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.copied())
            .collect::<Vec<_>>();
        Ok(Self::new_internal(file_paths, file_sizes, num_rows))
    }
}

impl TryFrom<&FileInfos> for RecordBatch {
    type Error = DaftError;

    fn try_from(file_info: &FileInfos) -> DaftResult<Self> {
        let columns = vec![
            Series::try_from((
                "path",
                daft_arrow::array::Utf8Array::<i64>::from_iter_values(file_info.file_paths.iter())
                    .to_boxed(),
            ))?,
            Series::try_from((
                "size",
                daft_arrow::array::PrimitiveArray::<i64>::from(&file_info.file_sizes).to_boxed(),
            ))?,
            Series::try_from((
                "num_rows",
                daft_arrow::array::PrimitiveArray::<i64>::from(&file_info.num_rows).to_boxed(),
            ))?,
        ];
        Self::from_nonempty_columns(columns)
    }
}

impl PartialEq for RecordBatch {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        if self.schema != other.schema {
            return false;
        }
        for (lhs, rhs) in self.columns.iter().zip(other.columns.iter()) {
            if lhs != rhs {
                return false;
            }
        }
        true
    }
}

impl Eq for RecordBatch {}

impl Display for RecordBatch {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = self.to_comfy_table(Some(32));
        writeln!(f, "{table}")
    }
}

impl AsRef<Self> for RecordBatch {
    fn as_ref(&self) -> &Self {
        self
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};

    use crate::RecordBatch;

    #[test]
    fn add_int_and_float_expression() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3])).into_series();
        let b = Float64Array::from(("b", vec![1., 2., 3.])).into_series();
        let _schema = Schema::new(vec![
            a.field().clone().rename("a"),
            b.field().clone().rename("b"),
        ]);
        let table = RecordBatch::from_nonempty_columns(vec![a, b])?;
        let e1 = resolved_col("a").add(resolved_col("b"));
        let result = table.eval_expression(&BoundExpr::try_new(e1, &table.schema)?)?;
        assert_eq!(*result.data_type(), DataType::Float64);
        assert_eq!(result.len(), 3);

        let e2 = resolved_col("a")
            .add(resolved_col("b"))
            .cast(&DataType::Int64);
        let result = table.eval_expression(&&BoundExpr::try_new(e2, &table.schema)?)?;
        assert_eq!(*result.data_type(), DataType::Int64);
        assert_eq!(result.len(), 3);

        Ok(())
    }
}
