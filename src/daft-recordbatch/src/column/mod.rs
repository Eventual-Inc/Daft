mod scalar;

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_display::table_display::StrValue;
use common_error::DaftResult;
use daft_core::{
    lit::Literal,
    prelude::{DataType, Field, Series},
};
pub use scalar::ScalarColumn;

/// A column within a [`RecordBatch`][crate::RecordBatch].
///
/// Can be either a fully materialized [`Series`] or a [`ScalarColumn`] that
/// repeats a single [`Literal`] value for a given length.  Scalar columns are
/// O(1) in memory regardless of the logical row count and are only expanded
/// into a full array when an operation actually requires element-wise access
/// (via [`as_materialized_series`][Column::as_materialized_series]).
///
/// Row-preserving operations such as [`slice`][Column::slice],
/// [`filter`][Column::filter], and [`take`][Column::take] keep scalar columns
/// compact — they simply adjust the logical length without allocating.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Column {
    Series(Series),
    Scalar(ScalarColumn),
}

impl Column {
    /// Returns the [`DataType`] of the column.
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Series(s) => s.data_type(),
            Self::Scalar(s) => s.data_type(),
        }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        match self {
            Self::Series(s) => s.name(),
            Self::Scalar(s) => s.name(),
        }
    }

    /// Returns a [`Field`] describing the column's name and type.
    pub fn field(&self) -> Field {
        match self {
            Self::Series(s) => s.field().clone(),
            Self::Scalar(s) => Field::new(s.name(), s.data_type().clone()),
        }
    }

    /// Returns the logical number of rows in the column.
    pub fn len(&self) -> usize {
        match self {
            Self::Series(s) => s.len(),
            Self::Scalar(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to a materialized [`Series`].
    ///
    /// For [`Column::Series`] this is a no-op.  For [`Column::Scalar`] the
    /// underlying [`Series`] is lazily constructed on the first call and cached
    /// for subsequent access via [`OnceLock`][std::sync::OnceLock].
    pub fn as_materialized_series(&self) -> &Series {
        match self {
            Self::Series(s) => s,
            Self::Scalar(s) => s.as_materialized_series(),
        }
    }

    /// Consumes the column and returns the materialized [`Series`].
    ///
    /// If the scalar was already materialized the cached value is returned
    /// without re-allocating; otherwise it is constructed on the fly.
    pub fn take_materialized_series(self) -> Series {
        match self {
            Self::Series(s) => s,
            Self::Scalar(s) => s.take_materialized_series(),
        }
    }

    /// Renames the column in place.
    pub fn rename(&mut self, name: &str) {
        match self {
            Self::Series(s) => {
                *s = s.rename(name);
            }
            Self::Scalar(s) => s.rename(name),
        }
    }

    /// Returns a new column with the given name.
    pub fn with_name(mut self, name: &str) -> Self {
        self.rename(name);
        self
    }

    /// Casts the column to the given [`DataType`].
    ///
    /// Scalar columns are cast without materialization by casting the
    /// underlying [`Literal`] value directly.
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Self> {
        match self {
            Self::Series(s) => s.cast(dtype).map(Self::Series),
            Self::Scalar(s) => s.cast(dtype).map(Self::Scalar),
        }
    }

    /// Returns the approximate in-memory size of the column in bytes.
    ///
    /// Scalar columns report only the size of the stored scalar, not the
    /// expanded array size.
    pub fn size_bytes(&self) -> usize {
        match self {
            Self::Series(s) => s.size_bytes(),
            Self::Scalar(s) => s.size_bytes(),
        }
    }

    /// Converts logical types to their physical representation.
    ///
    /// This is a no-op when the physical type already matches the logical type.
    /// Scalar columns are cast without materialization.
    pub fn as_physical(&self) -> DaftResult<Self> {
        let physical_dtype = self.data_type().to_physical();
        if &physical_dtype == self.data_type() {
            Ok(self.clone())
        } else {
            self.cast(&physical_dtype)
        }
    }

    /// Broadcasts the column to the given length.
    ///
    /// Scalar columns simply adjust their logical length (O(1)).  Series
    /// columns delegate to [`Series::broadcast`].
    pub fn broadcast(&self, length: usize) -> DaftResult<Self> {
        match self {
            Self::Series(s) => {
                if s.len() == length {
                    Ok(Self::Series(s.clone()))
                } else {
                    s.broadcast(length).map(Self::Series)
                }
            }
            Self::Scalar(s) => Ok(Self::Scalar(s.resize(length))),
        }
    }

    /// Returns a sub-range `[start, end)` of the column.
    ///
    /// Scalar columns adjust their logical length without materializing.
    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        match self {
            Self::Series(s) => s.slice(start, end).map(Self::Series),
            Self::Scalar(s) => {
                let clamped_end = end.min(s.len());
                let new_len = clamped_end.saturating_sub(start);
                Ok(Self::Scalar(s.resize(new_len)))
            }
        }
    }

    /// Returns the first `num` rows of the column.
    ///
    /// Scalar columns adjust their logical length without materializing.
    pub fn head(&self, num: usize) -> DaftResult<Self> {
        match self {
            Self::Series(s) => s.head(num).map(Self::Series),
            Self::Scalar(s) => Ok(Self::Scalar(s.resize(s.len().min(num)))),
        }
    }

    /// Filters the column by a boolean mask.
    ///
    /// Scalar columns count the number of `true` values in the mask and
    /// adjust their logical length accordingly, without materializing.
    pub fn filter(&self, mask: &daft_core::prelude::BooleanArray) -> DaftResult<Self> {
        match self {
            Self::Series(s) => s.filter(mask).map(Self::Series),
            Self::Scalar(s) => {
                let true_count = mask.into_iter().flatten().filter(|b| *b).count();
                Ok(Self::Scalar(s.resize(true_count)))
            }
        }
    }

    /// Gathers rows by index.
    ///
    /// Scalar columns adjust their logical length to `idx.len()` without
    /// materializing.
    pub fn take(&self, idx: &daft_core::prelude::UInt64Array) -> DaftResult<Self> {
        match self {
            Self::Series(s) => s.take(idx).map(Self::Series),
            Self::Scalar(s) => Ok(Self::Scalar(s.resize(idx.len()))),
        }
    }

    /// Concatenates multiple columns into one.
    ///
    /// If every column is a scalar with the same value, the result is a single
    /// scalar with the combined length.  Otherwise all columns are materialized
    /// and concatenated via [`Series::concat`].
    pub fn concat(columns: &[&Self]) -> DaftResult<Self> {
        if columns.is_empty() {
            return Err(common_error::DaftError::ValueError(
                "Need at least 1 Column to concat".to_string(),
            ));
        }

        if let Some(Self::Scalar(first_scalar)) = columns.first()
            && columns
                .iter()
                .all(|c| matches!(c, Self::Scalar(s) if s.scalar() == first_scalar.scalar()))
        {
            let total_len: usize = columns.iter().map(|c| c.len()).sum();
            return Ok(Self::Scalar(ScalarColumn::new(
                Arc::from(first_scalar.name()),
                first_scalar.data_type().clone(),
                first_scalar.scalar().clone(),
                total_len,
            )));
        }

        let series: Vec<&Series> = columns.iter().map(|c| c.as_materialized_series()).collect();
        Series::concat(series.as_slice()).map(Self::Series)
    }

    /// Returns `true` if this column is a [`ScalarColumn`].
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(..))
    }

    /// Creates a new scalar column from a [`Literal`] value.
    pub fn new_scalar(name: &str, dtype: DataType, value: Literal, length: usize) -> Self {
        Self::Scalar(ScalarColumn::new(Arc::from(name), dtype, value, length))
    }

    /// Returns `true` if the value at `idx` is non-null.
    ///
    /// For scalar columns, this is `true` when the underlying scalar is
    /// non-null, regardless of `idx`.
    pub fn is_valid(&self, idx: usize) -> bool {
        match self {
            Self::Series(s) => s.inner.nulls().is_none_or(|v| v.is_valid(idx)),
            Self::Scalar(s) => !s.has_nulls(),
        }
    }

    /// Returns the display string for the value at `idx`.
    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if idx >= self.len() {
            return Err(common_error::DaftError::ValueError(format!(
                "index {} out of bounds for Column of length {}",
                idx,
                self.len()
            )));
        }
        match self {
            Self::Series(s) => Ok(s.str_value(idx)),
            Self::Scalar(s) => Ok(s.resize(1).as_materialized_series().str_value(0)),
        }
    }

    pub fn get_lit(&self, idx: usize) -> Literal {
        match self {
            Self::Series(s) => s.get_lit(idx),
            Self::Scalar(s) => s.scalar().clone(),
        }
    }

    pub fn null_count(&self) -> usize {
        match self {
            Self::Series(s) => s.null_count(),
            Self::Scalar(s) => s.null_count(),
        }
    }
}

impl From<Series> for Column {
    fn from(s: Series) -> Self {
        if s.len() == 1 {
            Self::Scalar(ScalarColumn::new(
                Arc::from(s.name()),
                s.data_type().clone(),
                s.get_lit(0),
                1,
            ))
        } else {
            Self::Series(s)
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        if self.name() != other.name()
            || self.data_type() != other.data_type()
            || self.len() != other.len()
        {
            return false;
        }
        match (self, other) {
            (Self::Scalar(a), Self::Scalar(b)) => a.scalar() == b.scalar(),
            _ => self.as_materialized_series() == other.as_materialized_series(),
        }
    }
}

impl Eq for Column {}

impl Hash for Column {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s = self.as_materialized_series();
        let hashes = s.hash(None).expect("Failed to hash Column");
        for h in &hashes {
            h.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{lit::Literal, prelude::*, series::IntoSeries};

    use super::{Column, ScalarColumn};

    fn make_int_series(name: &str, vals: Vec<i64>) -> Series {
        Int64Array::from_vec(name, vals).into_series()
    }

    // ---- ScalarColumn tests ----

    #[test]
    fn scalar_column_basics() {
        let sc = ScalarColumn::new(Arc::from("x"), DataType::Int64, Literal::Int64(42), 100);
        assert_eq!(sc.name(), "x");
        assert_eq!(sc.data_type(), &DataType::Int64);
        assert_eq!(sc.len(), 100);
        assert!(!sc.is_empty());
        assert!(!sc.has_nulls());
        assert_eq!(sc.scalar(), &Literal::Int64(42));
    }

    #[test]
    fn scalar_column_null() {
        let sc = ScalarColumn::new(Arc::from("n"), DataType::Utf8, Literal::Null, 10);
        assert!(sc.has_nulls());
    }

    #[test]
    fn scalar_column_empty() {
        let sc = ScalarColumn::new_empty(Arc::from("e"), DataType::Float64);
        assert_eq!(sc.len(), 0);
        assert!(sc.is_empty());
        assert!(!sc.has_nulls());
    }

    #[test]
    fn scalar_column_materialize() {
        let sc = ScalarColumn::new(Arc::from("v"), DataType::Int64, Literal::Int64(7), 3);
        let s = sc.as_materialized_series();
        assert_eq!(s.len(), 3);
        assert_eq!(s.name(), "v");
        assert_eq!(s.i64().unwrap().get(0).unwrap(), 7);
        assert_eq!(s.i64().unwrap().get(2).unwrap(), 7);
    }

    #[test]
    fn scalar_column_materialize_cached() {
        let sc = ScalarColumn::new(Arc::from("c"), DataType::Int64, Literal::Int64(1), 5);
        let ptr1 = sc.as_materialized_series() as *const Series;
        let ptr2 = sc.as_materialized_series() as *const Series;
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn scalar_column_resize() {
        let sc = ScalarColumn::new(Arc::from("r"), DataType::Int64, Literal::Int64(9), 10);
        let resized = sc.resize(3);
        assert_eq!(resized.len(), 3);
        assert_eq!(resized.scalar(), &Literal::Int64(9));
    }

    #[test]
    fn scalar_column_resize_same_length() {
        let sc = ScalarColumn::new(Arc::from("r"), DataType::Int64, Literal::Int64(9), 10);
        let resized = sc.resize(10);
        assert_eq!(resized.len(), 10);
    }

    #[test]
    fn scalar_column_cast() -> DaftResult<()> {
        let sc = ScalarColumn::new(Arc::from("c"), DataType::Int64, Literal::Int64(42), 5);
        let casted = sc.cast(&DataType::Float64)?;
        assert_eq!(casted.data_type(), &DataType::Float64);
        assert_eq!(casted.len(), 5);
        let s = casted.as_materialized_series();
        assert_eq!(s.f64().unwrap().get(0).unwrap(), 42.0);
        Ok(())
    }

    #[test]
    fn scalar_column_rename() {
        let mut sc = ScalarColumn::new(Arc::from("old"), DataType::Int64, Literal::Int64(1), 2);
        sc.rename("new");
        assert_eq!(sc.name(), "new");
        assert_eq!(sc.as_materialized_series().name(), "new");
    }

    #[test]
    fn scalar_column_from_single_value_series() -> DaftResult<()> {
        let s = make_int_series("s", vec![99]);
        let sc = ScalarColumn::from_single_value_series(&s, 50)?;
        assert_eq!(sc.len(), 50);
        assert_eq!(sc.scalar(), &Literal::Int64(99));
        Ok(())
    }

    // ---- Column tests ----

    #[test]
    fn column_from_series() {
        let s = make_int_series("a", vec![1, 2, 3]);
        let col = Column::from(s);
        assert!(!col.is_scalar());
        assert_eq!(col.len(), 3);
        assert_eq!(col.name(), "a");
        assert_eq!(col.data_type(), &DataType::Int64);
    }

    #[test]
    fn column_new_scalar() {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(42), 100);
        assert!(col.is_scalar());
        assert_eq!(col.len(), 100);
        assert_eq!(col.name(), "x");
    }

    #[test]
    fn column_scalar_materialize() {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(5), 3);
        let s = col.as_materialized_series();
        assert_eq!(s.len(), 3);
        assert_eq!(s.i64().unwrap().get(1).unwrap(), 5);
    }

    #[test]
    fn column_slice_scalar_stays_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 100);
        let sliced = col.slice(10, 30)?;
        assert!(sliced.is_scalar());
        assert_eq!(sliced.len(), 20);
        Ok(())
    }

    #[test]
    fn column_slice_series() -> DaftResult<()> {
        let col = Column::from(make_int_series("a", vec![10, 20, 30, 40, 50]));
        let sliced = col.slice(1, 4)?;
        assert!(!sliced.is_scalar());
        assert_eq!(sliced.len(), 3);
        Ok(())
    }

    #[test]
    fn column_filter_scalar_stays_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(7), 5);
        let mask = BooleanArray::from_iter(
            "mask",
            vec![Some(true), Some(false), Some(true), Some(false), Some(true)].into_iter(),
        );
        let filtered = col.filter(&mask)?;
        assert!(filtered.is_scalar());
        assert_eq!(filtered.len(), 3);
        Ok(())
    }

    #[test]
    fn column_take_scalar_stays_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(3), 100);
        let idx = UInt64Array::from_vec("idx", vec![0, 5, 10]);
        let taken = col.take(&idx)?;
        assert!(taken.is_scalar());
        assert_eq!(taken.len(), 3);
        Ok(())
    }

    #[test]
    fn column_broadcast_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 1);
        let broadcasted = col.broadcast(1000)?;
        assert!(broadcasted.is_scalar());
        assert_eq!(broadcasted.len(), 1000);
        Ok(())
    }

    #[test]
    fn column_head_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 100);
        let headed = col.head(10)?;
        assert!(headed.is_scalar());
        assert_eq!(headed.len(), 10);
        Ok(())
    }

    #[test]
    fn column_cast_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(42), 10);
        let casted = col.cast(&DataType::Float64)?;
        assert!(casted.is_scalar());
        assert_eq!(casted.data_type(), &DataType::Float64);
        assert_eq!(casted.len(), 10);
        Ok(())
    }

    #[test]
    fn column_as_physical_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Date, Literal::Date(1), 5);
        let phys = col.as_physical()?;
        assert!(phys.is_scalar());
        assert_eq!(phys.data_type(), &DataType::Int32);
        assert_eq!(phys.len(), 5);
        Ok(())
    }

    #[test]
    fn column_concat_same_scalars() -> DaftResult<()> {
        let a = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 10);
        let b = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 20);
        let c = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 30);
        let result = Column::concat(&[&a, &b, &c])?;
        assert!(result.is_scalar());
        assert_eq!(result.len(), 60);
        Ok(())
    }

    #[test]
    fn column_concat_different_scalars_materializes() -> DaftResult<()> {
        let a = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 2);
        let b = Column::new_scalar("x", DataType::Int64, Literal::Int64(2), 3);
        let result = Column::concat(&[&a, &b])?;
        assert!(!result.is_scalar());
        assert_eq!(result.len(), 5);
        Ok(())
    }

    #[test]
    fn column_concat_mixed_series_and_scalar() -> DaftResult<()> {
        let a = Column::from(make_int_series("x", vec![1, 2]));
        let b = Column::new_scalar("x", DataType::Int64, Literal::Int64(3), 2);
        let result = Column::concat(&[&a, &b])?;
        assert!(!result.is_scalar());
        assert_eq!(result.len(), 4);
        let s = result.as_materialized_series();
        assert_eq!(s.i64().unwrap().get(2).unwrap(), 3);
        assert_eq!(s.i64().unwrap().get(3).unwrap(), 3);
        Ok(())
    }

    #[test]
    fn column_size_bytes_scalar_is_small() {
        let scalar = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 1_000_000);
        let series = Column::from(make_int_series("x", vec![1; 1_000_000]));
        assert!(scalar.size_bytes() < series.size_bytes());
    }

    #[test]
    fn column_eq_scalar_scalar() {
        let a = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 10);
        let b = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 10);
        let c = Column::new_scalar("x", DataType::Int64, Literal::Int64(2), 10);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn column_eq_different_lengths() {
        let a = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 10);
        let b = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 20);
        assert_ne!(a, b);
    }

    #[test]
    fn column_is_valid_scalar() {
        let valid = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 5);
        assert!(valid.is_valid(0));
        assert!(valid.is_valid(4));

        let null = Column::new_scalar("x", DataType::Int64, Literal::Null, 5);
        assert!(!null.is_valid(0));
    }

    #[test]
    fn column_is_valid_series() {
        let s = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![Some(1), None, Some(3)].into_iter(),
        )
        .into_series();
        let col = Column::from(s);
        assert!(col.is_valid(0));
        assert!(!col.is_valid(1));
        assert!(col.is_valid(2));
    }

    #[test]
    fn column_with_name() {
        let col = Column::new_scalar("old", DataType::Int64, Literal::Int64(1), 5);
        let renamed = col.with_name("new");
        assert_eq!(renamed.name(), "new");
    }

    #[test]
    fn column_slice_scalar_end_past_length() -> DaftResult<()> {
        // len=25, slice(10, 30) should give 15 rows (rows 10..25), not 20
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 25);
        let sliced = col.slice(10, 30)?;
        assert!(sliced.is_scalar());
        assert_eq!(sliced.len(), 15);
        Ok(())
    }

    #[test]
    fn column_slice_scalar_start_past_length() -> DaftResult<()> {
        // len=5, slice(10, 20) should give 0 rows
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 5);
        let sliced = col.slice(10, 20)?;
        assert!(sliced.is_scalar());
        assert_eq!(sliced.len(), 0);
        Ok(())
    }

    #[test]
    fn column_str_value_scalar_bounds_check() {
        // str_value on a scalar with idx >= length should fail, not silently succeed
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(42), 5);
        assert!(col.str_value(10).is_err());
    }

    #[test]
    fn scalar_column_null_preserves_dtype() {
        // A null ScalarColumn with dtype Int64 should materialize as Int64, not Null
        let sc = ScalarColumn::new(Arc::from("x"), DataType::Int64, Literal::Null, 3);
        let s = sc.as_materialized_series();
        assert_eq!(s.data_type(), &DataType::Int64);
        assert_eq!(s.len(), 3);
        assert_eq!(s.null_count(), 3);
    }

    #[test]
    fn scalar_column_from_single_value_series_preserves_dtype() -> DaftResult<()> {
        // A null Series with Int64 dtype should produce a ScalarColumn with Int64 dtype
        let s = Int64Array::from_iter(
            Field::new("x", DataType::Int64),
            vec![None::<i64>].into_iter(),
        )
        .into_series();
        assert_eq!(s.data_type(), &DataType::Int64);
        let sc = ScalarColumn::from_single_value_series(&s, 10)?;
        assert_eq!(sc.data_type(), &DataType::Int64);
        Ok(())
    }

    #[test]
    fn column_is_valid_scalar_no_bounds_check() {
        // is_valid on scalar doesn't bounds-check (it's the same value everywhere),
        // but verify it returns correct values
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 5);
        assert!(col.is_valid(0));
        // idx past length — for scalar this is still "valid" since the value is non-null
        // (debatable whether this should panic, but documenting current behavior)
        assert!(col.is_valid(100));
    }

    #[test]
    fn column_head_past_length() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 5);
        let headed = col.head(100)?;
        assert!(headed.is_scalar());
        assert_eq!(headed.len(), 5);
        Ok(())
    }

    #[test]
    fn column_take_null_indices() -> DaftResult<()> {
        // take with null indices on a scalar — should still produce the right length
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(42), 100);
        let idx = UInt64Array::from_iter(
            Field::new("idx", DataType::UInt64),
            vec![Some(0u64), None, Some(2)].into_iter(),
        );
        let taken = col.take(&idx)?;
        assert!(taken.is_scalar());
        assert_eq!(taken.len(), 3);
        Ok(())
    }

    #[test]
    fn column_filter_all_false_scalar() -> DaftResult<()> {
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 3);
        let mask = BooleanArray::from_iter(
            "mask",
            vec![Some(false), Some(false), Some(false)].into_iter(),
        );
        let filtered = col.filter(&mask)?;
        assert!(filtered.is_scalar());
        assert_eq!(filtered.len(), 0);
        Ok(())
    }

    #[test]
    fn column_filter_with_nulls_in_mask() -> DaftResult<()> {
        // null in mask should be treated as false
        let col = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 4);
        let mask =
            BooleanArray::from_iter("mask", vec![Some(true), None, Some(true), None].into_iter());
        let filtered = col.filter(&mask)?;
        assert!(filtered.is_scalar());
        assert_eq!(filtered.len(), 2);
        Ok(())
    }

    #[test]
    fn column_concat_scalar_simplified_match() -> DaftResult<()> {
        // Ensure concat with a single scalar works (exercises the match path)
        let a = Column::new_scalar("x", DataType::Int64, Literal::Int64(1), 10);
        let result = Column::concat(&[&a])?;
        assert!(result.is_scalar());
        assert_eq!(result.len(), 10);
        Ok(())
    }

    #[test]
    fn column_hash_eq_contract() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash as _, Hasher as _},
        };

        fn hash_column(col: &Column) -> u64 {
            let mut hasher = DefaultHasher::new();
            col.hash(&mut hasher);
            hasher.finish()
        }

        let scalar = Column::new_scalar("x", DataType::Int64, Literal::Int64(42), 3);
        let series = Column::from(make_int_series("x", vec![42, 42, 42]));

        // If they're equal, their hashes must match
        assert_eq!(scalar, series);
        assert_eq!(hash_column(&scalar), hash_column(&series));
    }
}
