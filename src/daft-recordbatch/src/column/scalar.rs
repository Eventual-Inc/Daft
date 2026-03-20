use std::sync::{Arc, OnceLock};

use common_error::DaftResult;
use daft_core::{
    lit::Literal,
    prelude::{DataType, Series},
};

/// A column that stores a single [`Literal`] value repeated for a given length.
///
/// This is O(1) in memory regardless of the logical row count.  The full
/// [`Series`] is only constructed when [`as_materialized_series`][Self::as_materialized_series]
/// is called, and the result is cached in a [`OnceLock`] for subsequent access.
///
/// Operations like [`resize`][Self::resize] and [`cast`][Self::cast] produce
/// new `ScalarColumn`s without ever touching the materialized cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScalarColumn {
    name: Arc<str>,
    scalar: Literal,
    dtype: DataType,
    length: usize,
    #[serde(skip)]
    materialized: OnceLock<Series>,
}

impl ScalarColumn {
    /// Creates a new scalar column.
    pub fn new(name: Arc<str>, dtype: DataType, scalar: Literal, length: usize) -> Self {
        Self {
            name,
            scalar,
            dtype,
            length,
            materialized: OnceLock::new(),
        }
    }

    /// Creates an empty scalar column (length 0) with a null value.
    pub fn new_empty(name: Arc<str>, dtype: DataType) -> Self {
        Self::new(name, dtype, Literal::Null, 0)
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the data type.
    pub fn data_type(&self) -> &DataType {
        &self.dtype
    }

    /// Returns a reference to the underlying scalar value.
    pub fn scalar(&self) -> &Literal {
        &self.scalar
    }

    /// Returns the logical number of rows.
    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn _to_series(name: &str, scalar: &Literal, dtype: &DataType, length: usize) -> Series {
        if length == 0 {
            Series::empty(name, dtype)
        } else if matches!(scalar, Literal::Null) {
            Series::full_null(name, dtype, length)
        } else {
            let s: Series = scalar.clone().into();
            s.rename(name).broadcast(length).expect("broadcast scalar")
        }
    }

    fn to_series(&self) -> Series {
        Self::_to_series(&self.name, &self.scalar, &self.dtype, self.length)
    }

    /// Returns a reference to a materialized [`Series`], constructing and
    /// caching it on the first call.
    pub fn as_materialized_series(&self) -> &Series {
        self.materialized.get_or_init(|| self.to_series())
    }

    /// Consumes the scalar column and returns the materialized [`Series`].
    ///
    /// If the series was already cached it is returned without re-allocating.
    pub fn take_materialized_series(self) -> Series {
        self.materialized
            .into_inner()
            .unwrap_or_else(|| Self::_to_series(&self.name, &self.scalar, &self.dtype, self.length))
    }

    /// Returns a new scalar column with the given length.
    ///
    /// Returns `self` unchanged if the length already matches.
    /// The materialized cache is not carried over.
    pub fn resize(&self, length: usize) -> Self {
        if self.length == length {
            return self.clone();
        }
        Self::new(
            self.name.clone(),
            self.dtype.clone(),
            self.scalar.clone(),
            length,
        )
    }

    /// Renames the column in place, updating the cached series if present.
    pub fn rename(&mut self, name: &str) {
        if let Some(series) = self.materialized.get_mut() {
            *series = series.rename(name);
        }
        self.name = Arc::from(name);
    }

    /// Returns `true` if the scalar is null and the length is non-zero.
    pub fn has_nulls(&self) -> bool {
        self.length != 0 && matches!(self.scalar, Literal::Null)
    }
    pub fn null_count(&self) -> usize {
        if self.has_nulls() { self.length } else { 0 }
    }

    /// Returns the approximate in-memory size in bytes (scalar storage only,
    /// not the expanded array).
    pub fn size_bytes(&self) -> usize {
        self.name.len()
            + std::mem::size_of::<DataType>()
            + self.scalar.size_bytes()
            + std::mem::size_of::<usize>()
    }

    /// Casts the scalar to the given data type without materialization.
    pub fn cast(&self, dtype: &DataType) -> DaftResult<Self> {
        let casted = self.scalar.clone().cast(dtype)?;
        Ok(Self::new(
            self.name.clone(),
            dtype.clone(),
            casted,
            self.length,
        ))
    }

    /// Creates a `ScalarColumn` from a length-1 [`Series`] expanded to `length` rows.
    pub fn from_single_value_series(series: &Series, length: usize) -> DaftResult<Self> {
        let lit = Literal::try_from_single_value_series(series)?;
        Ok(Self::new(
            Arc::from(series.name()),
            series.data_type().clone(),
            lit,
            length,
        ))
    }
}
