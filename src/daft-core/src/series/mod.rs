mod array_impl;
mod from;
mod ops;
mod serdes;
mod series_like;
mod utils;
use std::{ops::Sub, sync::Arc};

pub use array_impl::IntoSeries;
use common_display::table_display::{make_comfy_table, StrValue};
use common_error::DaftResult;
use derive_more::Display;
use indexmap::{map::RawEntryApiV1, IndexMap};
pub use ops::cast_series_to_supertype;

pub(crate) use self::series_like::SeriesLike;
use crate::{
    array::{
        ops::{
            arrow2::comparison::build_is_equal, from_arrow::FromArrow, full::FullNull, DaftCompare,
        },
        DataArray,
    },
    datatypes::{DaftDataType, DaftNumericType, DataType, Field, FieldRef, NumericNative},
    prelude::AsArrow,
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
    with_match_daft_types,
};

#[derive(Clone, Debug, Display)]
#[display("{}\n", self.to_comfy_table())]
pub struct Series {
    pub inner: Arc<dyn SeriesLike>,
}

impl PartialEq for Series {
    fn eq(&self, other: &Self) -> bool {
        match self.equal(other) {
            Ok(arr) => arr.into_iter().all(|x| x.unwrap_or(false)),
            Err(_) => false,
        }
    }
}

impl Series {
    /// Build a hashset of the [`IndexHash`]s of each element in this [`Series`].
    ///
    /// The returned hashset can be used to probe for the existence of a given element in this [`Series`].
    /// Its length can also be used to determine the *exact* number of unique elements in this [`Series`].
    ///
    /// # Note
    /// 1. This function returns an `IndexMap<X, ()>` rather than a `HashSet<X>`. These two types are functionally equivalent.
    ///
    /// 2. `NULL`s are *not* inserted into the returned hashset. They won't be counted towards the final number of unique elements.
    pub fn build_probe_table_without_nulls(
        &self,
    ) -> DaftResult<IndexMap<IndexHash, (), IdentityBuildHasher>> {
        // Building a comparator function over a series of type `NULL` will result in a failure.
        // (I.e., `let comparator = build_is_equal(..)` will fail).
        //
        // Therefore, exit early with an empty hashmap.
        if matches!(self.data_type(), DataType::Null) {
            return Ok(IndexMap::default());
        }

        const DEFAULT_SIZE: usize = 20;
        let hashed_series = self.hash_with_validity(None)?;
        let array = self.to_arrow();
        let comparator = build_is_equal(&*array, &*array, true, false)?;

        let mut probe_table =
            IndexMap::<IndexHash, (), IdentityBuildHasher>::with_capacity_and_hasher(
                DEFAULT_SIZE,
                Default::default(),
            );

        for (idx, hash) in hashed_series.as_arrow().iter().enumerate() {
            let hash = match hash {
                Some(&hash) => hash,
                None => continue,
            };
            let entry = probe_table.raw_entry_v1().from_hash(hash, |other| {
                (hash == other.hash) && comparator(idx, other.idx as _)
            });
            if entry.is_none() {
                probe_table.insert(
                    IndexHash {
                        idx: idx as u64,
                        hash,
                    },
                    (),
                );
            }
        }

        Ok(probe_table)
    }

    /// Exports this Series into an Arrow arrow that is corrected for the Arrow type system.
    /// For example, Daft's TimestampArray is a logical type that is backed by an Int64Array Physical array.
    /// If we were to call `.as_arrow()` or `.physical`on the TimestampArray, we would get an Int64Array that represented the time units.
    /// However if we want to export our Timestamp array to another arrow system like arrow2 kernels or python, duckdb or more.
    /// We should convert it back to the canonical arrow dtype of Timestamp rather than Int64.
    /// To get the internal physical type without conversion, see `as_arrow()`.
    pub fn to_arrow(&self) -> Box<dyn arrow2::array::Array> {
        self.inner.to_arrow()
    }

    /// Creates a Series given an Arrow [`arrow2::array::Array`]
    ///
    /// TODO chore: consider accepting Into<FieldRef>
    ///
    /// This function will check the provided [`Field`] (and all its associated potentially nested fields/dtypes) against
    /// the provided [`arrow2::array::Array`] for compatibility, and returns an error if they do not match.
    pub fn from_arrow(
        field: FieldRef,
        arrow_arr: Box<dyn arrow2::array::Array>,
    ) -> DaftResult<Self> {
        with_match_daft_types!(field.dtype, |$T| {
            Ok(<<$T as DaftDataType>::ArrayType as FromArrow>::from_arrow(field, arrow_arr)?.into_series())
        })
    }

    /// Creates a Series that is all nulls
    pub fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        with_match_daft_types!(dtype, |$T| {
            <<$T as DaftDataType>::ArrayType as FullNull>::full_null(name, dtype, length).into_series()
        })
    }

    /// Creates an empty [`Series`]
    pub fn empty(field_name: &str, dtype: &DataType) -> Self {
        with_match_daft_types!(dtype, |$T| {
            <<$T as DaftDataType>::ArrayType as FullNull>::empty(field_name, dtype).into_series()
        })
    }

    pub fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    pub fn to_floating_data_type(&self) -> DaftResult<DataType> {
        self.inner.data_type().to_floating_representation()
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    #[must_use]
    pub fn rename<S: AsRef<str>>(&self, name: S) -> Self {
        self.inner.rename(name.as_ref())
    }

    pub fn field(&self) -> &Field {
        self.inner.field()
    }
    pub fn as_physical(&self) -> DaftResult<Self> {
        let physical_dtype = self.data_type().to_physical();
        if &physical_dtype == self.data_type() {
            Ok(self.clone())
        } else {
            self.inner.cast(&physical_dtype)
        }
    }

    pub fn to_comfy_table(&self) -> comfy_table::Table {
        let field = self.field();
        let field_disp = format!("{}\n---\n{}", field.name, field.dtype);

        make_comfy_table(
            [field_disp].as_slice(),
            Some([self as &dyn StrValue].as_slice()),
            Some(self.len()),
            Some(80),
        )
    }

    pub fn with_validity(&self, validity: Option<arrow2::bitmap::Bitmap>) -> DaftResult<Self> {
        self.inner.with_validity(validity)
    }

    pub fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        self.inner.validity()
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        let Some(validity) = self.validity() else {
            return true;
        };
        validity.get_bit(idx)
    }

    /// Attempts to downcast the Series to a primitive slice
    /// This will return an error if the Series is not of the physical type `T`
    /// # Example
    /// ```rust,no_run
    /// let i32_arr: &[i32] = series.try_as_slice::<i32>()?;
    ///
    /// let f64_arr: &[f64] = series.try_as_slice::<f64>()?;
    /// ```
    pub fn try_as_slice<N: NumericNative>(
        &self,
    ) -> DaftResult<&[<N::DAFTTYPE as DaftNumericType>::Native]>
    where
        N::DAFTTYPE: DaftNumericType + DaftDataType,
    {
        let data: &DataArray<N::DAFTTYPE> = self.downcast()?;
        Ok(data.as_slice())
    }

    /// Helper function to check that two series of floats are within some `epsilon` of each other.
    pub fn fuzzy_eq(&self, other: &Self, epsilon: &Self) -> bool {
        if self.data_type() != other.data_type() {
            return false;
        }
        match self.data_type() {
            DataType::Float32 | DataType::Float64 => {
                let diff = self
                    .sub(other)
                    .expect("Failed to subtract one series from the other")
                    .abs()
                    .expect("Failed to get absolute difference between the two given series");
                match diff.lte(epsilon) {
                    Ok(arr) => arr.into_iter().all(|x| x.unwrap_or(false)),
                    Err(_) => false,
                }
            }
            _ => self == other,
        }
    }
}
