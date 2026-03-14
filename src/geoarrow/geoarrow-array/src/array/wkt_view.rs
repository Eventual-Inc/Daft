use std::{str::FromStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, OffsetSizeTrait, StringViewArray, builder::StringViewBuilder, cast::AsArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    GeoArrowType, Metadata, WktType,
    error::{GeoArrowError, GeoArrowResult},
};
use wkt::Wkt;

use crate::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, array::GenericWktArray};

/// An immutable array of WKT geometries.
///
/// This is stored as an Arrow [`StringViewArray`] and is semantically equivalent to
/// `Vec<Option<Wkt>>` due to the internal validity bitmap.
///
/// Refer to [`crate::cast`] for converting this array to other GeoArrow array types.
#[derive(Debug, Clone, PartialEq)]
pub struct WktViewArray {
    pub(crate) data_type: WktType,
    pub(crate) array: StringViewArray,
}

impl WktViewArray {
    /// Create a new WktViewArray from a StringViewArray
    pub fn new(array: StringViewArray, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: WktType::new(metadata),
            array,
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access the underlying string array.
    pub fn inner(&self) -> &StringViewArray {
        &self.array
    }

    /// Slice this [`WktViewArray`].
    ///
    ///
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            array: self.array.slice(offset, length),
            data_type: self.data_type.clone(),
        }
    }

    /// Replace the [Metadata] in the array with the given metadata
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl GeoArrowArray for WktViewArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.array.logical_nulls()
    }

    #[inline]
    fn logical_null_count(&self) -> usize {
        self.array.logical_null_count()
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::WktView(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(Self::with_metadata(&self, metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for WktViewArray {
    type Item = Wkt<f64>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        let s = unsafe { self.array.value_unchecked(index) };
        Wkt::from_str(s).map_err(|err| GeoArrowError::Wkt(err.to_string()))
    }
}

impl IntoArrow for WktViewArray {
    type ArrowArray = StringViewArray;
    type ExtensionType = WktType;

    fn into_arrow(self) -> Self::ArrowArray {
        self.array
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl From<(StringViewArray, WktType)> for WktViewArray {
    fn from((value, typ): (StringViewArray, WktType)) -> Self {
        Self {
            data_type: typ,
            array: value,
        }
    }
}

impl TryFrom<(&dyn Array, WktType)> for WktViewArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::Utf8View => Ok((value.as_string_view().clone(), typ).into()),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected WktView DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WktViewArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field
            .try_extension_type::<WktType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> From<GenericWktArray<O>> for WktViewArray {
    fn from(value: GenericWktArray<O>) -> Self {
        let wkb_type = value.data_type;
        let binary_view_array = value.array;

        // Copy the bytes from the binary view array into a new byte array
        let mut builder = StringViewBuilder::new();
        binary_view_array
            .iter()
            .for_each(|value| builder.append_option(value));

        Self {
            data_type: wkb_type,
            array: builder.finish(),
        }
    }
}
