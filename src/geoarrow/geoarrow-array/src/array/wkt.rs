use std::{str::FromStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray,
    builder::GenericStringBuilder, cast::AsArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    GeoArrowType, Metadata, WktType,
    error::{GeoArrowError, GeoArrowResult},
};
use wkt::Wkt;

use crate::{
    GeoArrowArrayAccessor,
    array::WktViewArray,
    trait_::{GeoArrowArray, IntoArrow},
    util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32},
};

/// An immutable array of WKT geometries using GeoArrow's in-memory representation.
///
/// This is a wrapper around an Arrow [GenericStringArray] and is semantically equivalent to
/// `Vec<Option<WKT>>` due to the internal validity bitmap.
///
/// Refer to [`crate::cast`] for converting this array to other GeoArrow array types.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericWktArray<O: OffsetSizeTrait> {
    pub(crate) data_type: WktType,
    pub(crate) array: GenericStringArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> GenericWktArray<O> {
    /// Create a new GenericWktArray from a StringArray
    pub fn new(array: GenericStringArray<O>, metadata: Arc<Metadata>) -> Self {
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
    pub fn inner(&self) -> &GenericStringArray<O> {
        &self.array
    }

    /// Slice this [`GenericWktArray`].
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

    /// Replace the [`Metadata`] contained in this array.
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl<O: OffsetSizeTrait> GeoArrowArray for GenericWktArray<O> {
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
        if O::IS_LARGE {
            GeoArrowType::LargeWkt(self.data_type.clone())
        } else {
            GeoArrowType::Wkt(self.data_type.clone())
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(Self::with_metadata(&self, metadata))
    }
}

impl<'a, O: OffsetSizeTrait> GeoArrowArrayAccessor<'a> for GenericWktArray<O> {
    type Item = Wkt<f64>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        let s = unsafe { self.array.value_unchecked(index) };
        Wkt::from_str(s).map_err(|err| GeoArrowError::Wkt(err.to_string()))
    }
}

impl<O: OffsetSizeTrait> IntoArrow for GenericWktArray<O> {
    type ArrowArray = GenericStringArray<O>;
    type ExtensionType = WktType;

    fn into_arrow(self) -> Self::ArrowArray {
        GenericStringArray::new(
            self.array.offsets().clone(),
            self.array.values().clone(),
            self.array.nulls().cloned(),
        )
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl<O: OffsetSizeTrait> From<(GenericStringArray<O>, WktType)> for GenericWktArray<O> {
    fn from((value, typ): (GenericStringArray<O>, WktType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl TryFrom<(&dyn Array, WktType)> for GenericWktArray<i32> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::Utf8 => Ok((value.as_string::<i32>().clone(), typ).into()),
            DataType::LargeUtf8 => {
                let geom_array: GenericWktArray<i64> =
                    (value.as_string::<i64>().clone(), typ).into();
                geom_array.try_into()
            }
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected WktArray DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, WktType)> for GenericWktArray<i64> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::Utf8 => {
                let geom_array: GenericWktArray<i32> =
                    (value.as_string::<i32>().clone(), typ).into();
                Ok(geom_array.into())
            }
            DataType::LargeUtf8 => Ok((value.as_string::<i64>().clone(), typ).into()),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected WktArray DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for GenericWktArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field
            .try_extension_type::<WktType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for GenericWktArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field
            .try_extension_type::<WktType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}

impl From<GenericWktArray<i32>> for GenericWktArray<i64> {
    fn from(value: GenericWktArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self {
            data_type: value.data_type,
            array: LargeStringArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls),
        }
    }
}

impl TryFrom<GenericWktArray<i64>> for GenericWktArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: GenericWktArray<i64>) -> GeoArrowResult<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Ok(Self {
            data_type: value.data_type,
            array: StringArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls),
        })
    }
}

impl<O: OffsetSizeTrait> From<WktViewArray> for GenericWktArray<O> {
    fn from(value: WktViewArray) -> Self {
        let wkb_type = value.data_type;
        let binary_view_array = value.array;

        // Copy the bytes from the binary view array into a new byte array
        let mut builder = GenericStringBuilder::new();
        binary_view_array
            .iter()
            .for_each(|value| builder.append_option(value));

        Self {
            data_type: wkb_type,
            array: builder.finish(),
        }
    }
}

/// A [`GenericWktArray`] using `i32` offsets
///
/// The byte length of each element is represented by an i32.
///
/// See [`GenericWktArray`] for more information and examples
pub type WktArray = GenericWktArray<i32>;

/// A [`GenericWktArray`] using `i64` offsets
///
/// The byte length of each element is represented by an i64.
///
/// See [`GenericWktArray`] for more information and examples
pub type LargeWktArray = GenericWktArray<i64>;

// #[cfg(test)]
// mod test {
//     use arrow_array::builder::{LargeStringBuilder, StringBuilder};
//     use geoarrow_schema::{CoordType, Dimension};

//     use super::*;
//     use crate::{GeoArrowArray, cast::to_wkt, test::point};

//     fn wkt_data<O: OffsetSizeTrait>() -> GenericWktArray<O> {
//         to_wkt(&point::array(CoordType::Interleaved, Dimension::XY)).unwrap()
//     }

//     #[test]
//     fn parse_dyn_array_i32() {
//         let wkb_array = wkt_data::<i32>();
//         let array = wkb_array.to_array_ref();
//         let field = Field::new("geometry", array.data_type().clone(), true)
//             .with_extension_type(wkb_array.data_type.clone());
//         let wkb_array_retour: GenericWktArray<i32> = (array.as_ref(), &field).try_into().unwrap();

//         assert_eq!(wkb_array, wkb_array_retour);
//     }

//     #[test]
//     fn parse_dyn_array_i64() {
//         let wkb_array = wkt_data::<i64>();
//         let array = wkb_array.to_array_ref();
//         let field = Field::new("geometry", array.data_type().clone(), true)
//             .with_extension_type(wkb_array.data_type.clone());
//         let wkb_array_retour: GenericWktArray<i64> = (array.as_ref(), &field).try_into().unwrap();

//         assert_eq!(wkb_array, wkb_array_retour);
//     }

//     #[test]
//     fn convert_i32_to_i64() {
//         let wkb_array = wkt_data::<i32>();
//         let wkb_array_i64: GenericWktArray<i64> = wkb_array.clone().into();
//         let wkb_array_i32: GenericWktArray<i32> = wkb_array_i64.clone().try_into().unwrap();

//         assert_eq!(wkb_array, wkb_array_i32);
//     }

//     #[test]
//     fn convert_i64_to_i32_to_i64() {
//         let wkb_array = wkt_data::<i64>();
//         let wkb_array_i32: GenericWktArray<i32> = wkb_array.clone().try_into().unwrap();
//         let wkb_array_i64: GenericWktArray<i64> = wkb_array_i32.clone().into();

//         assert_eq!(wkb_array, wkb_array_i64);
//     }

//     /// Passing a field without an extension name should not panic
//     #[test]
//     fn allow_field_without_extension_name() {
//         // String array
//         let mut builder = StringBuilder::new();
//         builder.append_value("POINT(1 2)");
//         let array = Arc::new(builder.finish()) as ArrayRef;
//         let field = Field::new("geometry", array.data_type().clone(), true);
//         let _wkt_arr = GenericWktArray::<i32>::try_from((array.as_ref(), &field)).unwrap();

//         // Large string
//         let mut builder = LargeStringBuilder::new();
//         builder.append_value("POINT(1 2)");
//         let array = Arc::new(builder.finish()) as ArrayRef;
//         let field = Field::new("geometry", array.data_type().clone(), true);
//         let _wkt_arr = GenericWktArray::<i64>::try_from((array.as_ref(), &field)).unwrap();
//     }
// }
