use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray, cast::AsArray, types::Float64Type};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    BoxType, GeoArrowType, Metadata,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::{
    array::SeparatedCoordBuffer,
    scalar::Rect,
    trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow},
};

/// An immutable array of Rect or Box geometries.
///
/// A rect is an axis-aligned bounded rectangle whose area is defined by minimum and maximum
/// coordinates.
///
/// All rects must have the same dimension.
///
/// This is **not** an array type defined by the GeoArrow specification (as of spec version 0.1)
/// but is included here for parity with georust/geo, and to save memory for the output of
/// `bounds()`.
///
/// Internally this is implemented as a FixedSizeList, laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone)]
pub struct RectArray {
    pub(crate) data_type: BoxType,

    /// Separated arrays for each of the "lower" dimensions
    lower: SeparatedCoordBuffer,

    /// Separated arrays for each of the "upper" dimensions
    upper: SeparatedCoordBuffer,

    nulls: Option<NullBuffer>,
}

impl RectArray {
    /// Construct a new [`RectArray`] from parts
    pub fn new(
        lower: SeparatedCoordBuffer,
        upper: SeparatedCoordBuffer,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        assert_eq!(lower.dim(), upper.dim());
        Self {
            data_type: BoxType::new(lower.dim(), metadata),
            lower,
            upper,
            nulls,
        }
    }

    /// Access the coordinate buffer of the "lower" corner of the RectArray
    ///
    /// Note that this needs to be interpreted in conjunction with the [null
    /// buffer][Self::logical_nulls].
    pub fn lower(&self) -> &SeparatedCoordBuffer {
        &self.lower
    }

    /// Access the coordinate buffer of the "upper" corner of the RectArray
    ///
    /// Note that this needs to be interpreted in conjunction with the [null
    /// buffer][Self::logical_nulls].
    pub fn upper(&self) -> &SeparatedCoordBuffer {
        &self.upper
    }

    /// Slice this [`RectArray`].
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
            data_type: self.data_type.clone(),
            lower: self.lower().slice(offset, length),
            upper: self.upper().slice(offset, length),
            nulls: self.nulls.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the [`Metadata`] of this array.
    pub fn with_metadata(self, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: self.data_type.with_metadata(metadata),
            ..self
        }
    }
}

impl GeoArrowArray for RectArray {
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
        self.lower.len()
    }

    #[inline]
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls.clone()
    }

    #[inline]
    fn logical_null_count(&self) -> usize {
        self.nulls.as_ref().map(|v| v.null_count()).unwrap_or(0)
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls
            .as_ref()
            .map(|n| n.is_null(i))
            .unwrap_or_default()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::Rect(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.with_metadata(metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for RectArray {
    type Item = Rect<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        Ok(Rect::new(&self.lower, &self.upper, index))
    }
}

impl IntoArrow for RectArray {
    type ArrowArray = StructArray;
    type ExtensionType = BoxType;

    fn into_arrow(self) -> Self::ArrowArray {
        let fields = match self.data_type.data_type() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        };

        let mut arrays: Vec<ArrayRef> = vec![];

        // values_array takes care of the correct number of dimensions
        arrays.extend_from_slice(self.lower.values_array().as_slice());
        arrays.extend_from_slice(self.upper.values_array().as_slice());

        let nulls = self.nulls;
        StructArray::new(fields, arrays, nulls)
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&StructArray, BoxType)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&StructArray, BoxType)) -> GeoArrowResult<Self> {
        let dim = typ.dimension();
        let nulls = value.nulls();
        let columns = value.columns();
        if columns.len() != dim.size() * 2 {
            return Err(GeoArrowError::InvalidGeoArrow(format!(
                "Invalid number of columns for RectArray: expected {} but got {}",
                dim.size() * 2,
                columns.len()
            )));
        }

        let lower = columns[0..dim.size()]
            .iter()
            .map(|c| c.as_primitive::<Float64Type>().values().clone())
            .collect::<Vec<_>>();
        let lower = SeparatedCoordBuffer::from_vec(lower, dim)?;

        let upper = columns[dim.size()..]
            .iter()
            .map(|c| c.as_primitive::<Float64Type>().values().clone())
            .collect::<Vec<_>>();
        let upper = SeparatedCoordBuffer::from_vec(upper, dim)?;

        Ok(Self::new(
            lower,
            upper,
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, BoxType)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, BoxType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::Struct(_) => (value.as_struct(), dim).try_into(),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected Rect DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for RectArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field.try_extension_type::<BoxType>()?;
        (arr, typ).try_into()
    }
}

impl PartialEq for RectArray {
    fn eq(&self, other: &Self) -> bool {
        // A naive implementation of PartialEq would check for buffer equality. This won't always
        // work for null elements where the actual value can be undefined and doesn't have to be
        // equal. As such, it's simplest to reuse the upstream PartialEq impl, especially since
        // RectArray only has one coordinate type.
        self.clone().into_arrow() == other.clone().into_arrow()
    }
}

// #[cfg(test)]
// mod test {
//     use geo_traits::to_geo::ToGeoRect;
//     use geoarrow_schema::Dimension;

//     use super::*;
//     use crate::{builder::RectBuilder, test::rect};

//     #[test]
//     fn geo_round_trip() {
//         let geoms = [Some(rect::r0()), None, Some(rect::r1()), None];
//         let typ = BoxType::new(Dimension::XY, Default::default());
//         let geo_arr =
//             RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), typ).finish();

//         for (i, g) in geo_arr.iter().enumerate() {
//             assert_eq!(geoms[i], g.transpose().unwrap().map(|g| g.to_rect()));
//         }

//         // Test sliced
//         for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
//             assert_eq!(geoms[i + 2], g.transpose().unwrap().map(|g| g.to_rect()));
//         }
//     }

//     #[test]
//     fn try_from_arrow() {
//         let geo_arr = rect::r_array();

//         let extension_type = geo_arr.extension_type().clone();
//         let field = extension_type.to_field("geometry", true);

//         let arrow_arr = geo_arr.to_array_ref();

//         let geo_arr2: RectArray = (arrow_arr.as_ref(), extension_type).try_into().unwrap();
//         let geo_arr3: RectArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

//         assert_eq!(geo_arr, geo_arr2);
//         assert_eq!(geo_arr, geo_arr3);
//     }

//     #[test]
//     fn partial_eq() {
//         let arr1 = rect::r_array();
//         assert_eq!(arr1, arr1);
//     }
// }
