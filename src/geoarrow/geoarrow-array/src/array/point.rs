use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray, StructArray, cast::AsArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, Metadata, PointType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer},
    eq::point_eq,
    scalar::Point,
    trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow},
};

/// An immutable array of Point geometries.
///
/// All points must have the same dimension.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
pub struct PointArray {
    pub(crate) data_type: PointType,
    pub(crate) coords: CoordBuffer,
    pub(crate) nulls: Option<NullBuffer>,
}

/// Perform checks:
///
/// - Validity mask must have the same length as the coordinates.
pub(super) fn check(coords: &CoordBuffer, validity_len: Option<usize>) -> GeoArrowResult<()> {
    if validity_len.is_some_and(|len| len != coords.len()) {
        return Err(GeoArrowError::InvalidGeoArrow(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    Ok(())
}

impl PointArray {
    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    pub fn new(coords: CoordBuffer, validity: Option<NullBuffer>, metadata: Arc<Metadata>) -> Self {
        Self::try_new(coords, validity, metadata).unwrap()
    }

    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    pub fn try_new(
        coords: CoordBuffer,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> GeoArrowResult<Self> {
        check(&coords, nulls.as_ref().map(|v| v.len()))?;
        Ok(Self {
            data_type: PointType::new(coords.dim(), metadata).with_coord_type(coords.coord_type()),
            coords,
            nulls,
        })
    }

    /// Access the underlying coordinate buffer
    ///
    /// Note that some coordinates may be null, depending on the value of [`Self::logical_nulls`]
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> usize {
        self.len()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let dimension = self.data_type.dimension();
        let validity_len = self.nulls.as_ref().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths() * dimension.size() * 8
    }

    /// Slice this [`PointArray`].
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
            coords: self.coords.slice(offset, length),
            nulls: self.nulls.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the [`CoordType`] of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self {
            data_type: self.data_type.with_coord_type(coord_type),
            coords: self.coords.into_coord_type(coord_type),
            ..self
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

impl GeoArrowArray for PointArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_arrow()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.coords.len()
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
        GeoArrowType::Point(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.with_metadata(metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for PointArray {
    type Item = Point<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        Ok(Point::new(&self.coords, index))
    }
}

impl IntoArrow for PointArray {
    type ArrowArray = ArrayRef;
    type ExtensionType = PointType;

    fn into_arrow(self) -> Self::ArrowArray {
        let validity = self.nulls;
        let dim = self.coords.dim();
        match self.coords {
            CoordBuffer::Interleaved(c) => Arc::new(FixedSizeListArray::new(
                c.values_field().into(),
                dim.size() as i32,
                Arc::new(c.values_array()),
                validity,
            )),
            CoordBuffer::Separated(c) => {
                let fields = c.values_field();
                Arc::new(StructArray::new(fields.into(), c.values_array(), validity))
            }
        }
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&FixedSizeListArray, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&FixedSizeListArray, PointType)) -> GeoArrowResult<Self> {
        let interleaved_coords = InterleavedCoordBuffer::from_arrow(value, typ.dimension())?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.nulls().cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&StructArray, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&StructArray, PointType)) -> GeoArrowResult<Self> {
        let validity = value.nulls();
        let separated_coords = SeparatedCoordBuffer::from_arrow(value, typ.dimension())?;
        Ok(Self::new(
            CoordBuffer::Separated(separated_coords),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, PointType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::FixedSizeList(_, _) => (value.as_fixed_size_list(), typ).try_into(),
            DataType::Struct(_) => (value.as_struct(), typ).try_into(),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected Point DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field.try_extension_type::<PointType>()?;
        (arr, typ).try_into()
    }
}

// Implement a custom PartialEq for PointArray to allow Point(EMPTY) comparisons, which is stored
// as (NaN, NaN). By default, these resolve to false
impl PartialEq for PointArray {
    fn eq(&self, other: &Self) -> bool {
        if self.nulls != other.nulls {
            return false;
        }

        if self.coords.len() != other.coords.len() {
            return false;
        }

        for point_idx in 0..self.len() {
            let p1 = self.get(point_idx).unwrap();
            let p2 = other.get(point_idx).unwrap();
            match (p1, p2) {
                (Some(p1), Some(p2)) => {
                    if !point_eq(&p1, &p2) {
                        return false;
                    }
                }
                (None, None) => continue,
                _ => return false,
            }
        }

        true
    }
}

impl GeometryTypeId for PointArray {
    const GEOMETRY_TYPE_OFFSET: i8 = 1;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}

// #[cfg(test)]
// mod test {
//     use geo_traits::to_geo::ToGeoPoint;
//     use geoarrow_schema::{CoordType, Dimension};

//     use super::*;
//     use crate::{builder::PointBuilder, test::point};

//     #[test]
//     fn geo_round_trip() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let geoms = [
//                 Some(point::p0()),
//                 Some(point::p1()),
//                 None,
//                 Some(point::p2()),
//             ];
//             let typ = PointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
//             let geo_arr =
//                 PointBuilder::from_nullable_points(geoms.iter().map(|x| x.as_ref()), typ).finish();

//             for (i, g) in geo_arr.iter().enumerate() {
//                 assert_eq!(geoms[i], g.transpose().unwrap().map(|g| g.to_point()));
//             }

//             // Test sliced
//             for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
//                 assert_eq!(geoms[i + 2], g.transpose().unwrap().map(|g| g.to_point()));
//             }
//         }
//     }

//     #[test]
//     fn try_from_arrow() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let geo_arr = point::array(coord_type, dim);

//                 let point_type = geo_arr.extension_type().clone();
//                 let field = point_type.to_field("geometry", true);

//                 let arrow_arr = geo_arr.to_array_ref();

//                 let geo_arr2: PointArray = (arrow_arr.as_ref(), point_type).try_into().unwrap();
//                 let geo_arr3: PointArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

//                 assert_eq!(geo_arr, geo_arr2);
//                 assert_eq!(geo_arr, geo_arr3);
//             }
//         }
//     }

//     #[test]
//     fn into_coord_type() {
//         for dim in [
//             Dimension::XY,
//             Dimension::XYZ,
//             Dimension::XYM,
//             Dimension::XYZM,
//         ] {
//             let geo_arr = point::array(CoordType::Interleaved, dim);
//             let geo_arr2 = geo_arr
//                 .clone()
//                 .into_coord_type(CoordType::Separated)
//                 .into_coord_type(CoordType::Interleaved);

//             assert_eq!(geo_arr, geo_arr2);
//         }
//     }

//     #[test]
//     fn partial_eq() {
//         for dim in [
//             Dimension::XY,
//             Dimension::XYZ,
//             Dimension::XYM,
//             Dimension::XYZM,
//         ] {
//             let arr1 = point::array(CoordType::Interleaved, dim);
//             let arr2 = point::array(CoordType::Separated, dim);
//             assert_eq!(arr1, arr1);
//             assert_eq!(arr2, arr2);
//             assert_eq!(arr1, arr2);

//             assert_ne!(arr1, arr2.slice(0, 2));
//         }
//     }
// }
