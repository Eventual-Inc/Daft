use std::sync::Arc;

use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, cast::AsArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, LineStringType, Metadata,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    array::{CoordBuffer, GenericWkbArray},
    builder::LineStringBuilder,
    capacity::LineStringCapacity,
    eq::offset_buffer_eq,
    scalar::LineString,
    trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow},
    util::{OffsetBufferUtils, offsets_buffer_i32_to_i64},
};

/// An immutable array of LineString geometries.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct LineStringArray {
    pub(crate) data_type: LineStringType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i64>,

    /// Validity bitmap
    pub(crate) nulls: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetBuffer<i64>,
) -> GeoArrowResult<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::InvalidGeoArrow(
            "nulls mask length must match the number of values".to_string(),
        ));
    }

    // Offset can be smaller than coords length if sliced
    if *geom_offsets.last() as usize > coords.len() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "largest geometry offset must not be longer than coords length".to_string(),
        ));
    }

    Ok(())
}

impl LineStringArray {
    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, nulls, metadata).unwrap()
    }

    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the nulls buffer does not have the same length as the number of geometries
    /// - if the geometry offsets do not match the number of coordinates
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> GeoArrowResult<Self> {
        check(&coords, nulls.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            data_type: LineStringType::new(coords.dim(), metadata)
                .with_coord_type(coords.coord_type()),
            coords,
            geom_offsets,
            nulls,
        })
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i64> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> LineStringCapacity {
        LineStringCapacity::new(*self.geom_offsets.last() as usize, self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls.as_ref().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes(self.data_type.dimension())
    }

    /// Slice this [`LineStringArray`].
    ///
    /// # Implementation
    ///
    /// This operation is `O(1)` as it amounts to increasing a few ref counts.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
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

impl GeoArrowArray for LineStringArray {
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
        self.geom_offsets.len_proxy()
    }

    #[inline]
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls.clone()
    }

    #[inline]
    fn logical_null_count(&self) -> usize {
        self.nulls.as_ref().map(|v| v.null_count()).unwrap_or(0)
    }

    fn is_null(&self, i: usize) -> bool {
        self.nulls
            .as_ref()
            .map(|n| n.is_null(i))
            .unwrap_or_default()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::LineString(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.with_metadata(metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for LineStringArray {
    type Item = LineString<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        Ok(LineString::new(&self.coords, &self.geom_offsets, index))
    }
}

impl IntoArrow for LineStringArray {
    type ArrowArray = GenericListArray<i64>;
    type ExtensionType = LineStringType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = match self.data_type.data_type() {
            DataType::LargeList(inner_field) => inner_field,
            _ => unreachable!(),
        };
        let nulls = self.nulls;
        let coord_array = self.coords.into_array_ref();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, nulls)
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i32>, LineStringType)) -> GeoArrowResult<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = offsets_buffer_i32_to_i64(value.offsets());
        let nulls = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i64>, LineStringType)) -> GeoArrowResult<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = value.offsets();
        let nulls = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}
impl TryFrom<(&dyn Array, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, LineStringType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::List(_) => (value.as_list::<i32>(), typ).try_into(),
            DataType::LargeList(_) => (value.as_list::<i64>(), typ).try_into(),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected LineString DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field.try_extension_type::<LineStringType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, LineStringType)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: (GenericWkbArray<O>, LineStringType)) -> GeoArrowResult<Self> {
        let mut_arr: LineStringBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl PartialEq for LineStringArray {
    fn eq(&self, other: &Self) -> bool {
        self.nulls == other.nulls
            && offset_buffer_eq(&self.geom_offsets, &other.geom_offsets)
            && self.coords == other.coords
    }
}

impl GeometryTypeId for LineStringArray {
    const GEOMETRY_TYPE_OFFSET: i8 = 2;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}

// #[cfg(test)]
// mod test {
//     use arrow_array::RecordBatch;
//     use arrow_schema::Schema;
//     use geo_traits::to_geo::ToGeoLineString;
//     use geoarrow_schema::{CoordType, Dimension};

//     use super::*;
//     use crate::test::linestring;

//     #[test]
//     fn geo_round_trip() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let geoms = [Some(linestring::ls0()), None, Some(linestring::ls1()), None];
//             let typ =
//                 LineStringType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
//             let geo_arr = LineStringBuilder::from_nullable_line_strings(&geoms, typ).finish();

//             for (i, g) in geo_arr.iter().enumerate() {
//                 assert_eq!(geoms[i], g.transpose().unwrap().map(|g| g.to_line_string()));
//             }

//             // Test sliced
//             for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
//                 assert_eq!(
//                     geoms[i + 2],
//                     g.transpose().unwrap().map(|g| g.to_line_string())
//                 );
//             }
//         }
//     }

//     #[test]
//     fn geo_round_trip2() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let geo_arr = linestring::array(coord_type, Dimension::XY);
//             let geo_geoms = geo_arr
//                 .iter()
//                 .map(|x| x.transpose().unwrap().map(|g| g.to_line_string()))
//                 .collect::<Vec<_>>();

//             let typ =
//                 LineStringType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
//             let geo_arr2 = LineStringBuilder::from_nullable_line_strings(&geo_geoms, typ).finish();
//             assert_eq!(geo_arr, geo_arr2);
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
//                 let geo_arr = linestring::array(coord_type, dim);

//                 let extension_type = geo_arr.extension_type().clone();
//                 let field = extension_type.to_field("geometry", true);

//                 let arrow_arr = geo_arr.to_array_ref();

//                 let geo_arr2: LineStringArray =
//                     (arrow_arr.as_ref(), extension_type).try_into().unwrap();
//                 let geo_arr3: LineStringArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

//                 assert_eq!(geo_arr, geo_arr2);
//                 assert_eq!(geo_arr, geo_arr3);
//             }
//         }
//     }

//     #[test]
//     fn partial_eq() {
//         let arr1 = linestring::ls_array(CoordType::Interleaved);
//         let arr2 = linestring::ls_array(CoordType::Separated);
//         assert_eq!(arr1, arr1);
//         assert_eq!(arr2, arr2);
//         assert_eq!(arr1, arr2);

//         assert_ne!(arr1, arr2.slice(0, 2));
//     }

//     #[test]
//     fn test_validation_with_sliced_array() {
//         let arr = linestring::array(CoordType::Interleaved, Dimension::XY);
//         let sliced = arr.slice(0, 1);

//         let back = LineStringArray::try_from((
//             sliced.to_array_ref().as_ref(),
//             arr.extension_type().clone(),
//         ))
//         .unwrap();
//         assert_eq!(back.len(), 1);
//     }

//     #[test]
//     fn slice_then_go_through_arrow() {
//         let arr = linestring::array(CoordType::Separated, Dimension::XY);
//         let sliced_array = arr.slice(0, 1);

//         let ls_array: LineStringArray = (
//             sliced_array.to_array_ref().as_ref(),
//             arr.extension_type().clone(),
//         )
//             .try_into()
//             .unwrap();
//         assert_eq!(ls_array.len(), 1);
//     }

//     #[test]
//     fn slice_back_from_arrow_rs_record_batch() {
//         let arr = linestring::array(CoordType::Separated, Dimension::XY);
//         let field = arr.extension_type().to_field("geometry", true);
//         let schema = Schema::new(vec![field]);

//         let batch = RecordBatch::try_new(Arc::new(schema), vec![arr.to_array_ref()]).unwrap();
//         let sliced_batch = batch.slice(0, 1);

//         let array = sliced_batch.column(0);
//         let field = sliced_batch.schema_ref().field(0);
//         let ls_array: LineStringArray = (array.as_ref(), field).try_into().unwrap();
//         assert_eq!(ls_array.len(), 1);
//     }

//     #[test]
//     fn slice_back_from_arrow_rs_array() {
//         let arr = linestring::array(CoordType::Separated, Dimension::XY);
//         let field = arr.extension_type().to_field("geometry", true);

//         let array = arr.to_array_ref();
//         let sliced_array = array.slice(0, 1);

//         let ls_array: LineStringArray = (sliced_array.as_ref(), &field).try_into().unwrap();
//         assert_eq!(ls_array.len(), 1);
//     }

//     #[test]
//     fn slice_back_from_arrow_rs_array_with_nulls() {
//         let arr = linestring::ls_array(CoordType::Separated);
//         let field = arr.extension_type().to_field("geometry", true);

//         let array = arr.to_array_ref();
//         let sliced_array = array.slice(0, 1);

//         let ls_array: LineStringArray = (sliced_array.as_ref(), &field).try_into().unwrap();
//         assert_eq!(ls_array.len(), 1);
//     }
// }
