use std::sync::Arc;

use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, cast::AsArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, Metadata, MultiPolygonType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    array::{CoordBuffer, GenericWkbArray, PolygonArray},
    builder::MultiPolygonBuilder,
    capacity::MultiPolygonCapacity,
    eq::offset_buffer_eq,
    scalar::MultiPolygon,
    trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow},
    util::{OffsetBufferUtils, offsets_buffer_i32_to_i64},
};

/// An immutable array of MultiPolygon geometries.
///
/// This is semantically equivalent to `Vec<Option<MultiPolygon>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiPolygonArray {
    pub(crate) data_type: MultiPolygonType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i64>,

    /// Offsets into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i64>,

    /// Validity bitmap
    pub(crate) nulls: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<i64>,
    polygon_offsets: &OffsetBuffer<i64>,
    ring_offsets: &OffsetBuffer<i64>,
    validity_len: Option<usize>,
) -> GeoArrowResult<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::InvalidGeoArrow(
            "nulls mask length must match the number of values".to_string(),
        ));
    }

    // Offset can be smaller than coords length if sliced
    if *ring_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if *polygon_offsets.last() as usize != ring_offsets.len_proxy() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "largest polygon offset must match ring offsets length".to_string(),
        ));
    }

    if *geom_offsets.last() as usize > polygon_offsets.len_proxy() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "largest geometry offset must not be longer than polygon offsets length".to_string(),
        ));
    }

    Ok(())
}

impl MultiPolygonArray {
    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i64>,
        polygon_offsets: OffsetBuffer<i64>,
        ring_offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            nulls,
            metadata,
        )
        .unwrap()
    }

    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the nulls is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i64>,
        polygon_offsets: OffsetBuffer<i64>,
        ring_offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> GeoArrowResult<Self> {
        check(
            &coords,
            &geom_offsets,
            &polygon_offsets,
            &ring_offsets,
            nulls.as_ref().map(|v| v.len()),
        )?;
        Ok(Self {
            data_type: MultiPolygonType::new(coords.dim(), metadata)
                .with_coord_type(coords.coord_type()),
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            nulls,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        Field::new_large_list(name, self.vertices_field(), false).into()
    }

    fn polygons_field(&self) -> Arc<Field> {
        let name = "polygons";
        Field::new_large_list(name, self.rings_field(), false).into()
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i64> {
        &self.geom_offsets
    }

    /// Access the underlying polygon offsets buffer
    pub fn polygon_offsets(&self) -> &OffsetBuffer<i64> {
        &self.polygon_offsets
    }

    /// Access the underlying ring offsets buffer
    pub fn ring_offsets(&self) -> &OffsetBuffer<i64> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiPolygonCapacity {
        MultiPolygonCapacity::new(
            *self.ring_offsets.last() as usize,
            *self.polygon_offsets.last() as usize,
            *self.geom_offsets.last() as usize,
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls.as_ref().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes(self.data_type.dimension())
    }

    /// Slice this [`MultiPolygonArray`].
    ///
    /// # Panic
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
            polygon_offsets: self.polygon_offsets.clone(),
            ring_offsets: self.ring_offsets.clone(),
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

impl GeoArrowArray for MultiPolygonArray {
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

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.nulls
            .as_ref()
            .map(|n| n.is_null(i))
            .unwrap_or_default()
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::MultiPolygon(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.with_metadata(metadata))
    }
}

impl<'a> GeoArrowArrayAccessor<'a> for MultiPolygonArray {
    type Item = MultiPolygon<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> GeoArrowResult<Self::Item> {
        Ok(MultiPolygon::new(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        ))
    }
}

impl IntoArrow for MultiPolygonArray {
    type ArrowArray = GenericListArray<i64>;
    type ExtensionType = MultiPolygonType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let rings_field = self.rings_field();
        let polygons_field = self.polygons_field();

        let nulls = self.nulls;
        let coord_array = ArrayRef::from(self.coords);
        let ring_array = Arc::new(GenericListArray::new(
            vertices_field,
            self.ring_offsets,
            coord_array,
            None,
        ));
        let polygons_array = Arc::new(GenericListArray::new(
            rings_field,
            self.polygon_offsets,
            ring_array,
            None,
        ));

        GenericListArray::new(polygons_field, self.geom_offsets, polygons_array, nulls)
    }

    fn extension_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(
        (geom_array, typ): (&GenericListArray<i32>, MultiPolygonType),
    ) -> GeoArrowResult<Self> {
        let geom_offsets = offsets_buffer_i32_to_i64(geom_array.offsets());
        let nulls = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i32>();

        let polygon_offsets = offsets_buffer_i32_to_i64(polygons_array.offsets());
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i32>();

        let ring_offsets = offsets_buffer_i32_to_i64(rings_array.offsets());
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(
        (geom_array, typ): (&GenericListArray<i64>, MultiPolygonType),
    ) -> GeoArrowResult<Self> {
        let geom_offsets = geom_array.offsets();
        let nulls = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i64>();

        let polygon_offsets = polygons_array.offsets();
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i64>();

        let ring_offsets = rings_array.offsets();
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            nulls.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, MultiPolygonType)) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::List(_) => (value.as_list::<i32>(), typ).try_into(),
            DataType::LargeList(_) => (value.as_list::<i64>(), typ).try_into(),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected MultiPolygon DataType: {dt:?}",
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> GeoArrowResult<Self> {
        let typ = field.try_extension_type::<MultiPolygonType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: (GenericWkbArray<O>, MultiPolygonType)) -> GeoArrowResult<Self> {
        let mut_arr: MultiPolygonBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl From<PolygonArray> for MultiPolygonArray {
    fn from(value: PolygonArray) -> Self {
        let (coord_type, dimension, metadata) = value.data_type.into_inner();
        let new_type = MultiPolygonType::new(dimension, metadata).with_coord_type(coord_type);

        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let ring_offsets = value.ring_offsets;
        let polygon_offsets = value.geom_offsets;
        let nulls = value.nulls;
        Self {
            data_type: new_type,
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            nulls,
        }
    }
}

impl PartialEq for MultiPolygonArray {
    fn eq(&self, other: &Self) -> bool {
        self.nulls == other.nulls
            && offset_buffer_eq(&self.geom_offsets, &other.geom_offsets)
            && offset_buffer_eq(&self.polygon_offsets, &other.polygon_offsets)
            && offset_buffer_eq(&self.ring_offsets, &other.ring_offsets)
            && self.coords == other.coords
    }
}

impl GeometryTypeId for MultiPolygonArray {
    const GEOMETRY_TYPE_OFFSET: i8 = 6;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}

// #[cfg(test)]
// mod test {
//     use geo_traits::to_geo::ToGeoMultiPolygon;
//     use geoarrow_schema::{CoordType, Dimension};

//     use super::*;
//     use crate::test::multipolygon;

//     #[test]
//     fn geo_round_trip() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let geoms = [
//                 Some(multipolygon::mp0()),
//                 None,
//                 Some(multipolygon::mp1()),
//                 None,
//             ];
//             let typ = MultiPolygonType::new(Dimension::XY, Default::default())
//                 .with_coord_type(coord_type);
//             let geo_arr = MultiPolygonBuilder::from_nullable_multi_polygons(&geoms, typ).finish();

//             for (i, g) in geo_arr.iter().enumerate() {
//                 assert_eq!(
//                     geoms[i],
//                     g.transpose().unwrap().map(|g| g.to_multi_polygon())
//                 );
//             }

//             // Test sliced
//             for (i, g) in geo_arr.slice(2, 2).iter().enumerate() {
//                 assert_eq!(
//                     geoms[i + 2],
//                     g.transpose().unwrap().map(|g| g.to_multi_polygon())
//                 );
//             }
//         }
//     }

//     #[test]
//     fn geo_round_trip2() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let geo_arr = multipolygon::array(coord_type, Dimension::XY);
//             let geo_geoms = geo_arr
//                 .iter()
//                 .map(|x| x.transpose().unwrap().map(|g| g.to_multi_polygon()))
//                 .collect::<Vec<_>>();

//             let typ = MultiPolygonType::new(Dimension::XY, Default::default())
//                 .with_coord_type(coord_type);
//             let geo_arr2 =
//                 MultiPolygonBuilder::from_nullable_multi_polygons(&geo_geoms, typ).finish();
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
//                 let geo_arr = multipolygon::array(coord_type, dim);

//                 let extension_type = geo_arr.extension_type().clone();
//                 let field = extension_type.to_field("geometry", true);

//                 let arrow_arr = geo_arr.to_array_ref();

//                 let geo_arr2: MultiPolygonArray =
//                     (arrow_arr.as_ref(), extension_type).try_into().unwrap();
//                 let geo_arr3: MultiPolygonArray = (arrow_arr.as_ref(), &field).try_into().unwrap();

//                 assert_eq!(geo_arr, geo_arr2);
//                 assert_eq!(geo_arr, geo_arr3);
//             }
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
//             let arr1 = multipolygon::array(CoordType::Interleaved, dim);
//             let arr2 = multipolygon::array(CoordType::Separated, dim);
//             assert_eq!(arr1, arr1);
//             assert_eq!(arr2, arr2);
//             assert_eq!(arr1, arr2);

//             assert_ne!(arr1, arr2.slice(0, 2));
//         }
//     }

//     #[test]
//     fn test_validation_with_sliced_array() {
//         let arr = multipolygon::array(CoordType::Interleaved, Dimension::XY);
//         let sliced = arr.slice(0, 1);

//         let back = MultiPolygonArray::try_from((
//             sliced.to_array_ref().as_ref(),
//             arr.extension_type().clone(),
//         ))
//         .unwrap();
//         assert_eq!(back.len(), 1);
//     }

//     #[test]
//     fn test_validation_with_array_sliced_by_arrow_rs() {
//         let arr = multipolygon::array(CoordType::Interleaved, Dimension::XY);
//         let sliced = arr.to_array_ref().slice(0, 1);

//         let back =
//             MultiPolygonArray::try_from((sliced.as_ref(), arr.extension_type().clone())).unwrap();
//         assert_eq!(back.len(), 1);
//     }
// }
