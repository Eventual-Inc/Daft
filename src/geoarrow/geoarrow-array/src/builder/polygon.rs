use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait, PolygonTrait,
    RectTrait,
};
use geoarrow_schema::{
    Dimension, PolygonType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, PolygonArray},
    builder::{
        CoordBufferBuilder, OffsetsBuilder,
        geo_trait_wrappers::{RectWrapper, TriangleWrapper},
    },
    capacity::PolygonCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<Polygon>>`: a mutable collection of Polygons.
///
/// Converting an [`PolygonBuilder`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct PolygonBuilder {
    data_type: PolygonType,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i64>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<i64>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl PolygonBuilder {
    /// Creates a new empty [`PolygonBuilder`].
    pub fn new(typ: PolygonType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`PolygonBuilder`] with given capacity and no validity.
    pub fn with_capacity(typ: PolygonType, capacity: PolygonCapacity) -> Self {
        let coords = CoordBufferBuilder::with_capacity(
            capacity.coord_capacity,
            typ.coord_type(),
            typ.dimension(),
        );
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            ring_offsets: OffsetsBuilder::with_capacity(capacity.ring_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more Polygons.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.ring_offsets.reserve(capacity.ring_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more Polygons.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, capacity: PolygonCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.ring_offsets.reserve_exact(capacity.ring_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.ring_offsets.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Push a raw offset to the underlying geometry offsets buffer.
    ///
    /// # Invariants
    ///
    /// Care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_push_geom_offset(&mut self, offsets_length: usize) -> GeoArrowResult<()> {
        self.geom_offsets.try_push_usize(offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    /// Push a raw offset to the underlying ring offsets buffer.
    ///
    /// # Invariants
    ///
    /// Care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_push_ring_offset(&mut self, offsets_length: usize) -> GeoArrowResult<()> {
        self.ring_offsets.try_push_usize(offsets_length)?;
        Ok(())
    }

    /// Consume the builder and convert to an immutable [`PolygonArray`]
    pub fn finish(mut self) -> PolygonArray {
        let validity = self.validity.finish();

        PolygonArray::new(
            self.coords.finish(),
            self.geom_offsets.finish(),
            self.ring_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_polygon(
        &mut self,
        value: Option<&impl PolygonTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(polygon) = value {
            let exterior_ring = polygon.exterior();
            if exterior_ring.is_none() {
                self.push_empty();
                return Ok(());
            }

            // - Get exterior ring
            // - Add exterior ring's # of coords self.ring_offsets
            // - Push ring's coords to self.coords
            let ext_ring = polygon.exterior().unwrap();
            self.ring_offsets.try_push_usize(ext_ring.num_coords())?;
            for coord in ext_ring.coords() {
                self.coords.push_coord(&coord);
            }

            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            self.geom_offsets.try_push_usize(num_interiors + 1)?;

            // For each interior ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords
            for int_ring in polygon.interiors() {
                self.ring_offsets.try_push_usize(int_ring.num_coords())?;
                for coord in int_ring.coords() {
                    self.coords.push_coord(&coord);
                }
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new Rect to this builder
    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(rect) = value {
            let rect_wrapper = RectWrapper::try_new(rect)?;
            self.push_polygon(Some(&rect_wrapper))?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Polygon, a MultiPolygon of length 1, or Rect.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Polygon(g) => self.push_polygon(Some(g))?,
                GeometryType::MultiPolygon(mp) => {
                    let num_polygons = mp.num_polygons();
                    if num_polygons == 0 {
                        self.push_empty();
                    } else if num_polygons == 1 {
                        self.push_polygon(Some(&mp.polygon(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::IncorrectGeometryType(format!(
                            "Expected MultiPolygon with only one polygon in PolygonBuilder, got {num_polygons} polygons",
                        )));
                    }
                }
                GeometryType::Rect(g) => self.push_rect(Some(g))?,
                GeometryType::Triangle(tri) => self.push_polygon(Some(&TriangleWrapper(tri)))?,
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected Polygon compatible geometry, got {}",
                        gt.name()
                    )));
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_polygon| self.push_polygon(maybe_polygon))
            .unwrap();
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_geometry_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> GeoArrowResult<()> {
        geoms.into_iter().try_for_each(|g| self.push_geometry(g))?;
        Ok(())
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Invariants
    ///
    /// Care must be taken to ensure that pushing raw coordinates to the array upholds the
    /// necessary invariants of the array.
    #[inline]
    pub(crate) fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        self.coords.push_coord(coord);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_empty(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(true);
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_polygons(geoms: &[impl PolygonTrait<T = f64>], typ: PolygonType) -> Self {
        let capacity = PolygonCapacity::from_polygons(geoms.iter().map(Some));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_polygons(
        geoms: &[Option<impl PolygonTrait<T = f64>>],
        typ: PolygonType,
    ) -> Self {
        let capacity = PolygonCapacity::from_polygons(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: PolygonType,
    ) -> GeoArrowResult<Self> {
        let capacity = PolygonCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, PolygonType)> for PolygonBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, PolygonType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for PolygonBuilder {
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn push_null(&mut self) {
        self.push_null();
    }

    fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        self.push_geometry(geometry)
    }

    fn finish(self) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.finish())
    }
}

impl GeometryTypeId for PolygonBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 3;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}

// #[cfg(test)]
// mod test {
//     use geo::BoundingRect;
//     use geo_traits::to_geo::ToGeoPolygon;
//     use geo_types::{Rect, coord};
//     use geoarrow_schema::{Dimension, PolygonType};

//     use crate::{GeoArrowArrayAccessor, builder::PolygonBuilder};

//     #[test]
//     fn test_push_rect() {
//         let mut builder = PolygonBuilder::new(PolygonType::new(Dimension::XY, Default::default()));

//         let rect = Rect::new(coord! { x: 10., y: 20. }, coord! { x: 30., y: 10. });
//         builder.push_rect(Some(&rect)).unwrap();
//         let array = builder.finish();

//         let polygon = array.value(0).unwrap().to_polygon();
//         let bounding_rect = polygon.bounding_rect().unwrap();
//         assert_eq!(rect, bounding_rect);
//     }
// }
