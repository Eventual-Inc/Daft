use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{
    CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiPolygonTrait, PolygonTrait,
};
use geoarrow_schema::{
    Dimension, MultiPolygonType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, MultiPolygonArray},
    builder::{CoordBufferBuilder, OffsetsBuilder, geo_trait_wrappers::RectWrapper},
    capacity::MultiPolygonCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<MultiPolygon>>`: a mutable collection of MultiPolygons.
///
/// Converting an [`MultiPolygonBuilder`] into a [`MultiPolygonArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiPolygonBuilder {
    data_type: MultiPolygonType,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i64>,

    /// OffsetsBuilder into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetsBuilder<i64>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<i64>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl MultiPolygonBuilder {
    /// Creates a new empty [`MultiPolygonBuilder`].
    pub fn new(typ: MultiPolygonType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`MultiPolygonBuilder`] with a capacity.
    pub fn with_capacity(typ: MultiPolygonType, capacity: MultiPolygonCapacity) -> Self {
        let coords = CoordBufferBuilder::with_capacity(
            capacity.coord_capacity,
            typ.coord_type(),
            typ.dimension(),
        );
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            polygon_offsets: OffsetsBuilder::with_capacity(capacity.polygon_capacity),
            ring_offsets: OffsetsBuilder::with_capacity(capacity.ring_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more MultiPolygons.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: MultiPolygonCapacity) {
        self.coords.reserve(additional.coord_capacity);
        self.ring_offsets.reserve(additional.ring_capacity);
        self.polygon_offsets.reserve(additional.polygon_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiPolygons.
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
    pub fn reserve_exact(&mut self, additional: MultiPolygonCapacity) {
        self.coords.reserve_exact(additional.coord_capacity);
        self.ring_offsets.reserve_exact(additional.ring_capacity);
        self.polygon_offsets
            .reserve_exact(additional.polygon_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.ring_offsets.shrink_to_fit();
        self.polygon_offsets.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Consume the builder and convert to an immutable [`MultiPolygonArray`]
    pub fn finish(mut self) -> MultiPolygonArray {
        let validity = self.validity.finish();

        MultiPolygonArray::new(
            self.coords.finish(),
            self.geom_offsets.finish(),
            self.polygon_offsets.finish(),
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
                self.push_empty()?;
                return Ok(());
            }

            if let Some(ext_ring) = polygon.exterior() {
                // Total number of polygons in this MultiPolygon
                let num_polygons = 1;
                self.geom_offsets.try_push_usize(num_polygons)?;

                for coord in ext_ring.coords() {
                    self.coords.push_coord(&coord);
                }

                // Total number of rings in this Multipolygon
                self.polygon_offsets
                    .try_push_usize(polygon.num_interiors() + 1)?;

                // Number of coords for each ring
                self.ring_offsets.try_push_usize(ext_ring.num_coords())?;

                for int_ring in polygon.interiors() {
                    self.ring_offsets.try_push_usize(int_ring.num_coords())?;

                    for coord in int_ring.coords() {
                        self.coords.push_coord(&coord);
                    }
                }
            } else {
                let num_polygons = 0;
                self.geom_offsets.try_push_usize(num_polygons)?;
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_polygon) = value {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            self.try_push_geom_offset(num_polygons)?;

            // Iterate over polygons
            for polygon in multi_polygon.polygons() {
                // Here we unwrap the exterior ring because a polygon inside a multi polygon should
                // never be empty.
                let ext_ring = polygon.exterior().unwrap();
                for coord in ext_ring.coords() {
                    self.coords.push_coord(&coord);
                }

                // Total number of rings in this Multipolygon
                self.polygon_offsets
                    .try_push_usize(polygon.num_interiors() + 1)?;

                // Number of coords for each ring
                self.ring_offsets.try_push_usize(ext_ring.num_coords())?;

                for int_ring in polygon.interiors() {
                    self.ring_offsets.try_push_usize(int_ring.num_coords())?;

                    for coord in int_ring.coords() {
                        self.coords.push_coord(&coord);
                    }
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Polygon or MultiPolygon.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Polygon(g) => self.push_polygon(Some(g))?,
                GeometryType::MultiPolygon(g) => self.push_multi_polygon(Some(g))?,
                GeometryType::Rect(g) => self.push_polygon(Some(&RectWrapper::try_new(g)?))?,
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected MultiPolygon compatible geometry, got {}",
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
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_polygon| self.push_multi_polygon(maybe_multi_polygon))
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

    /// Push a raw offset to the underlying geometry offsets buffer.
    ///
    /// # Invariants
    ///
    /// Care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    pub(crate) fn try_push_geom_offset(&mut self, offsets_length: usize) -> GeoArrowResult<()> {
        self.geom_offsets.try_push_usize(offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    /// Push a raw offset to the underlying polygon offsets buffer.
    ///
    /// # Invariants
    ///
    /// Care must be taken to ensure that pushing raw offsets
    /// upholds the necessary invariants of the array.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_push_polygon_offset(&mut self, offsets_length: usize) -> GeoArrowResult<()> {
        self.polygon_offsets.try_push_usize(offsets_length)?;
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

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Safety
    ///
    /// This is marked as unsafe because care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    #[inline]
    pub unsafe fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        self.coords.push_coord(coord);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_empty(&mut self) -> GeoArrowResult<()> {
        self.geom_offsets.try_push_usize(0)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same polygon array location
        // Note that we don't use self.try_push_geom_offset because that sets validity to true
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_multi_polygons(
        geoms: &[impl MultiPolygonTrait<T = f64>],
        typ: MultiPolygonType,
    ) -> Self {
        let capacity = MultiPolygonCapacity::from_multi_polygons(geoms.iter().map(Some));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_multi_polygons(
        geoms: &[Option<impl MultiPolygonTrait<T = f64>>],
        typ: MultiPolygonType,
    ) -> Self {
        let capacity = MultiPolygonCapacity::from_multi_polygons(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: MultiPolygonType,
    ) -> GeoArrowResult<Self> {
        let capacity = MultiPolygonCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, MultiPolygonType)> for MultiPolygonBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, MultiPolygonType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for MultiPolygonBuilder {
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

impl GeometryTypeId for MultiPolygonBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 6;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
