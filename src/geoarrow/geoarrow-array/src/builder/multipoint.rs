use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait};
use geoarrow_schema::{
    Dimension, MultiPointType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, MultiPointArray},
    builder::{CoordBufferBuilder, OffsetsBuilder},
    capacity::MultiPointCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<MultiPoint>>`: a mutable collection of MultiPoints.
///
/// Converting an [`MultiPointBuilder`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiPointBuilder {
    data_type: MultiPointType,

    coords: CoordBufferBuilder,

    geom_offsets: OffsetsBuilder<i64>,

    /// Validity is only defined at the geometry level
    validity: NullBufferBuilder,
}

impl MultiPointBuilder {
    /// Creates a new empty [`MultiPointBuilder`].
    pub fn new(typ: MultiPointType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`MultiPointBuilder`] with a capacity.
    pub fn with_capacity(typ: MultiPointType, capacity: MultiPointCapacity) -> Self {
        let coords = CoordBufferBuilder::with_capacity(
            capacity.coord_capacity,
            typ.coord_type(),
            typ.dimension(),
        );
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more MultiPoints.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve(capacity.coord_capacity);
        self.geom_offsets.reserve(capacity.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiPoints.
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
    pub fn reserve_exact(&mut self, capacity: MultiPointCapacity) {
        self.coords.reserve_exact(capacity.coord_capacity);
        self.geom_offsets.reserve_exact(capacity.geom_capacity);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Consume the builder and convert to an immutable [`MultiPointArray`]
    pub fn finish(mut self) -> MultiPointArray {
        let validity = self.validity.finish();
        MultiPointArray::new(
            self.coords.finish(),
            self.geom_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_multi_point(maybe_multi_point))
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

    /// Add a new Point to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(point) = value {
            self.coords.push_point(point);
            self.try_push_length(1)?;
        } else {
            self.push_null();
        }

        Ok(())
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_point) = value {
            let num_points = multi_point.num_points();
            for point in multi_point.points() {
                self.coords.push_point(&point);
            }
            self.try_push_length(num_points)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Point or MultiPoint.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Point(g) => self.push_point(Some(g))?,
                GeometryType::MultiPoint(g) => self.push_multi_point(Some(g))?,
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected MultiPoint compatible geometry, got {}",
                        gt.name()
                    )));
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Push a raw coordinate to the underlying coordinate array.
    ///
    /// # Invariant
    ///
    /// Care must be taken to ensure that pushing raw coordinates to the array upholds the
    /// necessary invariants of the array.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        self.coords.try_push_coord(coord)
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub(crate) fn try_push_length(&mut self, geom_offsets_length: usize) -> GeoArrowResult<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_multi_points(geoms: &[impl MultiPointTrait<T = f64>], typ: MultiPointType) -> Self {
        let capacity = MultiPointCapacity::from_multi_points(geoms.iter().map(Some));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_multi_points(
        geoms: &[Option<impl MultiPointTrait<T = f64>>],
        typ: MultiPointType,
    ) -> Self {
        let capacity = MultiPointCapacity::from_multi_points(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: MultiPointType,
    ) -> GeoArrowResult<Self> {
        let capacity = MultiPointCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, MultiPointType)> for MultiPointBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, MultiPointType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for MultiPointBuilder {
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

impl GeometryTypeId for MultiPointBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 4;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
