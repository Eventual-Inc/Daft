use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, MultiPointTrait, PointTrait};
use geoarrow_schema::{
    Dimension, PointType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, PointArray},
    builder::CoordBufferBuilder,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<Point>>`: a mutable collection of Points.
///
/// Converting an [`PointBuilder`] into a [`PointArray`] is `O(1)`.
#[derive(Debug)]
pub struct PointBuilder {
    data_type: PointType,
    pub(crate) coords: CoordBufferBuilder,
    pub(crate) validity: NullBufferBuilder,
}

impl PointBuilder {
    /// Creates a new empty [`PointBuilder`].
    pub fn new(typ: PointType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`PointBuilder`] with a capacity.
    pub fn with_capacity(typ: PointType, capacity: usize) -> Self {
        let coords = CoordBufferBuilder::with_capacity(capacity, typ.coord_type(), typ.dimension());
        Self {
            coords,
            validity: NullBufferBuilder::new(capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more points to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to
    /// speculatively avoid frequent reallocations. After calling `reserve`,
    /// capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.coords.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more points.
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
    pub fn reserve_exact(&mut self, additional: usize) {
        self.coords.reserve_exact(additional);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Consume the builder and convert to an immutable [`PointArray`]
    pub fn finish(mut self) -> PointArray {
        let validity = self.validity.finish();
        PointArray::new(
            self.coords.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Add a new coord to the end of this array, interpreting the coord as a non-empty point.
    ///
    /// ## Panics
    ///
    /// - If the added coordinate does not have the same dimension as the point array.
    #[inline]
    pub fn push_coord(&mut self, value: Option<&impl CoordTrait<T = f64>>) {
        self.try_push_coord(value).unwrap()
    }

    /// Add a new point to the end of this array.
    ///
    /// ## Panics
    ///
    /// - If the added point does not have the same dimension as the point array.
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) {
        self.try_push_point(value).unwrap()
    }

    /// Add a new coord to the end of this array, where the coord is a non-empty point
    ///
    /// ## Errors
    ///
    /// - If the added coordinate does not have the same dimension as the point array.
    #[inline]
    pub fn try_push_coord(
        &mut self,
        value: Option<&impl CoordTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            self.coords.try_push_coord(value)?;
            self.validity.append(true);
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Add a new point to the end of this array.
    ///
    /// ## Errors
    ///
    /// - If the added point does not have the same dimension as the point array.
    #[inline]
    pub fn try_push_point(
        &mut self,
        value: Option<&impl PointTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            self.coords.try_push_point(value)?;
            self.validity.append(true);
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Add a valid but empty point to the end of this array.
    #[inline]
    pub fn push_empty(&mut self) {
        self.coords.push_constant(f64::NAN);
        self.validity.append_non_null();
    }

    /// Add a new null value to the end of this array.
    #[inline]
    pub fn push_null(&mut self) {
        self.coords.push_constant(f64::NAN);
        self.validity.append_null();
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not Point or a MultiPoint with length 1.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::Point(p) => self.push_point(Some(p)),
                GeometryType::MultiPoint(mp) => {
                    let num_points = mp.num_points();
                    if num_points == 0 {
                        self.push_empty();
                    } else if num_points == 1 {
                        self.push_point(Some(&mp.point(0).unwrap()))
                    } else {
                        return Err(GeoArrowError::IncorrectGeometryType(format!(
                            "Expected MultiPoint with only one point in PointBuilder, got {num_points} points",
                        )));
                    }
                }
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected point, got {}",
                        gt.name()
                    )));
                }
            }
        } else {
            self.push_null()
        };
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .for_each(|maybe_polygon| self.push_point(maybe_polygon));
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_geometry_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> GeoArrowResult<()> {
        geoms.into_iter().try_for_each(|g| self.push_geometry(g))?;
        Ok(())
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_points<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl PointTrait<T = f64> + 'a)>,
        typ: PointType,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(typ, geoms.len());
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(Some(maybe_point)));
        mutable_array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_points<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl PointTrait<T = f64> + 'a)>>,
        typ: PointType,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(typ, geoms.len());
        geoms
            .into_iter()
            .for_each(|maybe_point| mutable_array.push_point(maybe_point));
        mutable_array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: PointType,
    ) -> GeoArrowResult<Self> {
        let capacity = geoms.len();
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, PointType)> for PointBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, PointType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for PointBuilder {
    fn len(&self) -> usize {
        self.coords.len()
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

impl GeometryTypeId for PointBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 1;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
