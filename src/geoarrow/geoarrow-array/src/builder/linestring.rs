use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait};
use geoarrow_schema::{
    Dimension, LineStringType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, LineStringArray},
    builder::{CoordBufferBuilder, OffsetsBuilder, geo_trait_wrappers::LineWrapper},
    capacity::LineStringCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<LineString>>`: a mutable collection of LineStrings.
///
/// Converting an [`LineStringBuilder`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct LineStringBuilder {
    data_type: LineStringType,

    pub(crate) coords: CoordBufferBuilder,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i64>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl LineStringBuilder {
    /// Creates a new empty [`LineStringBuilder`].
    pub fn new(typ: LineStringType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`LineStringBuilder`] with a capacity.
    pub fn with_capacity(typ: LineStringType, capacity: LineStringCapacity) -> Self {
        let coords = CoordBufferBuilder::with_capacity(
            capacity.coord_capacity,
            typ.coord_type(),
            typ.dimension(),
        );
        Self {
            coords,
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity()),
            validity: NullBufferBuilder::new(capacity.geom_capacity()),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more LineStrings.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: LineStringCapacity) {
        self.coords.reserve(additional.coord_capacity());
        self.geom_offsets.reserve(additional.geom_capacity());
    }

    /// Reserves the minimum capacity for at least `additional` more LineStrings.
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
    pub fn reserve_exact(&mut self, additional: LineStringCapacity) {
        self.coords.reserve_exact(additional.coord_capacity());
        self.geom_offsets.reserve_exact(additional.geom_capacity());
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    #[inline]
    pub(crate) fn try_push_length(&mut self, geom_offsets_length: usize) -> GeoArrowResult<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    /// Add a valid but empty LineString to the end of this array.
    #[inline]
    pub fn push_empty(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(true);
    }

    /// Add a new null value to the end of this array.
    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Consume the builder and convert to an immutable [`LineStringArray`]
    pub fn finish(mut self) -> LineStringArray {
        let validity = self.validity.finish();
        LineStringArray::new(
            self.coords.finish(),
            self.geom_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_line_strings(geoms: &[impl LineStringTrait<T = f64>], typ: LineStringType) -> Self {
        let capacity = LineStringCapacity::from_line_strings(geoms.iter().map(Some));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_line_strings(
        geoms: &[Option<impl LineStringTrait<T = f64>>],
        typ: LineStringType,
    ) -> Self {
        let capacity = LineStringCapacity::from_line_strings(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Add a new LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(line_string) = value {
            let num_coords = line_string.num_coords();
            for coord in line_string.coords() {
                self.coords.try_push_coord(&coord)?;
            }
            self.try_push_length(num_coords)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_line_string(maybe_multi_point))
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
    #[allow(dead_code)]
    pub(crate) fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        self.coords.try_push_coord(coord)
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not LineString or a MultiLineString with length 1.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::LineString(g) => self.push_line_string(Some(g))?,
                GeometryType::MultiLineString(ml) => {
                    let num_line_strings = ml.num_line_strings();
                    if num_line_strings == 0 {
                        self.push_empty();
                    } else if num_line_strings == 1 {
                        self.push_line_string(Some(&ml.line_string(0).unwrap()))?
                    } else {
                        return Err(GeoArrowError::IncorrectGeometryType(format!(
                            "Expected MultiLineString with only one LineString in LineStringBuilder, got {num_line_strings} line strings",
                        )));
                    }
                }
                GeometryType::Line(l) => self.push_line_string(Some(&LineWrapper(l)))?,
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected LineString, got {}",
                        gt.name()
                    )));
                }
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: LineStringType,
    ) -> GeoArrowResult<Self> {
        let capacity = LineStringCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, LineStringType)> for LineStringBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, LineStringType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for LineStringBuilder {
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

impl GeometryTypeId for LineStringBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 2;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
