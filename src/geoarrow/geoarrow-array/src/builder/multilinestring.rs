use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait};
use geoarrow_schema::{
    Dimension, MultiLineStringType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, MultiLineStringArray},
    builder::{CoordBufferBuilder, OffsetsBuilder},
    capacity::MultiLineStringCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
    util::GeometryTypeName,
};

/// The GeoArrow equivalent to `Vec<Option<MultiLineString>>`: a mutable collection of
/// MultiLineStrings.
///
/// Converting an [`MultiLineStringBuilder`] into a [`MultiLineStringArray`] is `O(1)`.
#[derive(Debug)]
pub struct MultiLineStringBuilder {
    data_type: MultiLineStringType,

    pub(crate) coords: CoordBufferBuilder,

    /// OffsetsBuilder into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetsBuilder<i64>,

    /// OffsetsBuilder into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetsBuilder<i64>,

    /// Validity is only defined at the geometry level
    pub(crate) validity: NullBufferBuilder,
}

impl MultiLineStringBuilder {
    /// Creates a new empty [`MultiLineStringBuilder`].
    pub fn new(typ: MultiLineStringType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`MultiLineStringBuilder`] with a capacity.
    pub fn with_capacity(typ: MultiLineStringType, capacity: MultiLineStringCapacity) -> Self {
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

    /// Reserves capacity for at least `additional` more MultiLineStrings.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: MultiLineStringCapacity) {
        self.coords.reserve(additional.coord_capacity);
        self.ring_offsets.reserve(additional.ring_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more MultiLineStrings.
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
    pub fn reserve_exact(&mut self, additional: MultiLineStringCapacity) {
        self.coords.reserve_exact(additional.coord_capacity);
        self.ring_offsets.reserve_exact(additional.ring_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.coords.shrink_to_fit();
        self.ring_offsets.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// The canonical method to create a [`MultiLineStringBuilder`] out of its internal
    /// components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn try_new(
        coords: CoordBufferBuilder,
        geom_offsets: OffsetsBuilder<i64>,
        ring_offsets: OffsetsBuilder<i64>,
        validity: NullBufferBuilder,
        data_type: MultiLineStringType,
    ) -> GeoArrowResult<Self> {
        // check(
        //     &coords.clone().into(),
        //     &geom_offsets.clone().into(),
        //     &ring_offsets.clone().into(),
        //     validity.as_ref().map(|x| x.len()),
        // )?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
            data_type,
        })
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

    /// Consume the builder and convert to an immutable [`MultiLineStringArray`]
    pub fn finish(mut self) -> MultiLineStringArray {
        let validity = self.validity.finish();

        MultiLineStringArray::new(
            self.coords.finish(),
            self.geom_offsets.finish(),
            self.ring_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
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
            // Total number of linestrings in this multilinestring
            let num_line_strings = 1;
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            self.ring_offsets.try_push_usize(line_string.num_coords())?;

            for coord in line_string.coords() {
                self.coords.push_coord(&coord);
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_line_string) = value {
            // Total number of linestrings in this multilinestring
            let num_line_strings = multi_line_string.num_line_strings();
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            // Number of coords for each ring
            for line_string in multi_line_string.line_strings() {
                self.ring_offsets.try_push_usize(line_string.num_coords())?;

                for coord in line_string.coords() {
                    self.coords.push_coord(&coord);
                }
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new geometry to this builder
    ///
    /// This will error if the geometry type is not LineString or MultiLineString.
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(value) = value {
            match value.as_type() {
                GeometryType::LineString(g) => self.push_line_string(Some(g))?,
                GeometryType::MultiLineString(g) => self.push_multi_line_string(Some(g))?,
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected MultiLineString compatible geometry, got {}",
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
        geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_multi_point| self.push_multi_line_string(maybe_multi_point))
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
    /// Care must be taken to ensure that pushing raw coordinates
    /// to the array upholds the necessary invariants of the array.
    #[inline]
    pub(crate) fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        self.coords.push_coord(coord);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        // NOTE! Only the geom_offsets array needs to get extended, because the next geometry will
        // point to the same ring array location
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_multi_line_strings(
        geoms: &[impl MultiLineStringTrait<T = f64>],
        typ: MultiLineStringType,
    ) -> Self {
        let capacity = MultiLineStringCapacity::from_multi_line_strings(geoms.iter().map(Some));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_multi_line_strings(
        geoms: &[Option<impl MultiLineStringTrait<T = f64>>],
        typ: MultiLineStringType,
    ) -> Self {
        let capacity =
            MultiLineStringCapacity::from_multi_line_strings(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        array
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: MultiLineStringType,
    ) -> GeoArrowResult<Self> {
        let capacity = MultiLineStringCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_geometry_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, MultiLineStringType)>
    for MultiLineStringBuilder
{
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, MultiLineStringType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for MultiLineStringBuilder {
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

impl GeometryTypeId for MultiLineStringBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 5;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
