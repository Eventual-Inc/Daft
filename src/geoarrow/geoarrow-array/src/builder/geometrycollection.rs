use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_schema::{
    Dimension, GeometryCollectionType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{GenericWkbArray, GeometryCollectionArray},
    builder::{
        MixedGeometryBuilder, OffsetsBuilder,
        geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper},
    },
    capacity::GeometryCollectionCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
};

/// The GeoArrow equivalent to `Vec<Option<GeometryCollection>>`: a mutable collection of
/// GeometryCollections.
///
/// Converting an [`GeometryCollectionBuilder`] into a [`GeometryCollectionArray`] is `O(1)`.
#[derive(Debug)]
pub struct GeometryCollectionBuilder {
    data_type: GeometryCollectionType,

    pub(crate) geoms: MixedGeometryBuilder,

    pub(crate) geom_offsets: OffsetsBuilder<i64>,

    pub(crate) validity: NullBufferBuilder,
}

impl<'a> GeometryCollectionBuilder {
    /// Creates a new empty [`GeometryCollectionBuilder`].
    pub fn new(typ: GeometryCollectionType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new empty [`GeometryCollectionBuilder`] with the provided
    /// [capacity][GeometryCollectionCapacity].
    pub fn with_capacity(
        typ: GeometryCollectionType,
        capacity: GeometryCollectionCapacity,
    ) -> Self {
        Self {
            geoms: MixedGeometryBuilder::with_capacity_and_options(
                typ.dimension(),
                capacity.mixed_capacity,
                typ.coord_type(),
            ),
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Change whether to prefer multi or single arrays for new single-part geometries.
    ///
    /// If `true`, a new `Point` will be added to the `MultiPointBuilder` child array, a new
    /// `LineString` will be added to the `MultiLineStringBuilder` child array, and a new `Polygon`
    /// will be added to the `MultiPolygonBuilder` child array.
    ///
    /// This can be desired when the user wants to downcast the array to a single geometry array
    /// later, as casting to a, say, `MultiPointArray` from a `GeometryCollectionArray` could be
    /// done zero-copy.
    ///
    /// Note that only geometries added _after_ this method is called will be affected.
    pub fn with_prefer_multi(self, prefer_multi: bool) -> Self {
        Self {
            geoms: self.geoms.with_prefer_multi(prefer_multi),
            ..self
        }
    }

    /// Reserves capacity for at least `additional` more GeometryCollections.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve(additional.mixed_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more GeometryCollections.
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
    pub fn reserve_exact(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve_exact(additional.mixed_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.geoms.shrink_to_fit();
        self.geom_offsets.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// Consume the builder and convert to an immutable [`GeometryCollectionArray`]
    pub fn finish(mut self) -> GeometryCollectionArray {
        let validity = self.validity.finish();
        GeometryCollectionArray::new(
            self.geoms.finish(),
            self.geom_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Push a Point onto the end of this builder
    #[inline]
    fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_point(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a LineString onto the end of this builder
    #[inline]
    fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_line_string(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a Polygon onto the end of this builder
    #[inline]
    fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_polygon(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiPoint onto the end of this builder
    #[inline]
    fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_point(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiLineString onto the end of this builder
    #[inline]
    fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_line_string(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiPolygon onto the end of this builder
    #[inline]
    fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_polygon(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        use geo_traits::GeometryType::*;

        if let Some(g) = value {
            match g.as_type() {
                Point(p) => self.push_point(Some(p))?,
                LineString(p) => {
                    self.push_line_string(Some(p))?;
                }
                Polygon(p) => self.push_polygon(Some(p))?,
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(p) => self.push_geometry_collection(Some(p))?,
                Rect(r) => self.push_polygon(Some(&RectWrapper::try_new(r)?))?,
                Triangle(tri) => self.push_polygon(Some(&TriangleWrapper(tri)))?,
                Line(l) => self.push_line_string(Some(&LineWrapper(l)))?,
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Push a GeometryCollection onto the end of this builder
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(gc) = value {
            let num_geoms = gc.num_geometries();
            for g in gc.geometries() {
                self.geoms.push_geometry(&g)?;
            }
            self.try_push_length(num_geoms)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_gc| self.push_geometry_collection(maybe_gc))
            .unwrap();
    }

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
    pub fn from_geometry_collections(
        geoms: &[impl GeometryCollectionTrait<T = f64>],
        typ: GeometryCollectionType,
    ) -> GeoArrowResult<Self> {
        let capacity =
            GeometryCollectionCapacity::from_geometry_collections(geoms.iter().map(Some))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometry_collections(
        geoms: &[Option<impl GeometryCollectionTrait<T = f64>>],
        typ: GeometryCollectionType,
    ) -> GeoArrowResult<Self> {
        let capacity = GeometryCollectionCapacity::from_geometry_collections(
            geoms.iter().map(|x| x.as_ref()),
        )?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: GeometryCollectionType,
    ) -> GeoArrowResult<Self> {
        let capacity =
            GeometryCollectionCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        for geom in geoms {
            array.push_geometry(geom.as_ref())?;
        }
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, GeometryCollectionType)>
    for GeometryCollectionBuilder
{
    type Error = GeoArrowError;

    fn try_from(
        (value, typ): (GenericWkbArray<O>, GeometryCollectionType),
    ) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for GeometryCollectionBuilder {
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

impl GeometryTypeId for GeometryCollectionBuilder {
    const GEOMETRY_TYPE_OFFSET: i8 = 7;

    fn dimension(&self) -> Dimension {
        self.data_type.dimension()
    }
}
