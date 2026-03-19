use geo_traits::*;
use geoarrow_schema::{
    CoordType, Dimension, LineStringType, MultiLineStringType, MultiPointType, MultiPolygonType,
    PointType, PolygonType,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::{
    array::MixedGeometryArray,
    builder::{
        LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
        PointBuilder, PolygonBuilder,
        geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper},
    },
    capacity::MixedCapacity,
    trait_::GeoArrowArrayBuilder,
};

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries, all
/// of which have the same dimension.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`MixedGeometryBuilder`] into a [`MixedGeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub(crate) struct MixedGeometryBuilder {
    /// The dimension of this builder.
    ///
    /// All underlying arrays must contain a coordinate buffer of this same dimension.
    dim: Dimension,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    pub(crate) points: PointBuilder,
    pub(crate) line_strings: LineStringBuilder,
    pub(crate) polygons: PolygonBuilder,
    pub(crate) multi_points: MultiPointBuilder,
    pub(crate) multi_line_strings: MultiLineStringBuilder,
    pub(crate) multi_polygons: MultiPolygonBuilder,

    // Invariant: `offsets.len() == types.len()`
    offsets: Vec<i32>,

    /// Whether to prefer multi or single arrays for new geometries.
    ///
    /// E.g. if this is `true` and a Point geometry is added, it will be added to the
    /// MultiPointBuilder. If this is `false`, the Point geometry will be added to the
    /// PointBuilder.
    ///
    /// The idea is that always adding multi-geometries will make it easier to downcast later.
    pub(crate) prefer_multi: bool,
}

impl MixedGeometryBuilder {
    pub(crate) fn with_capacity_and_options(
        dim: Dimension,
        capacity: MixedCapacity,
        coord_type: CoordType,
    ) -> Self {
        // Don't store array metadata on child arrays
        Self {
            dim,
            types: vec![],
            points: PointBuilder::with_capacity(
                PointType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.point,
            ),
            line_strings: LineStringBuilder::with_capacity(
                LineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.line_string,
            ),
            polygons: PolygonBuilder::with_capacity(
                PolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.polygon,
            ),
            multi_points: MultiPointBuilder::with_capacity(
                MultiPointType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_point,
            ),
            multi_line_strings: MultiLineStringBuilder::with_capacity(
                MultiLineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_line_string,
            ),
            multi_polygons: MultiPolygonBuilder::with_capacity(
                MultiPolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_polygon,
            ),
            offsets: vec![],
            prefer_multi: DEFAULT_PREFER_MULTI,
        }
    }

    pub(crate) fn with_prefer_multi(self, prefer_multi: bool) -> Self {
        Self {
            prefer_multi,
            ..self
        }
    }

    pub(crate) fn reserve(&mut self, capacity: MixedCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);
        self.points.reserve(capacity.point);
        self.line_strings.reserve(capacity.line_string);
        self.polygons.reserve(capacity.polygon);
        self.multi_points.reserve(capacity.multi_point);
        self.multi_line_strings.reserve(capacity.multi_line_string);
        self.multi_polygons.reserve(capacity.multi_polygon);
    }

    pub(crate) fn reserve_exact(&mut self, capacity: MixedCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve_exact(total_num_geoms);
        self.offsets.reserve_exact(total_num_geoms);
        self.points.reserve_exact(capacity.point);
        self.line_strings.reserve_exact(capacity.line_string);
        self.polygons.reserve_exact(capacity.polygon);
        self.multi_points.reserve_exact(capacity.multi_point);
        self.multi_line_strings
            .reserve_exact(capacity.multi_line_string);
        self.multi_polygons.reserve_exact(capacity.multi_polygon);
    }

    /// Shrinks the capacity of self to fit.
    pub(crate) fn shrink_to_fit(&mut self) {
        self.types.shrink_to_fit();
        self.offsets.shrink_to_fit();
        self.points.shrink_to_fit();
        self.line_strings.shrink_to_fit();
        self.polygons.shrink_to_fit();
        self.multi_points.shrink_to_fit();
        self.multi_line_strings.shrink_to_fit();
        self.multi_polygons.shrink_to_fit();
    }

    pub(crate) fn finish(self) -> MixedGeometryArray {
        MixedGeometryArray::new(
            self.types.into(),
            self.offsets.into(),
            Some(self.points.finish()),
            Some(self.line_strings.finish()),
            Some(self.polygons.finish()),
            Some(self.multi_points.finish()),
            Some(self.multi_line_strings.finish()),
            Some(self.multi_polygons.finish()),
        )
    }

    /// Add a new Point to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPointBuilder` child
    /// array. Otherwise, it will be stored in the `PointBuilder` child array.
    #[inline]
    pub(crate) fn push_point(&mut self, value: &impl PointTrait<T = f64>) -> GeoArrowResult<()> {
        if self.prefer_multi {
            self.add_multi_point_type();
            self.multi_points.push_point(Some(value))
        } else {
            self.add_point_type();
            self.points.push_point(Some(value));
            Ok(())
        }
    }

    #[inline]
    fn add_point_type(&mut self) {
        self.offsets.push(self.points.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(1),
            Dimension::XYZ => self.types.push(11),
            Dimension::XYM => self.types.push(21),
            Dimension::XYZM => self.types.push(31),
        }
    }

    /// Add a new LineString to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiLineStringBuilder` child
    /// array. Otherwise, it will be stored in the `LineStringBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_line_string(
        &mut self,
        value: &impl LineStringTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        if self.prefer_multi {
            self.add_multi_line_string_type();
            self.multi_line_strings.push_line_string(Some(value))
        } else {
            self.add_line_string_type();
            self.line_strings.push_line_string(Some(value))
        }
    }

    #[inline]
    fn add_line_string_type(&mut self) {
        self.offsets
            .push(self.line_strings.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(2),
            Dimension::XYZ => self.types.push(12),
            Dimension::XYM => self.types.push(22),
            Dimension::XYZM => self.types.push(32),
        }
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPolygonBuilder` child
    /// array. Otherwise, it will be stored in the `PolygonBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_polygon(
        &mut self,
        value: &impl PolygonTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        if self.prefer_multi {
            self.add_multi_polygon_type();
            self.multi_polygons.push_polygon(Some(value))
        } else {
            self.add_polygon_type();
            self.polygons.push_polygon(Some(value))
        }
    }

    #[inline]
    fn add_polygon_type(&mut self) {
        self.offsets.push(self.polygons.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(3),
            Dimension::XYZ => self.types.push(13),
            Dimension::XYM => self.types.push(23),
            Dimension::XYZM => self.types.push(33),
        }
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_point(
        &mut self,
        value: &impl MultiPointTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        self.add_multi_point_type();
        self.multi_points.push_multi_point(Some(value))
    }

    #[inline]
    fn add_multi_point_type(&mut self) {
        self.offsets
            .push(self.multi_points.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(4),
            Dimension::XYZ => self.types.push(14),
            Dimension::XYM => self.types.push(24),
            Dimension::XYZM => self.types.push(34),
        }
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_line_string(
        &mut self,
        value: &impl MultiLineStringTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        self.add_multi_line_string_type();
        self.multi_line_strings.push_multi_line_string(Some(value))
    }

    #[inline]
    fn add_multi_line_string_type(&mut self) {
        self.offsets
            .push(self.multi_line_strings.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(5),
            Dimension::XYZ => self.types.push(15),
            Dimension::XYM => self.types.push(25),
            Dimension::XYZM => self.types.push(35),
        }
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    pub(crate) fn push_multi_polygon(
        &mut self,
        value: &impl MultiPolygonTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        self.add_multi_polygon_type();
        self.multi_polygons.push_multi_polygon(Some(value))
    }

    #[inline]
    fn add_multi_polygon_type(&mut self) {
        self.offsets
            .push(self.multi_polygons.len().try_into().unwrap());
        match self.dim {
            Dimension::XY => self.types.push(6),
            Dimension::XYZ => self.types.push(16),
            Dimension::XYM => self.types.push(26),
            Dimension::XYZM => self.types.push(36),
        }
    }

    #[inline]
    pub(crate) fn push_geometry(
        &mut self,
        geom: &'_ impl GeometryTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        use geo_traits::GeometryType::*;

        match geom.as_type() {
            Point(g) => {
                self.push_point(g)?;
            }
            LineString(g) => {
                self.push_line_string(g)?;
            }
            Polygon(g) => {
                self.push_polygon(g)?;
            }
            MultiPoint(p) => self.push_multi_point(p)?,
            MultiLineString(p) => self.push_multi_line_string(p)?,
            MultiPolygon(p) => self.push_multi_polygon(p)?,
            GeometryCollection(gc) => {
                if gc.num_geometries() == 1 {
                    self.push_geometry(&gc.geometry(0).unwrap())?
                } else {
                    return Err(GeoArrowError::InvalidGeoArrow(
                        "nested geometry collections not supported in GeoArrow".to_string(),
                    ));
                }
            }
            Rect(r) => self.push_polygon(&RectWrapper::try_new(r)?)?,
            Triangle(tri) => self.push_polygon(&TriangleWrapper(tri))?,
            Line(l) => self.push_line_string(&LineWrapper(l))?,
        };
        Ok(())
    }
}
