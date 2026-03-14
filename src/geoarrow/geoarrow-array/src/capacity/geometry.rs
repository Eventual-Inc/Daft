use std::ops::AddAssign;

use geo_traits::*;
use geoarrow_schema::{Dimension, error::GeoArrowResult};
use wkt::WktNum;

use crate::{
    array::DimensionIndex,
    builder::geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper},
    capacity::{
        GeometryCollectionCapacity, LineStringCapacity, MultiLineStringCapacity,
        MultiPointCapacity, MultiPolygonCapacity, PolygonCapacity,
    },
};

/// A counter for the buffer sizes of a [`GeometryArray`][crate::array::GeometryArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Default, Debug, Clone, Copy)]
pub struct GeometryCapacity {
    /// The number of null geometries. Ideally the builder will assign these to any array that has
    /// already been allocated. Otherwise we don't know where to assign them.
    nulls: usize,

    /// Simple: just the total number of points, nulls included
    pub(crate) points: [usize; 4],
    /// An array of [LineStringCapacity], ordered XY, XYZ, XYM, XYZM
    pub(crate) line_strings: [LineStringCapacity; 4],
    pub(crate) polygons: [PolygonCapacity; 4],
    pub(crate) mpoints: [MultiPointCapacity; 4],
    pub(crate) mline_strings: [MultiLineStringCapacity; 4],
    pub(crate) mpolygons: [MultiPolygonCapacity; 4],
    pub(crate) gcs: [GeometryCollectionCapacity; 4],

    /// Whether to prefer multi or single arrays for new geometries.
    prefer_multi: bool,
}

impl GeometryCapacity {
    /// Create a new capacity with known sizes.
    ///
    /// Note that the ordering within each array must be XY, XYZ, XYM, XYZM.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        nulls: usize,
        points: [usize; 4],
        line_strings: [LineStringCapacity; 4],
        polygons: [PolygonCapacity; 4],
        mpoints: [MultiPointCapacity; 4],
        mline_strings: [MultiLineStringCapacity; 4],
        mpolygons: [MultiPolygonCapacity; 4],
        gcs: [GeometryCollectionCapacity; 4],
    ) -> Self {
        Self {
            nulls,
            points,
            line_strings,
            polygons,
            mpoints,
            mline_strings,
            mpolygons,
            gcs,
            prefer_multi: false,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Default::default()
    }

    /// Set whether this capacity counter should prefer allocating "single-type" geometries like
    /// Point/LineString/Polygon in the arrays of their "Multi" counterparts.
    pub fn with_prefer_multi(mut self, prefer_multi: bool) -> Self {
        self.prefer_multi = prefer_multi;
        self
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        if self.points.iter().any(|c| *c > 0) {
            return false;
        }

        if self.line_strings.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.polygons.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mpoints.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mline_strings.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mpolygons.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.gcs.iter().any(|c| !c.is_empty()) {
            return false;
        }

        true
    }

    /// The total number of geometries across all geometry types.
    pub fn total_num_geoms(&self) -> usize {
        let mut total = 0;

        self.points.iter().for_each(|c| {
            total += c;
        });
        self.line_strings.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.polygons.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mpoints.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mline_strings.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mpolygons.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.gcs.iter().for_each(|c| {
            total += c.geom_capacity();
        });

        total
    }

    /// Access point capacity
    pub fn point(&self, dim: Dimension) -> usize {
        self.points[dim.order()]
    }

    /// Access LineString capacity
    pub fn line_string(&self, dim: Dimension) -> LineStringCapacity {
        self.line_strings[dim.order()]
    }

    /// Access Polygon capacity
    pub fn polygon(&self, dim: Dimension) -> PolygonCapacity {
        self.polygons[dim.order()]
    }

    /// Access MultiPoint capacity
    pub fn multi_point(&self, dim: Dimension) -> MultiPointCapacity {
        self.mpoints[dim.order()]
    }

    /// Access point capacity
    pub fn multi_line_string(&self, dim: Dimension) -> MultiLineStringCapacity {
        self.mline_strings[dim.order()]
    }

    /// Access point capacity
    pub fn multi_polygon(&self, dim: Dimension) -> MultiPolygonCapacity {
        self.mpolygons[dim.order()]
    }

    /// Access GeometryCollection capacity
    pub fn geometry_collection(&self, dim: Dimension) -> GeometryCollectionCapacity {
        self.gcs[dim.order()]
    }

    /// Add the capacity of the given Point
    #[inline]
    fn add_point(&mut self, point: Option<&impl PointTrait>) -> GeoArrowResult<()> {
        if let Some(point) = point {
            let dim = Dimension::try_from(point.dim())?;
            if self.prefer_multi {
                self.mpoints[dim.order()].add_point(Some(point));
            } else {
                self.points[dim.order()] += 1;
            }
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given LineString
    #[inline]
    fn add_line_string(
        &mut self,
        line_string: Option<&impl LineStringTrait>,
    ) -> GeoArrowResult<()> {
        if let Some(line_string) = line_string {
            let dim = Dimension::try_from(line_string.dim())?;
            if self.prefer_multi {
                self.mline_strings[dim.order()].add_line_string(Some(line_string));
            } else {
                self.line_strings[dim.order()].add_line_string(Some(line_string));
            }
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given Polygon
    #[inline]
    fn add_polygon(&mut self, polygon: Option<&impl PolygonTrait>) -> GeoArrowResult<()> {
        if let Some(polygon) = polygon {
            let dim = Dimension::try_from(polygon.dim())?;
            if self.prefer_multi {
                self.mpolygons[dim.order()].add_polygon(Some(polygon));
            } else {
                self.polygons[dim.order()].add_polygon(Some(polygon));
            }
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given MultiPoint
    #[inline]
    fn add_multi_point(
        &mut self,
        multi_point: Option<&impl MultiPointTrait>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_point) = multi_point {
            self.multi_point(multi_point.dim().try_into()?)
                .add_multi_point(Some(multi_point));
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given MultiLineString
    #[inline]
    fn add_multi_line_string(
        &mut self,
        multi_line_string: Option<&impl MultiLineStringTrait>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_line_string) = multi_line_string {
            self.multi_line_string(multi_line_string.dim().try_into()?)
                .add_multi_line_string(Some(multi_line_string));
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given MultiPolygon
    #[inline]
    fn add_multi_polygon(
        &mut self,
        multi_polygon: Option<&impl MultiPolygonTrait>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_polygon) = multi_polygon {
            self.multi_polygon(multi_polygon.dim().try_into()?)
                .add_multi_polygon(Some(multi_polygon));
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given Geometry
    #[inline]
    pub fn add_geometry<T: WktNum>(
        &mut self,
        geom: Option<&impl GeometryTrait<T = T>>,
    ) -> GeoArrowResult<()> {
        use geo_traits::GeometryType;

        if let Some(geom) = geom {
            match geom.as_type() {
                GeometryType::Point(g) => self.add_point(Some(g)),
                GeometryType::LineString(g) => self.add_line_string(Some(g)),
                GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                GeometryType::MultiPoint(p) => self.add_multi_point(Some(p)),
                GeometryType::MultiLineString(p) => self.add_multi_line_string(Some(p)),
                GeometryType::MultiPolygon(p) => self.add_multi_polygon(Some(p)),
                GeometryType::GeometryCollection(p) => self.add_geometry_collection(Some(p)),
                GeometryType::Rect(r) => self.add_polygon(Some(&RectWrapper::try_new(r)?)),
                GeometryType::Triangle(tri) => self.add_polygon(Some(&TriangleWrapper(tri))),
                GeometryType::Line(l) => self.add_line_string(Some(&LineWrapper(l))),
            }?;
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given GeometryCollection
    #[inline]
    fn add_geometry_collection<T: WktNum>(
        &mut self,
        gc: Option<&impl GeometryCollectionTrait<T = T>>,
    ) -> GeoArrowResult<()> {
        if let Some(gc) = gc {
            self.gcs[Dimension::try_from(gc.dim())?.order()].add_geometry_collection(Some(gc))?;
        } else {
            self.nulls += 1;
        };
        Ok(())
    }

    /// Construct a new counter pre-filled with the given geometries
    pub fn from_geometries<'a, T: WktNum>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = T> + 'a)>>,
    ) -> GeoArrowResult<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom)?;
        }
        Ok(counter)
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes(&self) -> usize {
        let mut count = 0;

        self.points
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c * Dimension::from_order(idx).unwrap().size() * 8);
        self.line_strings
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));
        self.polygons
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));
        self.mpoints
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));
        self.mline_strings
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));
        self.mpolygons
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));
        self.gcs
            .iter()
            .enumerate()
            .for_each(|(idx, c)| count += c.num_bytes(Dimension::from_order(idx).unwrap()));

        count
    }
}

impl AddAssign for GeometryCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.nulls += rhs.nulls;

        self.points = core::array::from_fn(|i| self.points[i] + rhs.points[i]);
        self.line_strings = core::array::from_fn(|i| self.line_strings[i] + rhs.line_strings[i]);
        self.polygons = core::array::from_fn(|i| self.polygons[i] + rhs.polygons[i]);
        self.mpoints = core::array::from_fn(|i| self.mpoints[i] + rhs.mpoints[i]);
        self.mline_strings = core::array::from_fn(|i| self.mline_strings[i] + rhs.mline_strings[i]);
        self.mpolygons = core::array::from_fn(|i| self.mpolygons[i] + rhs.mpolygons[i]);
        self.gcs = core::array::from_fn(|i| self.gcs[i] + rhs.gcs[i]);
    }
}
