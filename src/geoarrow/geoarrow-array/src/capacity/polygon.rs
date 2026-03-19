use std::ops::Add;

use geo_traits::{GeometryTrait, GeometryType, LineStringTrait, PolygonTrait, RectTrait};
use geoarrow_schema::{
    Dimension,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::util::GeometryTypeName;

/// A counter for the buffer sizes of a [`PolygonArray`][crate::array::PolygonArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct PolygonCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl PolygonCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(coord_capacity: usize, ring_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            ring_capacity,
            geom_capacity,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0, 0)
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0 && self.ring_capacity == 0 && self.geom_capacity == 0
    }

    /// The coordinate buffer capacity
    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    /// The ring offset buffer capacity
    pub fn ring_capacity(&self) -> usize {
        self.ring_capacity
    }

    /// The geometry offset buffer capacity
    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    /// Add the capacity of the given Polygon
    #[inline]
    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(polygon) = polygon {
            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            self.ring_capacity += num_interiors + 1;

            // Number of coords for each ring
            if let Some(exterior) = polygon.exterior() {
                self.coord_capacity += exterior.num_coords();
            }

            for int_ring in polygon.interiors() {
                self.coord_capacity += int_ring.num_coords();
            }
        }
    }

    /// Add the capacity of the given Rect
    #[inline]
    pub fn add_rect<'a>(&mut self, rect: Option<&'a (impl RectTrait + 'a)>) {
        self.geom_capacity += 1;
        if rect.is_some() {
            // A rect is a simple polygon with only one ring
            self.ring_capacity += 1;
            // A rect is a closed polygon with 5 coordinates
            self.coord_capacity += 5;
        }
    }

    /// Add the capacity of the given Geometry
    ///
    /// The type of the geometry must be either Polygon or Rect
    #[inline]
    pub fn add_geometry(&mut self, value: Option<&impl GeometryTrait>) -> GeoArrowResult<()> {
        if let Some(geom) = value {
            match geom.as_type() {
                GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                GeometryType::Rect(g) => self.add_rect(Some(g)),
                gt => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "Expected polygon, got {}",
                        gt.name()
                    )));
                }
            }
        } else {
            self.geom_capacity += 1;
        };
        Ok(())
    }

    /// Construct a new counter pre-filled with the given Polygons
    pub fn from_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_polygon in geoms.into_iter() {
            counter.add_polygon(maybe_polygon);
        }
        counter
    }

    /// Construct a new counter pre-filled with the given Rects
    pub fn from_rects<'a>(geoms: impl Iterator<Item = Option<&'a (impl RectTrait + 'a)>>) -> Self {
        let mut counter = Self::new_empty();
        for maybe_rect in geoms.into_iter() {
            counter.add_rect(maybe_rect);
        }
        counter
    }

    /// Construct a new counter pre-filled with the given geometries
    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> GeoArrowResult<Self> {
        let mut counter = Self::new_empty();
        for g in geoms.into_iter() {
            counter.add_geometry(g)?;
        }
        Ok(counter)
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes(&self, dim: Dimension) -> usize {
        let offsets_byte_width = 4;
        let num_offsets = self.geom_capacity + self.ring_capacity;
        (offsets_byte_width * num_offsets) + (self.coord_capacity * dim.size() * 8)
    }
}

impl Default for PolygonCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for PolygonCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let coord_capacity = self.coord_capacity + rhs.coord_capacity;
        let ring_capacity = self.ring_capacity + rhs.ring_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;
        Self::new(coord_capacity, ring_capacity, geom_capacity)
    }
}
