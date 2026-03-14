//! Contains helpers for working with GeoArrow Type IDs.

use crate::{
    Dimension, GeometryCollectionType, LineStringType, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};

/// Compute the Type ID for an array type-dimension combination.
///
/// The GeoArrow specification defines a Type ID for each geometry type and dimension combination.
/// <https://geoarrow.org/format.html#geometry>
pub trait GeometryTypeId {
    /// The integer offset for this geometry type.
    ///
    /// This matches the 2D geometry type IDs defined in the GeoArrow specification. For example,
    /// Point is 1, LineString is 2, etc.
    const GEOMETRY_TYPE_OFFSET: i8;

    /// The dimension of this geometry type.
    fn dimension(&self) -> Dimension;

    /// The Type ID for this geometry type and dimension.
    fn geometry_type_id(&self) -> i8 {
        (dimension_order(self.dimension()) * 10) + Self::GEOMETRY_TYPE_OFFSET
    }
}

fn dimension_order(dim: Dimension) -> i8 {
    match dim {
        Dimension::XY => 0,
        Dimension::XYZ => 1,
        Dimension::XYM => 2,
        Dimension::XYZM => 3,
    }
}

impl GeometryTypeId for PointType {
    const GEOMETRY_TYPE_OFFSET: i8 = 1;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for LineStringType {
    const GEOMETRY_TYPE_OFFSET: i8 = 2;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for PolygonType {
    const GEOMETRY_TYPE_OFFSET: i8 = 3;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for MultiPointType {
    const GEOMETRY_TYPE_OFFSET: i8 = 4;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for MultiLineStringType {
    const GEOMETRY_TYPE_OFFSET: i8 = 5;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for MultiPolygonType {
    const GEOMETRY_TYPE_OFFSET: i8 = 6;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}

impl GeometryTypeId for GeometryCollectionType {
    const GEOMETRY_TYPE_OFFSET: i8 = 7;

    fn dimension(&self) -> Dimension {
        self.dimension()
    }
}
