use std::{collections::HashSet, sync::Arc};

use arrow_array::{Array, ArrayRef, UnionArray, cast::AsArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, UnionMode};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryCollectionType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArrayAccessor,
    array::{
        DimensionIndex, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
        PointArray, PolygonArray,
    },
    builder::{
        LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
        PointBuilder, PolygonBuilder,
    },
    capacity::MixedCapacity,
    scalar::Geometry,
    trait_::GeoArrowArray,
};

/// # Invariants
///
/// - All arrays must have the same dimension
/// - All arrays must have the same coordinate layout (interleaved or separated)
///
/// - 1: Point
/// - 2: LineString
/// - 3: Polygon
/// - 4: MultiPoint
/// - 5: MultiLineString
/// - 6: MultiPolygon
/// - 7: GeometryCollection
/// - 11: Point Z
/// - 12: LineString Z
/// - 13: Polygon Z
/// - 14: MultiPoint Z
/// - 15: MultiLineString Z
/// - 16: MultiPolygon Z
/// - 17: GeometryCollection Z
/// - 21: Point M
/// - 22: LineString M
/// - 23: Polygon M
/// - 24: MultiPoint M
/// - 25: MultiLineString M
/// - 26: MultiPolygon M
/// - 27: GeometryCollection M
/// - 31: Point ZM
/// - 32: LineString ZM
/// - 33: Polygon ZM
/// - 34: MultiPoint ZM
/// - 35: MultiLineString ZM
/// - 36: MultiPolygon ZM
/// - 37: GeometryCollection ZM
#[derive(Debug, Clone)]
pub struct MixedGeometryArray {
    pub(crate) coord_type: CoordType,
    pub(crate) dim: Dimension,

    /// Invariant: every item in `type_ids` is `> 0 && < fields.len()` if `type_ids` are not provided.
    pub(crate) type_ids: ScalarBuffer<i8>,

    /// Invariant: `offsets.len() == type_ids.len()`
    pub(crate) offsets: ScalarBuffer<i32>,

    /// Invariant: Any of these arrays that are `Some()` must have length >0
    pub(crate) points: PointArray,
    pub(crate) line_strings: LineStringArray,
    pub(crate) polygons: PolygonArray,
    pub(crate) multi_points: MultiPointArray,
    pub(crate) multi_line_strings: MultiLineStringArray,
    pub(crate) multi_polygons: MultiPolygonArray,

    /// We don't need a separate slice_length, because that's the length of the full
    /// MixedGeometryArray
    slice_offset: usize,
}

impl MixedGeometryArray {
    /// Create a new MixedGeometryArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        type_ids: ScalarBuffer<i8>,
        offsets: ScalarBuffer<i32>,
        points: Option<PointArray>,
        line_strings: Option<LineStringArray>,
        polygons: Option<PolygonArray>,
        multi_points: Option<MultiPointArray>,
        multi_line_strings: Option<MultiLineStringArray>,
        multi_polygons: Option<MultiPolygonArray>,
    ) -> Self {
        let mut coord_types = HashSet::new();
        if let Some(points) = &points {
            coord_types.insert(points.data_type.coord_type());
        }
        if let Some(line_strings) = &line_strings {
            coord_types.insert(line_strings.data_type.coord_type());
        }
        if let Some(polygons) = &polygons {
            coord_types.insert(polygons.data_type.coord_type());
        }
        if let Some(multi_points) = &multi_points {
            coord_types.insert(multi_points.data_type.coord_type());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            coord_types.insert(multi_line_strings.data_type.coord_type());
        }
        if let Some(multi_polygons) = &multi_polygons {
            coord_types.insert(multi_polygons.data_type.coord_type());
        }
        assert!(coord_types.len() <= 1);
        let coord_type = coord_types
            .into_iter()
            .next()
            .unwrap_or(CoordType::Interleaved);

        let mut dimensions = HashSet::new();
        if let Some(points) = &points {
            dimensions.insert(points.data_type.dimension());
        }
        if let Some(line_strings) = &line_strings {
            dimensions.insert(line_strings.data_type.dimension());
        }
        if let Some(polygons) = &polygons {
            dimensions.insert(polygons.data_type.dimension());
        }
        if let Some(multi_points) = &multi_points {
            dimensions.insert(multi_points.data_type.dimension());
        }
        if let Some(multi_line_strings) = &multi_line_strings {
            dimensions.insert(multi_line_strings.data_type.dimension());
        }
        if let Some(multi_polygons) = &multi_polygons {
            dimensions.insert(multi_polygons.data_type.dimension());
        }
        assert_eq!(dimensions.len(), 1);
        let dim = dimensions.into_iter().next().unwrap();

        Self {
            coord_type,
            dim,
            type_ids,
            offsets,
            points: points.unwrap_or(
                PointBuilder::new(
                    PointType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            line_strings: line_strings.unwrap_or(
                LineStringBuilder::new(
                    LineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            polygons: polygons.unwrap_or(
                PolygonBuilder::new(
                    PolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            multi_points: multi_points.unwrap_or(
                MultiPointBuilder::new(
                    MultiPointType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            multi_line_strings: multi_line_strings.unwrap_or(
                MultiLineStringBuilder::new(
                    MultiLineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            multi_polygons: multi_polygons.unwrap_or(
                MultiPolygonBuilder::new(
                    MultiPolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                )
                .finish(),
            ),
            slice_offset: 0,
        }
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MixedCapacity {
        MixedCapacity::new(
            self.points.buffer_lengths(),
            self.line_strings.buffer_lengths(),
            self.polygons.buffer_lengths(),
            self.multi_points.buffer_lengths(),
            self.multi_line_strings.buffer_lengths(),
            self.multi_polygons.buffer_lengths(),
        )
    }

    /// Return `true` if this array has been sliced.
    pub(crate) fn is_sliced(&self) -> bool {
        // Note this is still not a valid check, because it could've been sliced with start 0 but
        // length less than the full length.
        // self.slice_offset > 0 || self.slice_length

        let mut child_lengths = 0;
        child_lengths += self.points.len();
        child_lengths += self.line_strings.len();
        child_lengths += self.polygons.len();
        child_lengths += self.multi_points.len();
        child_lengths += self.multi_line_strings.len();
        child_lengths += self.multi_polygons.len();

        child_lengths > self.len()
    }

    pub fn has_points(&self) -> bool {
        if self.points.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == PointType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_line_strings(&self) -> bool {
        if self.line_strings.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == LineStringType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_polygons(&self) -> bool {
        if self.polygons.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == PolygonType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_points(&self) -> bool {
        if self.multi_points.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == MultiPointType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_line_strings(&self) -> bool {
        if self.multi_line_strings.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == MultiLineStringType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_multi_polygons(&self) -> bool {
        if self.multi_polygons.is_empty() {
            return false;
        }

        // If the array has been sliced, check a point type id still exists
        if self.is_sliced() {
            for t in self.type_ids.iter() {
                if *t % 10 == MultiPolygonType::GEOMETRY_TYPE_OFFSET {
                    return true;
                }
            }

            return false;
        }

        true
    }

    pub fn has_only_points(&self) -> bool {
        self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_line_strings(&self) -> bool {
        !self.has_points()
            && self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_polygons(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_points(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_line_strings(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && self.has_multi_line_strings()
            && !self.has_multi_polygons()
    }

    pub fn has_only_multi_polygons(&self) -> bool {
        !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && self.has_multi_polygons()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        self.buffer_lengths().num_bytes(self.dim)
    }

    /// Slice this [`MixedGeometryArray`].
    ///
    /// # Implementation
    ///
    /// This operation is `O(F)` where `F` is the number of fields.
    ///
    /// # Panic
    ///
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coord_type: self.coord_type,
            dim: self.dim,
            type_ids: self.type_ids.slice(offset, length),
            offsets: self.offsets.slice(offset, length),
            points: self.points.clone(),
            line_strings: self.line_strings.clone(),
            polygons: self.polygons.clone(),
            multi_points: self.multi_points.clone(),
            multi_line_strings: self.multi_line_strings.clone(),
            multi_polygons: self.multi_polygons.clone(),
            slice_offset: self.slice_offset + offset,
        }
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self {
            coord_type,
            points: self.points.into_coord_type(coord_type),
            line_strings: self.line_strings.into_coord_type(coord_type),
            polygons: self.polygons.into_coord_type(coord_type),
            multi_points: self.multi_points.into_coord_type(coord_type),
            multi_line_strings: self.multi_line_strings.into_coord_type(coord_type),
            multi_polygons: self.multi_polygons.into_coord_type(coord_type),
            ..self
        }
    }

    pub fn contained_types(&self) -> HashSet<GeoArrowType> {
        let mut types = HashSet::new();
        if self.has_points() {
            types.insert(self.points.data_type());
        }
        if self.has_line_strings() {
            types.insert(self.line_strings.data_type());
        }
        if self.has_polygons() {
            types.insert(self.polygons.data_type());
        }
        if self.has_multi_points() {
            types.insert(self.multi_points.data_type());
        }
        if self.has_multi_line_strings() {
            types.insert(self.multi_line_strings.data_type());
        }
        if self.has_multi_polygons() {
            types.insert(self.multi_polygons.data_type());
        }

        types
    }

    pub(crate) fn storage_type(&self) -> DataType {
        match GeometryCollectionType::new(self.dim, Default::default())
            .with_coord_type(self.coord_type)
            .data_type()
        {
            DataType::LargeList(inner_field) => inner_field.data_type().clone(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn into_array_ref(self) -> ArrayRef {
        Arc::new(UnionArray::from(self))
    }

    #[inline]
    fn len(&self) -> usize {
        // Note that `type_ids` is sliced as usual, and thus always has the correct length.
        self.type_ids.len()
    }

    // Note: this is copied from ArrayAccessor because MixedGeometryArray doesn't implement
    // GeoArrowArray
    pub(crate) unsafe fn value_unchecked(&self, index: usize) -> Geometry<'_> {
        let type_id = self.type_ids[index];
        let offset = self.offsets[index] as usize;

        let expect_msg = "native geometry value access should never error";
        match type_id % 10 {
            PointType::GEOMETRY_TYPE_OFFSET => {
                Geometry::Point(self.points.value(offset).expect(expect_msg))
            }
            LineStringType::GEOMETRY_TYPE_OFFSET => {
                Geometry::LineString(self.line_strings.value(offset).expect(expect_msg))
            }
            PolygonType::GEOMETRY_TYPE_OFFSET => {
                Geometry::Polygon(self.polygons.value(offset).expect(expect_msg))
            }
            MultiPointType::GEOMETRY_TYPE_OFFSET => {
                Geometry::MultiPoint(self.multi_points.value(offset).expect(expect_msg))
            }
            MultiLineStringType::GEOMETRY_TYPE_OFFSET => {
                Geometry::MultiLineString(self.multi_line_strings.value(offset).expect(expect_msg))
            }
            MultiPolygonType::GEOMETRY_TYPE_OFFSET => {
                Geometry::MultiPolygon(self.multi_polygons.value(offset).expect(expect_msg))
            }
            GeometryCollectionType::GEOMETRY_TYPE_OFFSET => {
                panic!("nested geometry collections not supported in GeoArrow")
            }
            _ => unreachable!("unknown type_id {}", type_id),
        }
    }

    // Note: this is copied from ArrayAccessor because MixedGeometryArray doesn't implement
    // GeoArrowArray
    pub(crate) fn value(&self, index: usize) -> Geometry<'_> {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }
}

impl From<MixedGeometryArray> for UnionArray {
    fn from(value: MixedGeometryArray) -> Self {
        let union_fields = match value.storage_type() {
            DataType::Union(union_fields, _) => union_fields,
            _ => unreachable!(),
        };

        let child_arrays = vec![
            value.points.into_array_ref(),
            value.line_strings.into_array_ref(),
            value.polygons.into_array_ref(),
            value.multi_points.into_array_ref(),
            value.multi_line_strings.into_array_ref(),
            value.multi_polygons.into_array_ref(),
        ];

        UnionArray::try_new(
            union_fields,
            value.type_ids,
            Some(value.offsets),
            child_arrays,
        )
        .unwrap()
    }
}

impl TryFrom<(&UnionArray, Dimension, CoordType)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(
        (value, dim, coord_type): (&UnionArray, Dimension, CoordType),
    ) -> GeoArrowResult<Self> {
        let mut points: Option<PointArray> = None;
        let mut line_strings: Option<LineStringArray> = None;
        let mut polygons: Option<PolygonArray> = None;
        let mut multi_points: Option<MultiPointArray> = None;
        let mut multi_line_strings: Option<MultiLineStringArray> = None;
        let mut multi_polygons: Option<MultiPolygonArray> = None;

        match value.data_type() {
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    return Err(GeoArrowError::InvalidGeoArrow(
                        "Expected dense union".to_string(),
                    ));
                }

                for (type_id, _field) in fields.iter() {
                    let found_dimension = Dimension::from_order((type_id / 10) as _)?;

                    if dim != found_dimension {
                        return Err(GeoArrowError::InvalidGeoArrow(format!(
                            "expected dimension: {dim:?}, found child array with dimension {found_dimension:?} and type_id: {type_id}",
                        )));
                    }

                    match type_id % 10 {
                        PointType::GEOMETRY_TYPE_OFFSET => {
                            points = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PointType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        LineStringType::GEOMETRY_TYPE_OFFSET => {
                            line_strings = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    LineStringType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        PolygonType::GEOMETRY_TYPE_OFFSET => {
                            polygons = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    PolygonType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        MultiPointType::GEOMETRY_TYPE_OFFSET => {
                            multi_points = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPointType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        MultiLineStringType::GEOMETRY_TYPE_OFFSET => {
                            multi_line_strings = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiLineStringType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        MultiPolygonType::GEOMETRY_TYPE_OFFSET => {
                            multi_polygons = Some(
                                (
                                    value.child(type_id).as_ref(),
                                    MultiPolygonType::new(dim, Default::default())
                                        .with_coord_type(coord_type),
                                )
                                    .try_into()
                                    .unwrap(),
                            );
                        }
                        _ => {
                            return Err(GeoArrowError::InvalidGeoArrow(format!(
                                "Unexpected type_id {type_id} when converting to MixedGeometryArray",
                            )));
                        }
                    }
                }
            }
            _ => {
                return Err(GeoArrowError::InvalidGeoArrow(
                    "expected union type when converting to MixedGeometryArray".to_string(),
                ));
            }
        };

        let type_ids = value.type_ids().clone();
        // This is after checking for dense union
        let offsets = value.offsets().unwrap().clone();

        Ok(Self::new(
            type_ids,
            offsets,
            points,
            line_strings,
            polygons,
            multi_points,
            multi_line_strings,
            multi_polygons,
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension, CoordType)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(
        (value, dim, coord_type): (&dyn Array, Dimension, CoordType),
    ) -> GeoArrowResult<Self> {
        match value.data_type() {
            DataType::Union(_, _) => (value.as_union(), dim, coord_type).try_into(),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected MixedGeometryArray DataType: {dt:?}",
            ))),
        }
    }
}

impl PartialEq for MixedGeometryArray {
    fn eq(&self, other: &Self) -> bool {
        self.dim == other.dim
            && self.type_ids == other.type_ids
            && self.offsets == other.offsets
            && self.points == other.points
            && self.line_strings == other.line_strings
            && self.polygons == other.polygons
            && self.multi_points == other.multi_points
            && self.multi_line_strings == other.multi_line_strings
            && self.multi_polygons == other.multi_polygons
            && self.slice_offset == other.slice_offset
    }
}
