//! Contains the implementation of [`GeoArrowType`], which defines all geometry arrays in this
//! crate.

use std::sync::Arc;

use arrow_schema::{DataType, Field, extension::ExtensionType};

use crate::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
    WktType,
    error::{GeoArrowError, GeoArrowResult},
};

/// Geospatial data types supported by GeoArrow.
///
/// The variants of this enum include all possible GeoArrow geometry types, including both "native"
/// and "serialized" encodings.
///
/// Each variant uniquely identifies the physical buffer layout for the respective array type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GeoArrowType {
    /// A Point.
    Point(PointType),

    /// A LineString.
    LineString(LineStringType),

    /// A Polygon.
    Polygon(PolygonType),

    /// A MultiPoint.
    MultiPoint(MultiPointType),

    /// A MultiLineString.
    MultiLineString(MultiLineStringType),

    /// A MultiPolygon.
    MultiPolygon(MultiPolygonType),

    /// A GeometryCollection.
    GeometryCollection(GeometryCollectionType),

    /// A Rect.
    Rect(BoxType),

    /// A Geometry with unknown types or dimensions.
    Geometry(GeometryType),

    /// A WKB stored in a `BinaryArray` with `i32` offsets.
    Wkb(WkbType),

    /// A WKB stored in a `LargeBinaryArray` with `i64` offsets.
    LargeWkb(WkbType),

    /// A WKB stored in a `BinaryViewArray`.
    WkbView(WkbType),

    /// A WKT stored in a `StringArray` with `i32` offsets.
    Wkt(WktType),

    /// A WKT stored in a `LargeStringArray` with `i64` offsets.
    LargeWkt(WktType),

    /// A WKT stored in a `StringViewArray`.
    WktView(WktType),
}

impl From<GeoArrowType> for DataType {
    fn from(value: GeoArrowType) -> Self {
        value.to_data_type()
    }
}

impl GeoArrowType {
    /// Get the [`CoordType`] of this data type.
    ///
    /// WKB and WKT variants will return `None`.
    pub fn coord_type(&self) -> Option<CoordType> {
        use GeoArrowType::*;
        match self {
            Point(t) => Some(t.coord_type()),
            LineString(t) => Some(t.coord_type()),
            Polygon(t) => Some(t.coord_type()),
            MultiPoint(t) => Some(t.coord_type()),
            MultiLineString(t) => Some(t.coord_type()),
            MultiPolygon(t) => Some(t.coord_type()),
            GeometryCollection(t) => Some(t.coord_type()),
            Rect(_) => Some(CoordType::Separated),
            Geometry(t) => Some(t.coord_type()),
            Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_) | WktView(_) => None,
        }
    }

    /// Get the [`Dimension`] of this data type, if it has one.
    ///
    /// [`Geometry`][Self::Geometry] and WKB and WKT variants will return `None`.
    pub fn dimension(&self) -> Option<Dimension> {
        use GeoArrowType::*;
        match self {
            Point(t) => Some(t.dimension()),
            LineString(t) => Some(t.dimension()),
            Polygon(t) => Some(t.dimension()),
            MultiPoint(t) => Some(t.dimension()),
            MultiLineString(t) => Some(t.dimension()),
            MultiPolygon(t) => Some(t.dimension()),
            GeometryCollection(t) => Some(t.dimension()),
            Rect(t) => Some(t.dimension()),
            Geometry(_) | Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_) | WktView(_) => {
                None
            }
        }
    }

    /// Returns the [Metadata] contained within this type.
    pub fn metadata(&self) -> &Arc<Metadata> {
        use GeoArrowType::*;
        match self {
            Point(t) => t.metadata(),
            LineString(t) => t.metadata(),
            Polygon(t) => t.metadata(),
            MultiPoint(t) => t.metadata(),
            MultiLineString(t) => t.metadata(),
            MultiPolygon(t) => t.metadata(),
            GeometryCollection(t) => t.metadata(),
            Rect(t) => t.metadata(),
            Geometry(t) => t.metadata(),
            Wkb(t) | LargeWkb(t) | WkbView(t) => t.metadata(),
            Wkt(t) | LargeWkt(t) | WktView(t) => t.metadata(),
        }
    }
    /// Converts a [`GeoArrowType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_schema::DataType;
    /// # use geoarrow_schema::{Dimension, GeoArrowType, PointType};
    /// #
    /// let point_type = PointType::new(Dimension::XY, Default::default());
    /// let data_type = GeoArrowType::Point(point_type).to_data_type();
    /// assert!(matches!(data_type, DataType::Struct(_)));
    /// ```
    pub fn to_data_type(&self) -> DataType {
        use GeoArrowType::*;
        match self {
            Point(t) => t.data_type(),
            LineString(t) => t.data_type(),
            Polygon(t) => t.data_type(),
            MultiPoint(t) => t.data_type(),
            MultiLineString(t) => t.data_type(),
            MultiPolygon(t) => t.data_type(),
            GeometryCollection(t) => t.data_type(),
            Rect(t) => t.data_type(),
            Geometry(t) => t.data_type(),
            Wkb(_) => DataType::Binary,
            LargeWkb(_) => DataType::LargeBinary,
            WkbView(_) => DataType::BinaryView,
            Wkt(_) => DataType::Utf8,
            LargeWkt(_) => DataType::LargeUtf8,
            WktView(_) => DataType::Utf8View,
        }
    }

    /// Converts this [`GeoArrowType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_schema::{Dimension, GeoArrowType, PointType};
    /// #
    /// let point_type = PointType::new(Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let field = geoarrow_type.to_field("geometry", true);
    /// assert_eq!(field.name(), "geometry");
    /// assert!(field.is_nullable());
    /// assert_eq!(field.metadata()["ARROW:extension:name"], "geoarrow.point");
    /// ```
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        use GeoArrowType::*;
        match self {
            Point(t) => t.to_field(name, nullable),
            LineString(t) => t.to_field(name, nullable),
            Polygon(t) => t.to_field(name, nullable),
            MultiPoint(t) => t.to_field(name, nullable),
            MultiLineString(t) => t.to_field(name, nullable),
            MultiPolygon(t) => t.to_field(name, nullable),
            GeometryCollection(t) => t.to_field(name, nullable),
            Rect(t) => t.to_field(name, nullable),
            Geometry(t) => t.to_field(name, nullable),
            Wkb(t) | LargeWkb(t) | WkbView(t) => {
                Field::new(name, self.to_data_type(), nullable).with_extension_type(t.clone())
            }
            Wkt(t) | LargeWkt(t) | WktView(t) => {
                Field::new(name, self.to_data_type(), nullable).with_extension_type(t.clone())
            }
        }
    }

    /// Applies the provided [CoordType] onto self.
    ///
    /// [`Rect`][Self::Rect] and WKB and WKT variants will return the same type as they do not have
    /// a parameterized coordinate types.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_schema::{CoordType, Dimension, GeoArrowType, PointType};
    /// #
    /// let point_type = PointType::new(Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let new_type = geoarrow_type.with_coord_type(CoordType::Separated);
    ///
    /// assert_eq!(new_type.coord_type(), Some(CoordType::Separated));
    /// ```
    pub fn with_coord_type(self, coord_type: CoordType) -> GeoArrowType {
        use GeoArrowType::*;
        match self {
            Point(t) => Point(t.with_coord_type(coord_type)),
            LineString(t) => LineString(t.with_coord_type(coord_type)),
            Polygon(t) => Polygon(t.with_coord_type(coord_type)),
            MultiPoint(t) => MultiPoint(t.with_coord_type(coord_type)),
            MultiLineString(t) => MultiLineString(t.with_coord_type(coord_type)),
            MultiPolygon(t) => MultiPolygon(t.with_coord_type(coord_type)),
            GeometryCollection(t) => GeometryCollection(t.with_coord_type(coord_type)),
            Rect(t) => Rect(t),
            Geometry(t) => Geometry(t.with_coord_type(coord_type)),
            _ => self,
        }
    }

    /// Applies the provided [Dimension] onto self.
    ///
    /// [`Geometry`][Self::Geometry] and WKB and WKT variants will return the same type as they do
    /// not have a parameterized dimension.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_schema::{Dimension, GeoArrowType, PointType};
    /// #
    /// let point_type = PointType::new(Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let new_type = geoarrow_type.with_dimension(Dimension::XYZ);
    ///
    /// assert_eq!(new_type.dimension(), Some(Dimension::XYZ));
    /// ```
    pub fn with_dimension(self, dim: Dimension) -> GeoArrowType {
        use GeoArrowType::*;
        match self {
            Point(t) => Point(t.with_dimension(dim)),
            LineString(t) => LineString(t.with_dimension(dim)),
            Polygon(t) => Polygon(t.with_dimension(dim)),
            MultiPoint(t) => MultiPoint(t.with_dimension(dim)),
            MultiLineString(t) => MultiLineString(t.with_dimension(dim)),
            MultiPolygon(t) => MultiPolygon(t.with_dimension(dim)),
            GeometryCollection(t) => GeometryCollection(t.with_dimension(dim)),
            Rect(t) => Rect(t.with_dimension(dim)),
            Geometry(t) => Geometry(t),
            _ => self,
        }
    }

    /// Applies the provided [Metadata] onto self.
    pub fn with_metadata(self, meta: Arc<Metadata>) -> GeoArrowType {
        use GeoArrowType::*;
        match self {
            Point(t) => Point(t.with_metadata(meta)),
            LineString(t) => LineString(t.with_metadata(meta)),
            Polygon(t) => Polygon(t.with_metadata(meta)),
            MultiPoint(t) => MultiPoint(t.with_metadata(meta)),
            MultiLineString(t) => MultiLineString(t.with_metadata(meta)),
            MultiPolygon(t) => MultiPolygon(t.with_metadata(meta)),
            GeometryCollection(t) => GeometryCollection(t.with_metadata(meta)),
            Rect(t) => Rect(t.with_metadata(meta)),
            Geometry(t) => Geometry(t.with_metadata(meta)),
            Wkb(t) => Wkb(t.with_metadata(meta)),
            LargeWkb(t) => LargeWkb(t.with_metadata(meta)),
            WkbView(t) => WkbView(t.with_metadata(meta)),
            Wkt(t) => Wkt(t.with_metadata(meta)),
            LargeWkt(t) => LargeWkt(t.with_metadata(meta)),
            WktView(t) => WktView(t.with_metadata(meta)),
        }
    }

    /// Create a new [`GeoArrowType`] from an Arrow [`Field`], requiring GeoArrow metadata to be
    /// set.
    ///
    /// If the field does not have at least a GeoArrow extension name, an error will be returned.
    ///
    /// Create a new [`GeoArrowType`] from an Arrow [`Field`].
    ///
    /// This method requires GeoArrow metadata to be correctly set. If you wish to allow data type
    /// coercion without GeoArrow metadata, use [`GeoArrowType::from_arrow_field`] instead.
    ///
    /// - An `Ok(Some(_))` return value indicates that the field has valid GeoArrow extension metadata, and thus was able to match to a specific GeoArrow type.
    /// - An `Ok(None)` return value indicates that the field either does not have any Arrow extension name or the extension name is not a GeoArrow extension name.
    /// - An `Err` return value indicates that the field has a GeoArrow extension name, but it is
    ///   invalid. This can happen if the field's [`DataType`] is not compatible with the allowed
    ///   types for the given GeoArrow type, or if the GeoArrow metadata is malformed.
    pub fn from_extension_field(field: &Field) -> GeoArrowResult<Option<Self>> {
        if let Some(extension_name) = field.extension_type_name() {
            use GeoArrowType::*;
            let data_type = match extension_name {
                PointType::NAME => Point(field.try_extension_type()?),
                LineStringType::NAME => LineString(field.try_extension_type()?),
                PolygonType::NAME => Polygon(field.try_extension_type()?),
                MultiPointType::NAME => MultiPoint(field.try_extension_type()?),
                MultiLineStringType::NAME => MultiLineString(field.try_extension_type()?),
                MultiPolygonType::NAME => MultiPolygon(field.try_extension_type()?),
                GeometryCollectionType::NAME => GeometryCollection(field.try_extension_type()?),
                BoxType::NAME => Rect(field.try_extension_type()?),
                GeometryType::NAME => Geometry(field.try_extension_type()?),
                WkbType::NAME => match field.data_type() {
                    DataType::Binary => Wkb(field.try_extension_type()?),
                    DataType::LargeBinary => LargeWkb(field.try_extension_type()?),
                    DataType::BinaryView => WkbView(field.try_extension_type()?),
                    _ => {
                        return Err(GeoArrowError::InvalidGeoArrow(format!(
                            "Expected binary type for a field with extension name 'geoarrow.wkb', got '{}'",
                            field.data_type()
                        )));
                    }
                },
                WktType::NAME => match field.data_type() {
                    DataType::Utf8 => Wkt(field.try_extension_type()?),
                    DataType::LargeUtf8 => LargeWkt(field.try_extension_type()?),
                    DataType::Utf8View => WktView(field.try_extension_type()?),
                    _ => {
                        return Err(GeoArrowError::InvalidGeoArrow(format!(
                            "Expected string type for a field with extension name 'geoarrow.wkt', got '{}'",
                            field.data_type()
                        )));
                    }
                },
                _ => return Ok(None),
            };
            Ok(Some(data_type))
        } else {
            Ok(None)
        }
    }

    /// Create a new [`GeoArrowType`] from an Arrow [`Field`], inferring the GeoArrow type if
    /// GeoArrow metadata is not present.
    ///
    /// This will first try [`GeoArrowType::from_extension_field`], and if that fails, will try to
    /// infer the GeoArrow type from the field's [DataType]. This only works for Point, WKB, and
    /// WKT types, as those are the only types that can be unambiguously inferred from an Arrow
    /// [DataType].
    pub fn from_arrow_field(field: &Field) -> GeoArrowResult<Self> {
        use GeoArrowType::*;
        if let Some(geo_type) = Self::from_extension_field(field)? {
            Ok(geo_type)
        } else {
            let metadata = Arc::new(Metadata::try_from(field)?);
            let data_type = match field.data_type() {
                DataType::Struct(struct_fields) => {
                    if !struct_fields.iter().all(|f| matches!(f.data_type(), DataType::Float64) ) {
                        return Err(GeoArrowError::InvalidGeoArrow("all struct fields must be Float64 when inferring point type.".to_string()));
                    }

                    match struct_fields.len() {
                        2 => GeoArrowType::Point(PointType::new( Dimension::XY, metadata).with_coord_type(CoordType::Separated)),
                        3 => GeoArrowType::Point(PointType::new( Dimension::XYZ, metadata).with_coord_type(CoordType::Separated)),
                        4 => GeoArrowType::Point(PointType::new( Dimension::XYZM, metadata).with_coord_type(CoordType::Separated)),
                        l => return Err(GeoArrowError::InvalidGeoArrow(format!("invalid number of struct fields: {l}"))),
                    }
                },
                DataType::FixedSizeList(inner_field, list_size) => {
                    if !matches!(inner_field.data_type(), DataType::Float64 )  {
                        return Err(GeoArrowError::InvalidGeoArrow(format!("invalid inner field type of fixed size list: {}", inner_field.data_type())));
                    }

                    match list_size {
                        2 => GeoArrowType::Point(PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Interleaved)),
                        3 => GeoArrowType::Point(PointType::new(Dimension::XYZ, metadata).with_coord_type(CoordType::Interleaved)),
                        4 => GeoArrowType::Point(PointType::new(Dimension::XYZM, metadata).with_coord_type(CoordType::Interleaved)),
                        _ => return Err(GeoArrowError::InvalidGeoArrow(format!("invalid list_size: {list_size}"))),
                    }
                },
                DataType::Binary => Wkb(WkbType::new(metadata)),
                DataType::LargeBinary => LargeWkb(WkbType::new(metadata)),
                DataType::BinaryView => WkbView(WkbType::new(metadata)),
                DataType::Utf8 => Wkt(WktType::new(metadata)),
                DataType::LargeUtf8 => LargeWkt(WktType::new(metadata)),
                DataType::Utf8View => WktView(WktType::new(metadata)),
                _ => return Err(GeoArrowError::InvalidGeoArrow("Only FixedSizeList, Struct, Binary, LargeBinary, BinaryView, String, LargeString, and StringView arrays are unambiguously typed for a GeoArrow type and can be used without extension metadata.\nEnsure your array input has GeoArrow metadata.".to_string())),
            };

            Ok(data_type)
        }
    }
}

macro_rules! impl_into_geoarrowtype {
    ($source_type:ident, $variant:expr) => {
        impl From<$source_type> for GeoArrowType {
            fn from(value: $source_type) -> Self {
                $variant(value)
            }
        }
    };
}

impl_into_geoarrowtype!(PointType, GeoArrowType::Point);
impl_into_geoarrowtype!(LineStringType, GeoArrowType::LineString);
impl_into_geoarrowtype!(PolygonType, GeoArrowType::Polygon);
impl_into_geoarrowtype!(MultiPointType, GeoArrowType::MultiPoint);
impl_into_geoarrowtype!(MultiLineStringType, GeoArrowType::MultiLineString);
impl_into_geoarrowtype!(MultiPolygonType, GeoArrowType::MultiPolygon);
impl_into_geoarrowtype!(GeometryCollectionType, GeoArrowType::GeometryCollection);
impl_into_geoarrowtype!(BoxType, GeoArrowType::Rect);
impl_into_geoarrowtype!(GeometryType, GeoArrowType::Geometry);

impl TryFrom<&Field> for GeoArrowType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> GeoArrowResult<Self> {
        if let Some(geo_type) = Self::from_extension_field(field)? {
            Ok(geo_type)
        } else {
            Err(GeoArrowError::InvalidGeoArrow(
                "Expected GeoArrow extension metadata, found none or unsupported extension."
                    .to_string(),
            ))
        }
    }
}
