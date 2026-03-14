/// Two argument version of the `downcast_geoarrow_array!` macro.
/// Downcast any combination of two [GeoArrowArray][geoarrow_array::GeoArrowArray] to a concrete-typed array based on its [`GeoArrowType`][geoarrow_schema::GeoArrowType].
///
/// This is in private utils in geoarrow-geo because we don't yet have a stable API for this macro.
macro_rules! downcast_geoarrow_array_two_args {
    ($array1:ident, $array2:ident, $fn:expr $(, $args:expr )* $(,)?) => {
        match $array1.data_type() {
            geoarrow_schema::GeoArrowType::Point(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::LineString(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::Polygon(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::MultiPoint(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::MultiLineString(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::MultiPolygon(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::Geometry(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::GeometryCollection(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::Rect(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::Wkb(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::LargeWkb(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::WkbView(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::Wkt(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::LargeWkt(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
            geoarrow_schema::GeoArrowType::WktView(_) => match $array2.data_type() {
                geoarrow_schema::GeoArrowType::Point(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Polygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPoint(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_point($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiLineString(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_line_string($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::MultiPolygon(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_multi_polygon($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Geometry(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::GeometryCollection(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_geometry_collection($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Rect(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_rect($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkb(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WkbView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkb_view($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::Wkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i32>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::LargeWkt(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt::<i64>($array2)
                    $(, $args )*
                ),
                geoarrow_schema::GeoArrowType::WktView(_) => $fn(
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array1),
                    geoarrow_array::cast::AsGeoArrowArray::as_wkt_view($array2)
                    $(, $args )*
                ),
            },
        }
    };
}

pub(crate) use downcast_geoarrow_array_two_args;

// #[cfg(test)]
// mod tests {
//     use geoarrow_array::GeoArrowArray;
//     use geoarrow_schema::error::GeoArrowResult;

//     // Ensure macro gets called, so an error will appear to ensure exhaustiveness
//     #[allow(dead_code)]
//     fn _test_two_args_macro_exhaustiveness(
//         arr1: &dyn GeoArrowArray,
//         arr2: &dyn GeoArrowArray,
//     ) -> GeoArrowResult<()> {
//         downcast_geoarrow_array_two_args!(arr1, arr2, |_a1, _a2| Ok(()))
//     }
// }
