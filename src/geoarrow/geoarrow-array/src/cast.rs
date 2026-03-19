//! Helper functions for downcasting [`dyn GeoArrowArray`][GeoArrowArray] to concrete types and for
//! converting between GeoArrow array representations.

use std::sync::Arc;

use arrow_array::{
    OffsetSizeTrait,
    builder::{BinaryViewBuilder, GenericByteBuilder, GenericStringBuilder, StringViewBuilder},
    cast::AsArray,
};
use geoarrow_schema::{
    GeoArrowType, WkbType,
    error::{GeoArrowError, GeoArrowResult},
};
use wkb::{Endianness, writer::WriteOptions};

use crate::{
    GeoArrowArrayAccessor, IntoArrow,
    array::*,
    builder::{
        GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
        MultiPointBuilder, MultiPolygonBuilder, PointBuilder, PolygonBuilder, WkbBuilder,
    },
    trait_::GeoArrowArray,
};

/// Helpers for downcasting a [`GeoArrowArray`] to a concrete implementation.
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Int32Array, RecordBatch};
/// use arrow_schema::{Schema, Field, DataType, ArrowError};
/// use geo_types::point;
///
/// use geoarrow_array::array::PointArray;
/// use geoarrow_array::builder::PointBuilder;
/// use geoarrow_array::cast::AsGeoArrowArray;
/// use geoarrow_array::GeoArrowArray;
/// use geo_traits::CoordTrait;
/// use geoarrow_schema::{Dimension, PointType};
///
/// let point1 = point!(x: 1., y: 2.);
/// let point2 = point!(x: 3., y: 4.);
/// let point3 = point!(x: 5., y: 6.);
/// let geoms = [point1, point2, point3];
///
/// let geom_type = PointType::new(Dimension::XY, Default::default());
/// let point_array = PointBuilder::from_points(geoms.iter(), geom_type).finish();
///
/// let generic_array: Arc<dyn GeoArrowArray> = Arc::new(point_array.clone());
///
/// let point_array2 = generic_array.as_point();
/// assert_eq!(&point_array, point_array2);
/// ```
pub trait AsGeoArrowArray {
    /// Downcast this to a [`PointArray`] returning `None` if not possible
    fn as_point_opt(&self) -> Option<&PointArray>;

    /// Downcast this to a [`PointArray`] panicking if not possible
    #[inline]
    fn as_point(&self) -> &PointArray {
        self.as_point_opt().unwrap()
    }

    /// Downcast this to a [`LineStringArray`] with `i32` offsets returning `None` if not possible
    fn as_line_string_opt(&self) -> Option<&LineStringArray>;

    /// Downcast this to a [`LineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_line_string(&self) -> &LineStringArray {
        self.as_line_string_opt().unwrap()
    }

    /// Downcast this to a [`PolygonArray`] with `i32` offsets returning `None` if not possible
    fn as_polygon_opt(&self) -> Option<&PolygonArray>;

    /// Downcast this to a [`PolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_polygon(&self) -> &PolygonArray {
        self.as_polygon_opt().unwrap()
    }

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets returning `None` if not possible
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray>;

    /// Downcast this to a [`MultiPointArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_point(&self) -> &MultiPointArray {
        self.as_multi_point_opt().unwrap()
    }

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray>;

    /// Downcast this to a [`MultiLineStringArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_line_string(&self) -> &MultiLineStringArray {
        self.as_multi_line_string_opt().unwrap()
    }

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray>;

    /// Downcast this to a [`MultiPolygonArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_multi_polygon(&self) -> &MultiPolygonArray {
        self.as_multi_polygon_opt().unwrap()
    }

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets returning `None` if not
    /// possible
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray>;

    /// Downcast this to a [`GeometryCollectionArray`] with `i32` offsets panicking if not possible
    #[inline]
    fn as_geometry_collection(&self) -> &GeometryCollectionArray {
        self.as_geometry_collection_opt().unwrap()
    }

    /// Downcast this to a [`RectArray`] returning `None` if not possible
    fn as_rect_opt(&self) -> Option<&RectArray>;

    /// Downcast this to a [`RectArray`] panicking if not possible
    #[inline]
    fn as_rect(&self) -> &RectArray {
        self.as_rect_opt().unwrap()
    }

    /// Downcast this to a [`GeometryArray`] returning `None` if not possible
    fn as_geometry_opt(&self) -> Option<&GeometryArray>;

    /// Downcast this to a [`GeometryArray`] panicking if not possible
    #[inline]
    fn as_geometry(&self) -> &GeometryArray {
        self.as_geometry_opt().unwrap()
    }

    /// Downcast this to a [`GenericWkbArray`] with `O` offsets returning `None` if not possible
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWkbArray<O>>;

    /// Downcast this to a [`GenericWkbArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkb<O: OffsetSizeTrait>(&self) -> &GenericWkbArray<O> {
        self.as_wkb_opt::<O>().unwrap()
    }

    /// Downcast this to a [`WkbViewArray`] returning `None` if not possible
    fn as_wkb_view_opt(&self) -> Option<&WkbViewArray>;

    /// Downcast this to a [`WkbViewArray`] panicking if not possible
    #[inline]
    fn as_wkb_view(&self) -> &WkbViewArray {
        self.as_wkb_view_opt().unwrap()
    }

    /// Downcast this to a [`GenericWktArray`] with `O` offsets returning `None` if not possible
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWktArray<O>>;

    /// Downcast this to a [`GenericWktArray`] with `O` offsets panicking if not possible
    #[inline]
    fn as_wkt<O: OffsetSizeTrait>(&self) -> &GenericWktArray<O> {
        self.as_wkt_opt::<O>().unwrap()
    }

    /// Downcast this to a [`WktViewArray`] returning `None` if not possible
    fn as_wkt_view_opt(&self) -> Option<&WktViewArray>;

    /// Downcast this to a [`WktViewArray`] panicking if not possible
    #[inline]
    fn as_wkt_view(&self) -> &WktViewArray {
        self.as_wkt_view_opt().unwrap()
    }
}

// `dyn GeoArrowArray + '_` is the same as upstream Arrow
impl AsGeoArrowArray for dyn GeoArrowArray + '_ {
    #[inline]
    fn as_point_opt(&self) -> Option<&PointArray> {
        self.as_any().downcast_ref::<PointArray>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&LineStringArray> {
        self.as_any().downcast_ref::<LineStringArray>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&PolygonArray> {
        self.as_any().downcast_ref::<PolygonArray>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray> {
        self.as_any().downcast_ref::<MultiPointArray>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray> {
        self.as_any().downcast_ref::<MultiLineStringArray>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray> {
        self.as_any().downcast_ref::<MultiPolygonArray>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray> {
        self.as_any().downcast_ref::<GeometryCollectionArray>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&RectArray> {
        self.as_any().downcast_ref::<RectArray>()
    }

    #[inline]
    fn as_geometry_opt(&self) -> Option<&GeometryArray> {
        self.as_any().downcast_ref::<GeometryArray>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWkbArray<O>> {
        self.as_any().downcast_ref::<GenericWkbArray<O>>()
    }

    #[inline]
    fn as_wkb_view_opt(&self) -> Option<&WkbViewArray> {
        self.as_any().downcast_ref::<WkbViewArray>()
    }

    #[inline]
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWktArray<O>> {
        self.as_any().downcast_ref::<GenericWktArray<O>>()
    }

    #[inline]
    fn as_wkt_view_opt(&self) -> Option<&WktViewArray> {
        self.as_any().downcast_ref::<WktViewArray>()
    }
}

impl AsGeoArrowArray for Arc<dyn GeoArrowArray> {
    #[inline]
    fn as_point_opt(&self) -> Option<&PointArray> {
        self.as_any().downcast_ref::<PointArray>()
    }

    #[inline]
    fn as_line_string_opt(&self) -> Option<&LineStringArray> {
        self.as_any().downcast_ref::<LineStringArray>()
    }

    #[inline]
    fn as_polygon_opt(&self) -> Option<&PolygonArray> {
        self.as_any().downcast_ref::<PolygonArray>()
    }

    #[inline]
    fn as_multi_point_opt(&self) -> Option<&MultiPointArray> {
        self.as_any().downcast_ref::<MultiPointArray>()
    }

    #[inline]
    fn as_multi_line_string_opt(&self) -> Option<&MultiLineStringArray> {
        self.as_any().downcast_ref::<MultiLineStringArray>()
    }

    #[inline]
    fn as_multi_polygon_opt(&self) -> Option<&MultiPolygonArray> {
        self.as_any().downcast_ref::<MultiPolygonArray>()
    }

    #[inline]
    fn as_geometry_collection_opt(&self) -> Option<&GeometryCollectionArray> {
        self.as_any().downcast_ref::<GeometryCollectionArray>()
    }

    #[inline]
    fn as_rect_opt(&self) -> Option<&RectArray> {
        self.as_any().downcast_ref::<RectArray>()
    }

    #[inline]
    fn as_geometry_opt(&self) -> Option<&GeometryArray> {
        self.as_any().downcast_ref::<GeometryArray>()
    }

    #[inline]
    fn as_wkb_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWkbArray<O>> {
        self.as_any().downcast_ref::<GenericWkbArray<O>>()
    }

    #[inline]
    fn as_wkb_view_opt(&self) -> Option<&WkbViewArray> {
        self.as_any().downcast_ref::<WkbViewArray>()
    }

    #[inline]
    fn as_wkt_opt<O: OffsetSizeTrait>(&self) -> Option<&GenericWktArray<O>> {
        self.as_any().downcast_ref::<GenericWktArray<O>>()
    }

    #[inline]
    fn as_wkt_view_opt(&self) -> Option<&WktViewArray> {
        self.as_any().downcast_ref::<WktViewArray>()
    }
}

/// Convert a [GeoArrowArray] to a [`GenericWkbArray`].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn GeoArrowArray) -> GeoArrowResult<GenericWkbArray<O>> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkb(arr.as_point()),
        LineString(_) => impl_to_wkb(arr.as_line_string()),
        Polygon(_) => impl_to_wkb(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkb(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkb(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkb(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkb(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkb(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkb(arr.as_rect()),
        Wkb(typ) => {
            // Note that here O is the _target_ offset type
            if O::IS_LARGE {
                // We need to convert from i32 to i64
                let large_arr: GenericWkbArray<i64> = arr.as_wkb::<i32>().clone().into();
                let array = large_arr.to_array_ref().as_binary::<O>().clone();
                Ok(GenericWkbArray::new(array, typ.metadata().clone()))
            } else {
                // Since O is already i32, we can just go via ArrayRef, and use .as_binary to cast
                // to O
                let array = arr.as_wkb::<i32>().to_array_ref();
                let array = array.as_binary::<O>().clone();
                Ok(GenericWkbArray::new(array, typ.metadata().clone()))
            }
        }
        LargeWkb(typ) => {
            if O::IS_LARGE {
                // Since O is already i64, we can just go via ArrayRef, and use .as_binary to cast
                // to O
                let array = arr.as_wkb::<i64>().to_array_ref();
                let array = array.as_binary::<O>().clone();
                Ok(GenericWkbArray::new(array, typ.metadata().clone()))
            } else {
                // We need to convert from i64 to i32
                let small_arr: GenericWkbArray<i32> = arr.as_wkb::<i64>().clone().try_into()?;
                let array = small_arr.to_array_ref().as_binary::<O>().clone();
                Ok(GenericWkbArray::new(array, typ.metadata().clone()))
            }
        }
        WkbView(_) => {
            let wkb_view_arr = arr.as_wkb_view();
            let metadata = wkb_view_arr.data_type().metadata().clone();
            let array = wkb_view_arr.clone().into_arrow();

            let mut builder = GenericByteBuilder::with_capacity(arr.len(), 0);
            array.iter().for_each(|value| builder.append_option(value));
            Ok(GenericWkbArray::new(builder.finish(), metadata))
        }
        Wkt(_) => impl_to_wkb(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_to_wkb(arr.as_wkt::<i64>()),
        WktView(_) => impl_to_wkb(arr.as_wkt_view()),
    }
}

fn impl_to_wkb<'a, O: OffsetSizeTrait>(
    geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<GenericWkbArray<O>> {
    let geoms = geo_arr
        .iter()
        .map(|x| x.transpose())
        .collect::<GeoArrowResult<Vec<_>>>()?;
    let wkb_type = WkbType::new(geo_arr.data_type().metadata().clone());
    Ok(WkbBuilder::from_nullable_geometries(geoms.as_slice(), wkb_type)?.finish())
}

/// Convert a [GeoArrowArray] to a [`WkbViewArray`].
pub fn to_wkb_view(arr: &dyn GeoArrowArray) -> GeoArrowResult<WkbViewArray> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkb_view(arr.as_point()),
        LineString(_) => impl_to_wkb_view(arr.as_line_string()),
        Polygon(_) => impl_to_wkb_view(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkb_view(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkb_view(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkb_view(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkb_view(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkb_view(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkb_view(arr.as_rect()),
        Wkb(_) => wkb_array_to_wkb_view(arr.as_wkb::<i32>()),
        LargeWkb(_) => wkb_array_to_wkb_view(arr.as_wkb::<i64>()),
        WkbView(_) => Ok(arr.as_wkb_view().clone()),
        Wkt(_) => impl_to_wkb_view(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_to_wkb_view(arr.as_wkt::<i64>()),
        WktView(_) => impl_to_wkb_view(arr.as_wkt_view()),
    }
}

/// Convert an arbitrary GeoArrowArray to a WkbViewArray.
///
/// This function will parse each geometry and re-encode it as WKB.
fn impl_to_wkb_view<'a>(
    geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<WkbViewArray> {
    let geoms = geo_arr
        .iter()
        .map(|x| x.transpose())
        .collect::<GeoArrowResult<Vec<_>>>()?;

    let mut builder = BinaryViewBuilder::with_capacity(geo_arr.len());
    let wkb_options = WriteOptions {
        endianness: Endianness::LittleEndian,
    };
    for maybe_geom in geoms {
        if let Some(geom) = maybe_geom {
            let mut buf = Vec::new();
            wkb::writer::write_geometry(&mut buf, &geom, &wkb_options).unwrap();
            builder.append_value(buf);
        } else {
            builder.append_null();
        }
    }

    let binary_view_arr = builder.finish();
    Ok(WkbViewArray::new(
        binary_view_arr,
        geo_arr.data_type().metadata().clone(),
    ))
}

/// A fast path of converting to WkbViewArray that does not parse and re-encode WKB buffers
fn wkb_array_to_wkb_view<O: OffsetSizeTrait>(
    arr: &GenericWkbArray<O>,
) -> GeoArrowResult<WkbViewArray> {
    let metadata = arr.data_type().metadata().clone();
    let mut builder = BinaryViewBuilder::with_capacity(arr.len());

    for value in arr.inner().iter() {
        if let Some(bytes) = value {
            builder.append_value(bytes);
        } else {
            builder.append_null();
        }
    }

    Ok(WkbViewArray::new(builder.finish(), metadata))
}

/// Parse a [`GenericWkbArray`] or [`WkbViewArray`] to a [`GeoArrowArray`] with the designated
/// [`GeoArrowType`].
///
/// Note that the GeoArrow metadata on the new array is taken from `to_type` **not** the original
/// array. Ensure you construct the [GeoArrowType] with the correct metadata.
///
/// Note that this will be slow if converting from a WKB array to another WKB-typed array. If
/// possible, use the `From` impls on WKB-typed arrays.
pub fn from_wkb<'a, A: GenericWkbArrayType<'a>>(
    arr: &'a A,
    to_type: GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // Make this a callback so that we don't actually generate this vec when converting from WKB to
    // WKT or WKB
    let geoms_fn = || {
        arr.iter()
            .map(|g| g.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()
    };

    use GeoArrowType::*;
    let result: Arc<dyn GeoArrowArray> = match to_type {
        Point(typ) => Arc::new(PointBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish()),
        LineString(typ) => {
            Arc::new(LineStringBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        Polygon(typ) => {
            Arc::new(PolygonBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiPoint(typ) => {
            Arc::new(MultiPointBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiLineString(typ) => {
            Arc::new(MultiLineStringBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiPolygon(typ) => {
            Arc::new(MultiPolygonBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        GeometryCollection(typ) => Arc::new(
            GeometryCollectionBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish(),
        ),
        Rect(_) => {
            return Err(GeoArrowError::IncorrectGeometryType(format!(
                "Cannot decode WKB geometries to Rect geometry type in from_wkb {to_type:?}",
            )));
        }
        Geometry(typ) => {
            Arc::new(GeometryBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        Wkb(typ) => {
            let mut wkb_arr = to_wkb::<i32>(arr)?;
            wkb_arr.data_type = typ;
            Arc::new(wkb_arr)
        }
        LargeWkb(typ) => {
            let mut wkb_arr = to_wkb::<i64>(arr)?;
            wkb_arr.data_type = typ;
            Arc::new(wkb_arr)
        }
        WkbView(typ) => {
            let mut wkb_view_arr = to_wkb_view(arr)?;
            wkb_view_arr.data_type = typ;
            Arc::new(wkb_view_arr)
        }
        Wkt(typ) => {
            let mut wkt_arr = to_wkt::<i32>(arr)?;
            wkt_arr.data_type = typ;
            Arc::new(wkt_arr)
        }
        LargeWkt(typ) => {
            let mut wkt_arr = to_wkt::<i64>(arr)?;
            wkt_arr.data_type = typ;
            Arc::new(wkt_arr)
        }
        WktView(typ) => {
            let mut wkt_view_arr = to_wkt_view(arr)?;
            wkt_view_arr.data_type = typ;
            Arc::new(wkt_view_arr)
        }
    };
    Ok(result)
}

/// Convert a [GeoArrowArray] to a [`GenericWktArray`].
pub fn to_wkt<O: OffsetSizeTrait>(arr: &dyn GeoArrowArray) -> GeoArrowResult<GenericWktArray<O>> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkt(arr.as_point()),
        LineString(_) => impl_to_wkt(arr.as_line_string()),
        Polygon(_) => impl_to_wkt(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkt(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkt(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkt(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkt(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkt(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkt(arr.as_rect()),
        Wkb(_) => impl_to_wkt(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_to_wkt(arr.as_wkb::<i64>()),
        WkbView(_) => impl_to_wkt(arr.as_wkb_view()),
        Wkt(typ) => {
            if O::IS_LARGE {
                let large_arr: GenericWktArray<i64> = arr.as_wkt::<i32>().clone().into();
                let array = large_arr.to_array_ref().as_string::<O>().clone();
                Ok(GenericWktArray::new(array, typ.metadata().clone()))
            } else {
                // Since O is already i32, we can just go via ArrayRef, and use .as_string to cast
                // to O
                let array = arr.as_wkt::<i32>().to_array_ref();
                let array = array.as_string::<O>().clone();
                Ok(GenericWktArray::new(array, typ.metadata().clone()))
            }
        }
        LargeWkt(typ) => {
            if O::IS_LARGE {
                // Since O is already i64, we can just go via ArrayRef, and use .as_string to cast
                // to O
                let array = arr.as_wkt::<i64>().to_array_ref();
                let array = array.as_string::<O>().clone();
                Ok(GenericWktArray::new(array, typ.metadata().clone()))
            } else {
                let small_arr: GenericWktArray<i32> = arr.as_wkt::<i64>().clone().try_into()?;
                let array = small_arr.to_array_ref().as_string::<O>().clone();
                Ok(GenericWktArray::new(array, typ.metadata().clone()))
            }
        }
        WktView(_) => {
            let wkt_view_arr = arr.as_wkt_view();
            let metadata = wkt_view_arr.data_type().metadata().clone();
            let array = wkt_view_arr.clone().into_arrow();

            let mut builder = GenericStringBuilder::with_capacity(arr.len(), 0);
            array.iter().for_each(|value| builder.append_option(value));
            Ok(GenericWktArray::new(builder.finish(), metadata))
        }
    }
}

fn impl_to_wkt<'a, O: OffsetSizeTrait>(
    geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<GenericWktArray<O>> {
    let metadata = geo_arr.data_type().metadata().clone();
    let mut builder = GenericStringBuilder::with_capacity(geo_arr.len(), 0);

    for maybe_geom in geo_arr.iter() {
        if let Some(geom) = maybe_geom {
            wkt::to_wkt::write_geometry(&mut builder, &geom?)
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;
            builder.append_value("");
        } else {
            builder.append_null();
        }
    }

    Ok(GenericWktArray::new(builder.finish(), metadata))
}

/// Convert a [GeoArrowArray] to a [`WktViewArray`].
pub fn to_wkt_view(arr: &dyn GeoArrowArray) -> GeoArrowResult<WktViewArray> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkt_view(arr.as_point()),
        LineString(_) => impl_to_wkt_view(arr.as_line_string()),
        Polygon(_) => impl_to_wkt_view(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkt_view(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkt_view(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkt_view(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkt_view(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkt_view(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkt_view(arr.as_rect()),
        Wkb(_) => impl_to_wkt_view(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_to_wkt_view(arr.as_wkb::<i64>()),
        WkbView(_) => impl_to_wkt_view(arr.as_wkb_view()),
        Wkt(_) => wkt_array_to_wkt_view(arr.as_wkt::<i32>()),
        LargeWkt(_) => wkt_array_to_wkt_view(arr.as_wkt::<i64>()),
        WktView(_) => Ok(arr.as_wkt_view().clone()),
    }
}

/// Convert an arbitrary GeoArrowArray to a WktViewArray.
///
/// This function will parse each geometry and re-encode it as WKT.
fn impl_to_wkt_view<'a>(
    geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<WktViewArray> {
    let metadata = geo_arr.data_type().metadata().clone();
    let mut builder = StringViewBuilder::with_capacity(geo_arr.len());

    for maybe_geom in geo_arr.iter() {
        if let Some(geom) = maybe_geom {
            let mut s = String::new();
            wkt::to_wkt::write_geometry(&mut s, &geom?)
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;
            builder.append_value(s);
        } else {
            builder.append_null();
        }
    }

    Ok(WktViewArray::new(builder.finish(), metadata))
}

/// A fast path of converting to WktViewArray that does not parse and re-encode WKT buffers
fn wkt_array_to_wkt_view<O: OffsetSizeTrait>(
    arr: &GenericWktArray<O>,
) -> GeoArrowResult<WktViewArray> {
    let metadata = arr.data_type().metadata().clone();
    let mut builder = StringViewBuilder::with_capacity(arr.len());

    for value in arr.inner().iter() {
        if let Some(s) = value {
            builder.append_value(s);
        } else {
            builder.append_null();
        }
    }

    Ok(WktViewArray::new(builder.finish(), metadata))
}

/// Parse a [`GenericWktArray`] or [`WktViewArray`] to a [`GeoArrowArray`] with the designated
/// [`GeoArrowType`].
///
/// Note that the GeoArrow metadata on the new array is taken from `to_type` **not** the original
/// array. Ensure you construct the [GeoArrowType] with the correct metadata.
pub fn from_wkt<A: GenericWktArrayType>(
    arr: &A,
    to_type: GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // Make this a callback so that we don't actually generate this vec when converting from WKT to
    // WKT or WKB
    let geoms_fn = || {
        arr.iter()
            .map(|g| g.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()
    };

    use GeoArrowType::*;
    let result: Arc<dyn GeoArrowArray> = match to_type {
        Point(typ) => Arc::new(PointBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish()),
        LineString(typ) => {
            Arc::new(LineStringBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        Polygon(typ) => {
            Arc::new(PolygonBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiPoint(typ) => {
            Arc::new(MultiPointBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiLineString(typ) => {
            Arc::new(MultiLineStringBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        MultiPolygon(typ) => {
            Arc::new(MultiPolygonBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        GeometryCollection(typ) => Arc::new(
            GeometryCollectionBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish(),
        ),
        Rect(_) => {
            return Err(GeoArrowError::IncorrectGeometryType(format!(
                "Cannot decode WKT geometries to Rect geometry type in from_wkt {to_type:?}",
            )));
        }
        Geometry(typ) => {
            Arc::new(GeometryBuilder::from_nullable_geometries(&geoms_fn()?, typ)?.finish())
        }
        Wkb(typ) => {
            let mut wkb_arr = to_wkb::<i32>(arr)?;
            wkb_arr.data_type = typ;
            Arc::new(wkb_arr)
        }
        LargeWkb(typ) => {
            let mut wkb_arr = to_wkb::<i64>(arr)?;
            wkb_arr.data_type = typ;
            Arc::new(wkb_arr)
        }
        WkbView(typ) => {
            let mut wkb_view_arr = to_wkb_view(arr)?;
            wkb_view_arr.data_type = typ;
            Arc::new(wkb_view_arr)
        }
        Wkt(typ) => {
            let mut wkt_arr = to_wkt::<i32>(arr)?;
            wkt_arr.data_type = typ;
            Arc::new(wkt_arr)
        }
        LargeWkt(typ) => {
            let mut wkt_arr = to_wkt::<i64>(arr)?;
            wkt_arr.data_type = typ;
            Arc::new(wkt_arr)
        }
        WktView(typ) => {
            let mut wkt_view_arr = to_wkt_view(arr)?;
            wkt_view_arr.data_type = typ;
            Arc::new(wkt_view_arr)
        }
    };
    Ok(result)
}

/// Re-export symbols needed for downcast macros
///
/// Name follows `serde` convention
#[doc(hidden)]
pub mod __private {
    pub use geoarrow_schema::GeoArrowType;
}

/// Downcast a [GeoArrowArray] to a concrete-typed array based on its [`GeoArrowType`].
///
/// For example: computing unsigned area:
///
/// ```
/// use arrow_array::Float64Array;
/// use arrow_array::builder::Float64Builder;
/// use geo::Area;
/// use geo_traits::to_geo::ToGeoGeometry;
/// use geoarrow_schema::error::GeoArrowResult;
/// use geoarrow_array::{GeoArrowArrayAccessor, GeoArrowArray, downcast_geoarrow_array};
///
/// pub fn unsigned_area(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
///     downcast_geoarrow_array!(array, impl_unsigned_area)
/// }
///
/// fn impl_unsigned_area<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
///     let mut builder = Float64Builder::with_capacity(array.len());
///
///     for item in array.iter() {
///         if let Some(geom) = item {
///             builder.append_value(geom?.to_geometry().unsigned_area());
///         } else {
///             builder.append_null();
///         }
///     }
///
///     Ok(builder.finish())
/// }
/// ```
///
/// You can also override the behavior of specific data types to specialize or provide a fast path.
/// For example, we know that points and lines will always have an area of 0, and don't need to
/// iterate over the input values to compute that.
///
/// ```
/// # use arrow_array::Float64Array;
/// # use arrow_array::builder::Float64Builder;
/// # use geo::Area;
/// # use geo_traits::to_geo::ToGeoGeometry;
/// # use geoarrow_schema::error::GeoArrowResult;
/// # use geoarrow_schema::GeoArrowType;
/// # use geoarrow_array::GeoArrowArrayAccessor;
/// #
/// # fn impl_unsigned_area<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
/// #     let mut builder = Float64Builder::with_capacity(array.len());
/// #
/// #     for item in array.iter() {
/// #         if let Some(geom) = item {
/// #             builder.append_value(geom?.to_geometry().unsigned_area());
/// #         } else {
/// #             builder.append_null();
/// #         }
/// #     }
/// #
/// #     Ok(builder.finish())
/// # }
/// #
/// fn impl_unsigned_area_specialized<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
///     use GeoArrowType::*;
///     match array.data_type() {
///         Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
///             let values = vec![0.0f64; array.len()];
///             Ok(Float64Array::new(values.into(), array.logical_nulls()))
///         }
///         _ => impl_unsigned_area(array),
///     }
/// }
/// ```
///
/// This is a simplified version of the upstream
/// [downcast_primitive_array][arrow_array::downcast_primitive_array].
///
/// If you would like to help in updating this `downcast_geoarrow_array` to support the full range
/// of functionality of the upstream `downcast_primitive_array`, please create an issue or submit a
/// PR.
#[macro_export]
macro_rules! downcast_geoarrow_array {
    ($array:ident, $fn:expr $(, $args:expr )* $(,)?) => {
        match $array.data_type() {
            $crate::cast::__private::GeoArrowType::Point(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_point($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::LineString(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_line_string($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::Polygon(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_polygon($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::MultiPoint(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_multi_point($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::MultiLineString(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_multi_line_string($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::MultiPolygon(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_multi_polygon($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::Geometry(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_geometry($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::GeometryCollection(_) => $fn(
                $crate::cast::AsGeoArrowArray::as_geometry_collection($array) $(, $args )*
            ),
            $crate::cast::__private::GeoArrowType::Rect(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_rect($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::Wkb(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkb::<i32>($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::LargeWkb(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkb::<i64>($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::WkbView(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkb_view($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::Wkt(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkt::<i32>($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::LargeWkt(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkt::<i64>($array) $(, $args )*)
            }
            $crate::cast::__private::GeoArrowType::WktView(_) => {
                $fn($crate::cast::AsGeoArrowArray::as_wkt_view($array) $(, $args )*)
            }
        }
    };
}

// #[cfg(test)]
// mod test {
//     use std::sync::Arc;

//     use geoarrow_schema::{CoordType, Dimension, WkbType};

//     use super::*;
//     use crate::test;

//     #[test]
//     fn test_cast_wkb_in_to_wkb() {
//         let wkb_arr: GenericWkbArray<i32> =
//             to_wkb(&test::point::array(CoordType::Separated, Dimension::XY)).unwrap();
//         let wkb_arr2: GenericWkbArray<i32> = to_wkb(&wkb_arr).unwrap();
//         let wkb_arr3: GenericWkbArray<i64> = to_wkb(&wkb_arr2).unwrap();
//         let wkb_arr4: GenericWkbArray<i64> = to_wkb(&wkb_arr3).unwrap();
//         let wkb_arr5: GenericWkbArray<i32> = to_wkb(&wkb_arr4).unwrap();
//         assert_eq!(wkb_arr, wkb_arr5);
//     }

//     #[test]
//     fn test_cast_wkt_in_to_wkt() {
//         let wkt_arr: GenericWktArray<i32> =
//             to_wkt(&test::point::array(CoordType::Separated, Dimension::XY)).unwrap();
//         let wkt_arr2: GenericWktArray<i32> = to_wkt(&wkt_arr).unwrap();
//         let wkt_arr3: GenericWktArray<i64> = to_wkt(&wkt_arr2).unwrap();
//         let wkt_arr4: GenericWktArray<i64> = to_wkt(&wkt_arr3).unwrap();
//         let wkt_arr5: GenericWktArray<i32> = to_wkt(&wkt_arr4).unwrap();
//         assert_eq!(wkt_arr, wkt_arr5);
//     }

//     // Start WKB round trip tests
//     #[test]
//     fn test_round_trip_wkb_point() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::point::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_point());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_point());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_linestring() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::linestring::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_line_string());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_line_string());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_polygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::polygon::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_polygon());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_polygon());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_multipoint() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multipoint::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_point());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_point());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_multilinestring() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multilinestring::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_line_string());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_line_string());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_multipolygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multipolygon::array(coord_type, dim);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_polygon());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_polygon());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_geometrycollection() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::geometrycollection::array(coord_type, dim, false);

//                 let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//                 let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_geometry_collection());

//                 let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//                 let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_geometry_collection());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkb_geometry() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let arr = test::geometry::array(coord_type, false);

//             let wkb_arr = to_wkb::<i32>(&arr).unwrap();
//             let arr2 = from_wkb(&wkb_arr, arr.data_type().clone()).unwrap();
//             assert_eq!(&arr, arr2.as_geometry());

//             let wkb_arr2 = to_wkb::<i64>(&arr).unwrap();
//             let arr3 = from_wkb(&wkb_arr2, arr.data_type().clone()).unwrap();
//             assert_eq!(&arr, arr3.as_geometry());
//         }
//     }

//     // Start WKT round trip tests
//     #[test]
//     fn test_round_trip_wkt_point() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::point::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_point());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_point());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_linestring() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::linestring::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_line_string());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_line_string());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_polygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::polygon::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_polygon());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_polygon());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_multipoint() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multipoint::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_point());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_point());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_multilinestring() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multilinestring::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_line_string());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_line_string());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_multipolygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::multipolygon::array(coord_type, dim);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_multi_polygon());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_multi_polygon());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_geometrycollection() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let arr = test::geometrycollection::array(coord_type, dim, false);

//                 let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//                 let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr2.as_geometry_collection());

//                 let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//                 let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//                 assert_eq!(&arr, arr3.as_geometry_collection());
//             }
//         }
//     }

//     #[test]
//     fn test_round_trip_wkt_geometry() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             let arr = test::geometry::array(coord_type, false);

//             let wkt_arr = to_wkt::<i32>(&arr).unwrap();
//             let arr2 = from_wkt(&wkt_arr, arr.data_type().clone()).unwrap();
//             assert_eq!(&arr, arr2.as_geometry());

//             let wkt_arr2 = to_wkt::<i64>(&arr).unwrap();
//             let arr3 = from_wkt(&wkt_arr2, arr.data_type().clone()).unwrap();
//             assert_eq!(&arr, arr3.as_geometry());
//         }
//     }

//     // Verify that this compiles with the macro
//     #[allow(dead_code)]
//     fn _to_wkb_test_downcast_macro(
//         arr: &dyn GeoArrowArray,
//     ) -> GeoArrowResult<GenericWkbArray<i32>> {
//         downcast_geoarrow_array!(arr, impl_to_wkb)
//     }

//     fn impl_to_wkb<'a>(
//         geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
//     ) -> GeoArrowResult<GenericWkbArray<i32>> {
//         let geoms = geo_arr
//             .iter()
//             .map(|x| x.transpose())
//             .collect::<std::result::Result<Vec<_>, _>>()
//             .unwrap();
//         let wkb_type = WkbType::new(geo_arr.data_type().metadata().clone());
//         Ok(WkbBuilder::from_nullable_geometries(geoms.as_slice(), wkb_type)?.finish())
//     }

//     // Verify that this compiles with the macro
//     #[test]
//     fn test_downcast_macro_with_param() {
//         let arr =
//             Arc::new(test::geometry::array(Default::default(), false)) as Arc<dyn GeoArrowArray>;
//         let arr_ref = arr.as_ref();
//         let x = downcast_geoarrow_array!(arr_ref, impl_inner_function_with_param, 1.0).unwrap();
//         assert_eq!(x, 1.0);
//     }

//     fn impl_inner_function_with_param<'a>(
//         _geo_arr: &'a impl GeoArrowArrayAccessor<'a>,
//         param: f64,
//     ) -> GeoArrowResult<f64> {
//         Ok(param)
//     }
// }
