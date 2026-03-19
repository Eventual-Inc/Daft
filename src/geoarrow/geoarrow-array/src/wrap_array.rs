use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray, UnionArray, cast::AsArray,
};
use arrow_schema::DataType;
use geoarrow_schema::{
    error::{GeoArrowError, GeoArrowResult},
    *,
};

use crate::{GeoArrowArray, array::*};

/// Using a GeoArrow geometry type, wrap the provided storage array as a GeoArrow array.
///
/// This is a convenient way to convert from an Arrow array to a GeoArrow array when you have an
/// extension type. You can also use the `TryFrom` implementations on each GeoArrow array type, but
/// this may be easier to remember and find.
pub trait WrapArray<Input> {
    /// The output GeoArrow array type.
    type Output: GeoArrowArray;

    /// Wrap the given storage array as an GeoArrow array.
    ///
    /// This terminology comes from pyarrow/Arrow C++, where extension types similarly have a
    /// [`wrap_array`](https://arrow.apache.org/docs/python/generated/pyarrow.ExtensionType.html#pyarrow.ExtensionType.wrap_array)
    /// method.
    fn wrap_array(&self, input: Input) -> GeoArrowResult<Self::Output>;
}

impl WrapArray<&StructArray> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &StructArray) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&FixedSizeListArray> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &FixedSizeListArray) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for LineStringType {
    type Output = LineStringArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        LineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for LineStringType {
    type Output = LineStringArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        LineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for PolygonType {
    type Output = PolygonArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        PolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for PolygonType {
    type Output = PolygonArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        PolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for MultiPointType {
    type Output = MultiPointArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiPointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiPointType {
    type Output = MultiPointArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiPointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for MultiLineStringType {
    type Output = MultiLineStringArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiLineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiLineStringType {
    type Output = MultiLineStringArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiLineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for MultiPolygonType {
    type Output = MultiPolygonArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiPolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiPolygonType {
    type Output = MultiPolygonArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiPolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&StructArray> for BoxType {
    type Output = RectArray;

    fn wrap_array(&self, input: &StructArray) -> GeoArrowResult<Self::Output> {
        RectArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for BoxType {
    type Output = RectArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        RectArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for GeometryCollectionType {
    type Output = GeometryCollectionArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        GeometryCollectionArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for GeometryCollectionType {
    type Output = GeometryCollectionArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        GeometryCollectionArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&UnionArray> for GeometryType {
    type Output = GeometryArray;

    fn wrap_array(&self, input: &UnionArray) -> GeoArrowResult<Self::Output> {
        GeometryArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for GeometryType {
    type Output = GeometryArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        GeometryArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&BinaryViewArray> for WkbType {
    type Output = WkbViewArray;

    fn wrap_array(&self, input: &BinaryViewArray) -> GeoArrowResult<Self::Output> {
        Ok(WkbViewArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&BinaryArray> for WkbType {
    type Output = WkbArray;

    fn wrap_array(&self, input: &BinaryArray) -> GeoArrowResult<Self::Output> {
        Ok(WkbArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&LargeBinaryArray> for WkbType {
    type Output = LargeWkbArray;

    fn wrap_array(&self, input: &LargeBinaryArray) -> GeoArrowResult<Self::Output> {
        Ok(LargeWkbArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&StringViewArray> for WktType {
    type Output = WktViewArray;

    fn wrap_array(&self, input: &StringViewArray) -> GeoArrowResult<Self::Output> {
        Ok(WktViewArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&dyn Array> for WkbType {
    type Output = Arc<dyn GeoArrowArray>;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        match input.data_type() {
            DataType::BinaryView => Ok(Arc::new(WkbViewArray::from((
                input.as_binary_view().clone(),
                self.clone(),
            )))),
            DataType::Binary => Ok(Arc::new(WkbArray::from((
                input.as_binary().clone(),
                self.clone(),
            )))),
            DataType::LargeBinary => Ok(Arc::new(LargeWkbArray::from((
                input.as_binary().clone(),
                self.clone(),
            )))),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected DataType for WkbType: {dt:?}",
            ))),
        }
    }
}

impl WrapArray<&StringArray> for WktType {
    type Output = WktArray;

    fn wrap_array(&self, input: &StringArray) -> GeoArrowResult<Self::Output> {
        Ok(WktArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&LargeStringArray> for WktType {
    type Output = LargeWktArray;

    fn wrap_array(&self, input: &LargeStringArray) -> GeoArrowResult<Self::Output> {
        Ok(LargeWktArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&dyn Array> for WktType {
    type Output = Arc<dyn GeoArrowArray>;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        match input.data_type() {
            DataType::Utf8View => Ok(Arc::new(WktViewArray::from((
                input.as_string_view().clone(),
                self.clone(),
            )))),
            DataType::Utf8 => Ok(Arc::new(WktArray::from((
                input.as_string().clone(),
                self.clone(),
            )))),
            DataType::LargeUtf8 => Ok(Arc::new(LargeWktArray::from((
                input.as_string().clone(),
                self.clone(),
            )))),
            dt => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unexpected DataType for WktType: {dt:?}",
            ))),
        }
    }
}

impl WrapArray<&dyn Array> for GeoArrowType {
    type Output = Arc<dyn GeoArrowArray>;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        use GeoArrowType::*;

        let result: Arc<dyn GeoArrowArray> = match self {
            Point(t) => Arc::new(t.wrap_array(input)?),
            LineString(t) => Arc::new(t.wrap_array(input)?),
            Polygon(t) => Arc::new(t.wrap_array(input)?),
            MultiPoint(t) => Arc::new(t.wrap_array(input)?),
            MultiLineString(t) => Arc::new(t.wrap_array(input)?),
            MultiPolygon(t) => Arc::new(t.wrap_array(input)?),
            GeometryCollection(t) => Arc::new(t.wrap_array(input)?),
            Rect(t) => Arc::new(t.wrap_array(input)?),
            Geometry(t) => Arc::new(t.wrap_array(input)?),
            Wkb(t) => Arc::new(WkbArray::try_from((input, t.clone()))?),
            LargeWkb(t) => Arc::new(LargeWkbArray::try_from((input, t.clone()))?),
            WkbView(t) => Arc::new(WkbViewArray::try_from((input, t.clone()))?),
            Wkt(t) => Arc::new(WktArray::try_from((input, t.clone()))?),
            LargeWkt(t) => Arc::new(LargeWktArray::try_from((input, t.clone()))?),
            WktView(t) => Arc::new(WktViewArray::try_from((input, t.clone()))?),
        };
        Ok(result)
    }
}
