use std::sync::Arc;

use arrow2::types::NativeType;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ListArray,
    datatypes::logical::GeometryArray,
    prelude::{DataType, Field},
    series::{IntoSeries, Series},
};
use geo::{
    Area, BooleanOps, Centroid, Contains, ConvexHull, EuclideanDistance, Geometry, Intersects,
};
use geozero::{wkb, wkt, GeozeroGeometry, ToGeo, ToWkb, ToWkt};
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub enum GeoOperation {
    Area,
    ConvexHull,
    Distance,
    Intersects,
    Intersection,
    Contains,
    Centroid,
}

struct GeometryArrayIter<'a> {
    cursor: usize,
    series: &'a GeometryArray,
}

impl GeometryArrayIter<'_> {
    fn new(backing: &GeometryArray) -> GeometryArrayIter {
        GeometryArrayIter {
            cursor: 0,
            series: backing,
        }
    }
}

impl<'a> Iterator for GeometryArrayIter<'a> {
    type Item = Option<Geometry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.series.len() {
            None
        } else {
            let series = self.series.physical.get(self.cursor);
            self.cursor += 1;
            match series {
                Some(series) => {
                    let bytes = series.u8().unwrap().as_slice();
                    Some(Some(wkb::Ewkb(bytes).to_geo().unwrap()))
                }
                None => Some(None),
            }
        }
    }
}

struct GeoSeriesHelper {
    geo_vec: Vec<u8>,
    offsets: Vec<i64>,
    validity: arrow2::bitmap::MutableBitmap,
}

impl GeoSeriesHelper {
    fn new(capacity: usize) -> Self {
        let mut series = Self {
            geo_vec: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity + 1),
            validity: arrow2::bitmap::MutableBitmap::with_capacity(capacity),
        };
        series.offsets.push(0i64);
        series
    }

    fn push(&mut self, geo: Geometry) {
        let coord_dims = geo.dims();
        let srid = geo.srid();
        let geo_bytes = geo.to_ewkb(coord_dims, srid).unwrap();
        self.geo_vec.extend(geo_bytes.iter());
        self.offsets
            .push(self.offsets.last().unwrap() + geo_bytes.len() as i64);
        self.validity.push(true);
    }

    fn null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap());
        self.validity.push(false);
    }

    fn into_series(self, name: &str) -> DaftResult<Series> {
        let data_array = ListArray::new(
            Field::new("data", DataType::List(Box::new(DataType::UInt8))),
            Series::try_from((
                "data",
                Box::new(arrow2::array::PrimitiveArray::from_vec(self.geo_vec))
                    as Box<dyn arrow2::array::Array>,
            ))?,
            arrow2::offset::OffsetsBuffer::try_from(self.offsets)?,
            self.validity.into(),
        );
        Ok(GeometryArray::new(Field::new(name, DataType::Geometry), data_array).into_series())
    }
}

pub fn decode_series(s: &Series, raise_error_on_failure: bool) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Binary => {
            let binary = s.binary()?;
            let arrow_array = binary
                .data()
                .as_any()
                .downcast_ref::<arrow2::array::BinaryArray<i64>>()
                .unwrap();
            let mut geo_helper = GeoSeriesHelper::new(arrow_array.len());
            for bytes in arrow_array {
                match bytes {
                    Some(bytes) => match wkb::Ewkb(bytes).to_geo() {
                        Ok(geo) => geo_helper.push(geo),
                        Err(_) => {
                            if raise_error_on_failure {
                                return Err(DaftError::ValueError(
                                    "Could not decode WKB".to_string(),
                                ));
                            }
                            geo_helper.null();
                        }
                    },
                    None => geo_helper.null(),
                }
            }
            geo_helper.into_series(binary.name())
        }
        DataType::Utf8 => {
            let utf8 = s.utf8()?;
            let mut geo_helper = GeoSeriesHelper::new(utf8.len());
            let arrow_array = utf8
                .data()
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap();
            for string in arrow_array {
                match string {
                    Some(string) => match wkt::Wkt(string).to_geo() {
                        Ok(geo) => geo_helper.push(geo),
                        Err(_) => {
                            if raise_error_on_failure {
                                return Err(DaftError::ValueError(format!(
                                    "Could not decode WKT text {}",
                                    string
                                )));
                            }
                            geo_helper.null();
                        }
                    },
                    None => geo_helper.null(),
                }
            }
            geo_helper.into_series(utf8.name())
        }
        other => Err(DaftError::TypeError(format!(
            "GeoDecode can only decode Binary or Utf8 arrays, got {}",
            other
        ))),
    }
}

fn to_wkt(s: &Series) -> DaftResult<Series> {
    let geo_array = s.geometry()?;
    let string_iter =
        GeometryArrayIter::new(geo_array).map(|geo| geo.map(|geo| geo.to_wkt().unwrap()));
    Series::from_arrow(
        Arc::new(Field::new(geo_array.name(), DataType::Utf8)),
        Box::new(arrow2::array::Utf8Array::<i64>::from_iter(string_iter)),
    )
}

fn to_wkb(s: &Series) -> DaftResult<Series> {
    let geo_array = s.geometry()?;
    let bytes_iter = geo_array
        .physical
        .into_iter()
        .map(|x| x.map(|x| x.u8().unwrap().as_slice().to_vec()));
    Series::from_arrow(
        Arc::new(Field::new(geo_array.name(), DataType::Binary)),
        Box::new(arrow2::array::BinaryArray::<i64>::from_iter(bytes_iter)),
    )
}

pub fn encode_series(s: &Series, text: bool) -> DaftResult<Series> {
    match text {
        true => to_wkt(s),
        false => to_wkb(s),
    }
}

pub fn geo_unary_dispatch(s: &Series, op: GeoOperation) -> DaftResult<Series> {
    match op {
        GeoOperation::Area => geo_unary_to_scalar::<f64, _>(s, |g| g.unsigned_area()),
        GeoOperation::ConvexHull => geo_unary_to_geo(s, |g| Some(g.convex_hull().into())),
        GeoOperation::Centroid => geo_unary_to_geo(s, |g| g.centroid().map(|c| c.into())),
        _ => Err(DaftError::ValueError(format!("unsupported op {:?}", op))),
    }
}

pub fn geo_binary_dispatch(lhs: &Series, rhs: &Series, op: GeoOperation) -> DaftResult<Series> {
    match op {
        GeoOperation::Distance => {
            geo_binary_to_scalar::<f64, _>(lhs, rhs, |l, r| l.euclidean_distance(&r))
        }
        GeoOperation::Intersects => geo_binary_to_bool(lhs, rhs, |l, r| l.intersects(&r)),
        GeoOperation::Contains => geo_binary_to_bool(lhs, rhs, |l, r| l.contains(&r)),
        GeoOperation::Intersection => geo_binary_to_geo(lhs, rhs, |l, r| match (l, r) {
            (Geometry::Polygon(l), Geometry::Polygon(r)) => Some(l.intersection(&r).into()),
            (Geometry::MultiPolygon(l), Geometry::MultiPolygon(r)) => {
                Some(l.intersection(&r).into())
            }
            _ => None,
        }),
        _ => Err(DaftError::ValueError(format!("unsupported op {:?}", op))),
    }
}

fn geo_unary_to_scalar<T: NativeType, F>(s: &Series, op_fn: F) -> DaftResult<Series>
where
    F: Fn(Geometry) -> T,
{
    let geo_array = s.geometry()?;
    let scalar_iter = GeometryArrayIter::new(geo_array).map(|geo| geo.map(&op_fn));
    let arrow_array = arrow2::array::PrimitiveArray::<T>::from_iter(scalar_iter);
    Series::from_arrow(
        Arc::new(Field::new(
            geo_array.name(),
            DataType::from(arrow_array.data_type()),
        )),
        Box::new(arrow_array),
    )
}

fn geo_unary_to_geo<F>(s: &Series, op_fn: F) -> DaftResult<Series>
where
    F: Fn(Geometry) -> Option<Geometry>,
{
    let geo_array = s.geometry()?;
    let mut geo_helper = GeoSeriesHelper::new(geo_array.len());
    for geo in GeometryArrayIter::new(geo_array) {
        match geo {
            Some(geo) => match op_fn(geo) {
                Some(result) => geo_helper.push(result),
                None => geo_helper.null(),
            },
            _ => geo_helper.null(),
        }
    }
    geo_helper.into_series(geo_array.name())
}

fn geo_binary_to_scalar<T: NativeType, F>(
    lhs: &Series,
    rhs: &Series,
    op_fn: F,
) -> DaftResult<Series>
where
    F: Fn(Geometry, Geometry) -> T,
{
    let lhs_array = lhs.geometry()?;
    let rhs_array = rhs.geometry()?;
    let scalar_iter = GeometryArrayIter::new(lhs_array)
        .zip(GeometryArrayIter::new(rhs_array))
        .map(|(lhg, rhg)| match (lhg, rhg) {
            (Some(l), Some(r)) => Some(op_fn(l, r)),
            _ => None,
        });
    let arrow_array = arrow2::array::PrimitiveArray::<T>::from_iter(scalar_iter);
    Series::from_arrow(
        Arc::new(Field::new(
            lhs_array.name(),
            DataType::from(arrow_array.data_type()),
        )),
        Box::new(arrow_array),
    )
}

fn geo_binary_to_bool<F>(lhs: &Series, rhs: &Series, op_fn: F) -> DaftResult<Series>
where
    F: Fn(Geometry, Geometry) -> bool,
{
    let lhs_array = lhs.geometry()?;
    let rhs_array = rhs.geometry()?;
    let scalar_iter = GeometryArrayIter::new(lhs_array)
        .zip(GeometryArrayIter::new(rhs_array))
        .map(|(lhg, rhg)| match (lhg, rhg) {
            (Some(l), Some(r)) => Some(op_fn(l, r)),
            _ => None,
        });
    Series::from_arrow(
        Arc::new(Field::new(lhs_array.name(), DataType::Boolean)),
        Box::new(arrow2::array::BooleanArray::from_iter(scalar_iter)),
    )
}

fn geo_binary_to_geo<F>(lhs: &Series, rhs: &Series, op_fn: F) -> DaftResult<Series>
where
    F: Fn(Geometry, Geometry) -> Option<Geometry>,
{
    let lhs_array = lhs.geometry()?;
    let rhs_array = rhs.geometry()?;
    let mut geo_helper = GeoSeriesHelper::new(lhs_array.len());
    for (lhg, rhg) in GeometryArrayIter::new(lhs_array).zip(GeometryArrayIter::new(rhs_array)) {
        match (lhg, rhg) {
            (Some(l), Some(r)) => match op_fn(l, r) {
                Some(geo) => geo_helper.push(geo),
                None => geo_helper.null(),
            },
            _ => geo_helper.null(),
        }
    }
    geo_helper.into_series(lhs_array.name())
}
