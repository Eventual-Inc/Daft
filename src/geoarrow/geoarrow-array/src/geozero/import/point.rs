use geoarrow_schema::PointType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::PointArray,
    builder::PointBuilder,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow PointArray.
pub trait ToPointArray {
    /// Convert to GeoArrow PointArray
    fn to_point_array(&self, typ: PointType) -> geozero::error::Result<PointArray> {
        Ok(self.to_point_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow PointBuilder
    fn to_point_builder(&self, typ: PointType) -> geozero::error::Result<PointBuilder>;
}

impl<T: GeozeroGeometry> ToPointArray for T {
    fn to_point_builder(&self, typ: PointType) -> geozero::error::Result<PointBuilder> {
        let mut mutable_point_array = PointBuilder::new(typ);
        self.process_geom(&mut mutable_point_array)?;
        Ok(mutable_point_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for PointBuilder {
    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.push_empty();
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> geozero::error::Result<()> {
        self.push_coord(from_xy(x, y).as_ref());
        Ok(())
    }

    fn coordinate(
        &mut self,
        x: f64,
        y: f64,
        z: Option<f64>,
        m: Option<f64>,
        t: Option<f64>,
        tm: Option<u64>,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.push_coord(from_xyzm(x, y, z, m).as_ref());
        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.reserve_exact(size);
        Ok(())
    }

    // Override all other trait _begin methods
    fn circularstring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn compoundcurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn tin_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn triangle_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    // fn multicurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
    //     Err(geozero::error::GeozeroError::Geometry(
    //         "Only point geometries allowed".to_string(),
    //     ))
    // }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn curvepolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multisurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }

    fn polyhedralsurface_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        Err(geozero::error::GeozeroError::Geometry(
            "Only point geometries allowed".to_string(),
        ))
    }
}

// #[cfg(test)]
// mod test {

//     use geo_types::{Geometry, GeometryCollection};
//     use geoarrow_schema::Dimension;

//     use super::*;
//     use crate::{
//         GeoArrowArrayAccessor,
//         test::{linestring, point},
//     };

//     #[test]
//     fn from_geozero() {
//         let geo = Geometry::GeometryCollection(
//             vec![
//                 Geometry::Point(point::p0()),
//                 Geometry::Point(point::p1()),
//                 Geometry::Point(point::p2()),
//             ]
//             .into(),
//         );

//         let typ = PointType::new(Dimension::XY, Default::default());
//         let point_array = geo.to_point_array(typ).unwrap();
//         assert_eq!(point_array.value(0).unwrap(), point::p0());
//         assert_eq!(point_array.value(1).unwrap(), point::p1());
//         assert_eq!(point_array.value(2).unwrap(), point::p2());
//     }

//     #[test]
//     fn from_geozero_error_multiple_geom_types() {
//         let geo = Geometry::GeometryCollection(GeometryCollection(vec![
//             Geometry::Point(point::p0()),
//             Geometry::LineString(linestring::ls0()),
//         ]));

//         let typ = PointType::new(Dimension::XY, Default::default());
//         let err = ToPointArray::to_point_array(&geo, typ).unwrap_err();
//         assert!(matches!(err, geozero::error::GeozeroError::Geometry(..)));
//     }
// }
