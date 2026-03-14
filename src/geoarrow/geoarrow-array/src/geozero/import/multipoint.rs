use geoarrow_schema::MultiPointType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::MultiPointArray,
    builder::MultiPointBuilder,
    capacity::MultiPointCapacity,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow MultiPointArray.
pub trait ToMultiPointArray {
    /// Convert to GeoArrow MultiPointArray
    fn to_multi_point_array(&self, typ: MultiPointType) -> geozero::error::Result<MultiPointArray> {
        Ok(self.to_multi_point_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow MultiPointBuilder
    fn to_multi_point_builder(
        &self,
        typ: MultiPointType,
    ) -> geozero::error::Result<MultiPointBuilder>;
}

impl<T: GeozeroGeometry> ToMultiPointArray for T {
    fn to_multi_point_builder(
        &self,
        typ: MultiPointType,
    ) -> geozero::error::Result<MultiPointBuilder> {
        let mut mutable_array = MultiPointBuilder::new(typ);
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for MultiPointBuilder {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(0, size);
        self.reserve(capacity);
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        self.push_coord(&from_xy(x, y).expect("valid coord"))
            .unwrap();
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
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        self.push_coord(&from_xyzm(x, y, z, m).expect("valid coord"))
            .unwrap();
        Ok(())
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(1, 0);
        self.reserve(capacity);
        self.try_push_length(1).unwrap();
        Ok(())
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        let capacity = MultiPointCapacity::new(size, 0);
        self.reserve(capacity);
        self.try_push_length(size).unwrap();
        Ok(())
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geo_types::{Geometry, MultiPoint};
//     use geoarrow_schema::Dimension;
//     use geozero::error::Result;

//     use super::*;
//     use crate::test::multipoint::{mp0, mp1};

//     #[test]
//     fn from_geozero() -> Result<()> {
//         let geo_geoms = vec![mp0(), MultiPoint(vec![]), mp1()];

//         let geo = Geometry::GeometryCollection(
//             geo_geoms
//                 .clone()
//                 .into_iter()
//                 .map(Geometry::MultiPoint)
//                 .collect(),
//         );
//         let typ = MultiPointType::new(Dimension::XY, Default::default());
//         let geo_arr = geo.to_multi_point_array(typ.clone()).unwrap();

//         let geo_arr2 = MultiPointBuilder::from_multi_points(&geo_geoms, typ).finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
