use std::{fmt::Debug, sync::Arc};

use geoarrow_schema::{GeometryType, error::GeoArrowResult};
use geozero::{GeomProcessor, GeozeroGeometry, error::GeozeroError, geo_types::GeoWriter};

use crate::{
    GeoArrowArray, array::GeometryArray, builder::GeometryBuilder, trait_::GeoArrowArrayBuilder,
};

/// GeoZero trait to convert to GeoArrow [`GeometryArray`].
///
/// **NOTE** only XY dimensions are currently supported here.
///
/// (This is because the internal GeoWriter only supports XY dimensions.)
pub trait ToGeometryArray {
    /// Convert to GeoArrow [`GeometryArray`]
    fn to_geometry_array(&self, typ: GeometryType) -> geozero::error::Result<GeometryArray> {
        Ok(self.to_geometry_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow [`GeometryBuilder`]
    fn to_geometry_builder(&self, typ: GeometryType) -> geozero::error::Result<GeometryBuilder>;
}

impl<T: GeozeroGeometry> ToGeometryArray for T {
    fn to_geometry_builder(&self, typ: GeometryType) -> geozero::error::Result<GeometryBuilder> {
        let mut stream_builder = GeometryStreamBuilder::new(typ);
        self.process_geom(&mut stream_builder)?;
        Ok(stream_builder.builder)
    }
}

/// A streaming builder for GeoArrow [`GeometryArray`].
///
/// This is useful in conjunction with [`geozero`] APIs because its coordinate stream requires the
/// consumer to keep track of which geometry type is currently being added to.
///
/// This implementation can be complex because we need to connect the push-based stream of the
/// geozero source (coordinate-by-coordinate) with the pull-based (complete geometry) APIs of the
/// [`GeometryBuilder`]. In particular, [`GeometryBuilder`] requires reading from _whole
/// geometries_.
///
/// This is implemented with an internal [GeoWriter] used to buffer each stream of coordinates.
/// Each incoming geometry is collected into a "current geometry", and then when that geometry's
/// stream is finished, that geometry is propagated on to the [`GeometryBuilder`] and the current
/// geometry is cleared.
///
/// Note that this has some memory overhead because of the buffering, and it requires copying
/// _once_ from the geozero source into the intermediate [geo_types] object, and then _again_ into
/// the GeoArrow array.
///
/// In the future we could use a bump allocator to improve memory performance here.
///
/// Converting an [`GeometryStreamBuilder`] into a [`GeometryArray`] is `O(1)`.
struct GeometryStreamBuilder {
    /// The underlying geometry builder. When each geometry is finished, we add the geometry to
    /// this builder.
    builder: GeometryBuilder,
    /// The current geometry being built. [GeoWriter] implements [GeomProcessor].
    current_geometry: GeoWriter,
    /// The current nesting level of geometry collections. This is required because geozero
    /// represents an array of geometries as a GeometryCollection. But we don't want to try to
    /// "finish" the `current_geometry` when this only represents the top-level sequence of
    /// geometries we're putting into the array.
    ///
    /// We should only "finish" the `current_geometry` for _nested_ geometry collections beyond the
    /// root level.
    geometry_collection_level: usize,
}

impl GeometryStreamBuilder {
    pub fn new(typ: GeometryType) -> Self {
        Self {
            builder: GeometryBuilder::new(typ),
            current_geometry: GeoWriter::new(),
            geometry_collection_level: 0,
        }
    }

    fn push_current_geometry(&mut self) -> geozero::error::Result<()> {
        let geom = self
            .current_geometry
            .take_geometry()
            .ok_or(GeozeroError::Geometry("Take geometry failed".to_string()))?;
        self.builder
            .push_geometry(Some(&geom))
            .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
        self.current_geometry = GeoWriter::new();
        Ok(())
    }
}

impl Debug for GeometryStreamBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.builder.fmt(f)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for GeometryStreamBuilder {
    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.xy(x, y, idx)
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
        self.current_geometry.coordinate(x, y, z, m, t, tm, idx)
    }

    fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
        // This needs to be separate because GeoWriter doesn't know how to handle empty points
        Err(GeozeroError::Geometry(
            "Empty points not currently supported in ToGeometryArray.".to_string(),
        ))
    }

    fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.point_begin(idx)
    }

    fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.point_end(idx)?;
        self.push_current_geometry()
    }

    fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipoint_begin(size, idx)
    }

    fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipoint_end(idx)?;
        self.push_current_geometry()
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.current_geometry.linestring_begin(tagged, size, idx)
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.linestring_end(tagged, idx)?;

        // When tagged is true, that means it's a standalone LineString and not part of a
        // MultiLineString
        if tagged {
            self.push_current_geometry()?;
        }
        Ok(())
    }

    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multilinestring_begin(size, idx)
    }

    fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multilinestring_end(idx)?;
        self.push_current_geometry()
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        self.current_geometry.polygon_begin(tagged, size, idx)
    }

    fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.polygon_end(tagged, idx)?;

        // When tagged is true, that means it's a standalone LineString and not part of a
        // MultiLineString
        if tagged {
            self.push_current_geometry()?;
        }

        Ok(())
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipolygon_begin(size, idx)
    }

    fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.current_geometry.multipolygon_end(idx)?;
        self.push_current_geometry()
    }

    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        if self.geometry_collection_level > 0 {
            self.current_geometry.geometrycollection_begin(size, idx)?;
        }

        self.geometry_collection_level += 1;
        Ok(())
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        self.geometry_collection_level -= 1;

        if self.geometry_collection_level > 0 {
            self.current_geometry.geometrycollection_end(idx)?;
            self.push_current_geometry()?;
        }

        Ok(())
    }
}

impl GeoArrowArrayBuilder for GeometryStreamBuilder {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn push_null(&mut self) {
        self.builder.push_null()
    }

    fn push_geometry(
        &mut self,
        geometry: Option<&impl geo_traits::GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        self.builder.push_geometry(geometry)
    }

    fn finish(self) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.builder.finish())
    }
}

// #[cfg(test)]
// mod test {
//     use geo_types::{Geometry, GeometryCollection};
//     use geozero::error::Result;

//     use super::*;
//     use crate::test::{linestring, multilinestring, multipoint, multipolygon, point, polygon};

//     fn geoms() -> Vec<geo_types::Geometry> {
//         vec![
//             point::p0().into(),
//             point::p1().into(),
//             point::p2().into(),
//             linestring::ls0().into(),
//             linestring::ls1().into(),
//             polygon::p0().into(),
//             polygon::p1().into(),
//             multipoint::mp0().into(),
//             multipoint::mp1().into(),
//             multilinestring::ml0().into(),
//             multilinestring::ml1().into(),
//             multipolygon::mp0().into(),
//             multipolygon::mp1().into(),
//         ]
//     }

//     #[test]
//     fn from_geo_using_geozero() -> Result<()> {
//         let geo_geoms = geoms().into_iter().map(Some).collect::<Vec<_>>();
//         let geo = Geometry::GeometryCollection(GeometryCollection(geoms()));
//         let typ = GeometryType::new(Default::default());
//         let geo_arr = geo.to_geometry_array(typ.clone()).unwrap();

//         let geo_arr2 = GeometryBuilder::from_nullable_geometries(&geo_geoms, typ)
//             .unwrap()
//             .finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
