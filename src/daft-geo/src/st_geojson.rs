use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BinaryArray, DataType, Field, IntoSeries, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geojson::GeoJson;
use serde::{Deserialize, Serialize};
use wkb::geom_to_wkb;

/// Parse a GeoJSON string into WKB bytes.
fn geojson_to_wkb(s: &str) -> Option<Vec<u8>> {
    let gj: GeoJson = s.parse().ok()?;
    let geom: geo::Geometry<f64> = match gj {
        GeoJson::Geometry(g) => geo::Geometry::try_from(g).ok()?,
        GeoJson::Feature(f) => geo::Geometry::try_from(f.geometry?).ok()?,
        GeoJson::FeatureCollection(_) => return None,
    };
    geom_to_wkb(&geom).ok()
}

// ── st_geomfromgeojson ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeomFromGeoJson;

#[typetag::serde]
impl ScalarUDF for StGeomFromGeoJson {
    fn name(&self) -> &'static str {
        "st_geomfromgeojson"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let series = inputs.required(0)?;
        let utf8 = series.utf8().map_err(|_| {
            DaftError::TypeError(format!(
                "st_geomfromgeojson: expected Utf8 input, got {}",
                series.data_type()
            ))
        })?;

        let values: Vec<Option<Vec<u8>>> = utf8
            .into_iter()
            .map(|opt| opt.and_then(geojson_to_wkb))
            .collect();

        let arr = BinaryArray::from_iter(self.name(), values.iter().map(|v| v.as_deref()));
        Ok(arr.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let f = inputs.required(0)?.to_field(schema)?;
        if !matches!(f.dtype, DataType::Utf8) {
            return Err(DaftError::TypeError(format!(
                "st_geomfromgeojson: expected Utf8, got {}",
                f.dtype
            )));
        }
        Ok(Field::new(self.name(), DataType::Binary))
    }

    fn docstring(&self) -> &'static str {
        "Parses a GeoJSON string and returns a WKB geometry."
    }
}

#[must_use]
pub fn st_geomfromgeojson(geojson: ExprRef) -> ExprRef {
    ScalarFn::builtin(StGeomFromGeoJson, vec![geojson]).into()
}

// ── st_geojsonfromgeom ─────────────────────────────────────────────────────

use crate::utils::{unary_geom_to_utf8, validate_geometry_field};

fn geom_to_geojson_str(g: &geo::Geometry) -> String {
    let gj = geojson::Geometry::from(g);
    gj.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeoJsonFromGeom;

#[typetag::serde]
impl ScalarUDF for StGeoJsonFromGeom {
    fn name(&self) -> &'static str {
        "st_geojsonfromgeom"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_utf8(inputs.required(0)?, self.name(), geom_to_geojson_str)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the GeoJSON string representation of a geometry."
    }
}

#[must_use]
pub fn st_geojsonfromgeom(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StGeoJsonFromGeom, vec![geom]).into()
}
