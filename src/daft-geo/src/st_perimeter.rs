use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Euclidean, Geodesic, Geometry, LineString, Polygon, line_measures::LengthMeasurable};
use serde::{Deserialize, Serialize};

use crate::utils::{read_bool_arg, read_bool_arg_expr, unary_geom_to_f64, validate_geometry_field};

/// Perimeter of a single polygon: length of the exterior ring plus every hole.
fn polygon_perimeter(p: &Polygon, use_spheroid: bool) -> f64 {
    let ring_len = |ls: &LineString| {
        if use_spheroid {
            ls.length(&Geodesic)
        } else {
            ls.length(&Euclidean)
        }
    };
    let mut total = ring_len(p.exterior());
    for hole in p.interiors() {
        total += ring_len(hole);
    }
    total
}

/// Perimeter of areal geometries. Non-areal types (Point, LineString, ...)
/// return 0.0, matching PostGIS `ST_Perimeter`.
fn geom_perimeter(g: &Geometry, use_spheroid: bool) -> f64 {
    match g {
        Geometry::Polygon(p) => polygon_perimeter(p, use_spheroid),
        Geometry::MultiPolygon(mp) => mp.iter().map(|p| polygon_perimeter(p, use_spheroid)).sum(),
        _ => 0.0,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StPerimeter;

#[typetag::serde]
impl ScalarUDF for StPerimeter {
    fn name(&self) -> &'static str {
        "st_perimeter"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let use_spheroid = read_bool_arg(&inputs, 1, "use_spheroid", self.name())?;
        unary_geom_to_f64(inputs.required(0)?, self.name(), |g| {
            geom_perimeter(g, use_spheroid)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        read_bool_arg_expr(&inputs, 1, "use_spheroid", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Perimeter of areal geometries (Polygon, MultiPolygon), summing the exterior \
         ring and all holes. Returns 0.0 for Points, LineStrings, and other non-areal types. \
         Coordinate units by default; WGS84 geodesic meters when use_spheroid=true (lon/lat input)."
    }
}

#[must_use]
pub fn st_perimeter(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StPerimeter, vec![geom]).into()
}
