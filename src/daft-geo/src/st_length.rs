use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Euclidean, Geodesic, Geometry, line_measures::LengthMeasurable};
use serde::{Deserialize, Serialize};

use crate::utils::{read_bool_arg, read_bool_arg_expr, unary_geom_to_f64, validate_geometry_field};

fn geom_length(g: &Geometry, use_spheroid: bool) -> f64 {
    if use_spheroid {
        match g {
            Geometry::Line(l) => l.length(&Geodesic),
            Geometry::LineString(ls) => ls.length(&Geodesic),
            Geometry::MultiLineString(mls) => mls.length(&Geodesic),
            // Points have zero length; Polygons and other types return 0.0 to
            // match the planar branch (which also returns 0.0 for those types).
            _ => 0.0,
        }
    } else {
        let euclidean = Euclidean;
        match g {
            Geometry::Line(l) => l.length(&euclidean),
            Geometry::LineString(ls) => ls.length(&euclidean),
            Geometry::MultiLineString(mls) => mls.length(&euclidean),
            _ => 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StLength;

#[typetag::serde]
impl ScalarUDF for StLength {
    fn name(&self) -> &'static str {
        "st_length"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let use_spheroid = read_bool_arg(&inputs, 1, "use_spheroid", self.name())?;
        unary_geom_to_f64(inputs.required(0)?, self.name(), |g| {
            geom_length(g, use_spheroid)
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
        "Length of line geometries (Line, LineString, MultiLineString). \
         Returns 0.0 for Points, Polygons, and all other geometry types — \
         use st_area with use_spheroid=true to obtain geodesic perimeter of polygons. \
         Coordinate units by default; WGS84 geodesic meters when use_spheroid=true (lon/lat input)."
    }
}

#[must_use]
pub fn st_length(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StLength, vec![geom]).into()
}
