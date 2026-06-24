use common_error::{DaftError, DaftResult};
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::{Euclidean, Geodesic, Geometry};
use geo::line_measures::LengthMeasurable;
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_f64, validate_geometry_field};

fn geom_length(g: &Geometry, use_spheroid: bool) -> f64 {
    if use_spheroid {
        match g {
            Geometry::Line(l) => l.length(&Geodesic),
            Geometry::LineString(ls) => ls.length(&Geodesic),
            Geometry::MultiLineString(mls) => mls.length(&Geodesic),
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

/// Extract `use_spheroid` from an optional trailing positional or named arg at call time.
fn read_use_spheroid(inputs: &FunctionArgs<Series>) -> DaftResult<bool> {
    let opt = inputs.optional((1usize, "use_spheroid"))?;
    match opt {
        None => Ok(false),
        Some(s) => {
            if s.data_type().is_boolean() && s.len() == 1 {
                Ok(s.bool().unwrap().get(0).unwrap_or(false))
            } else {
                Err(DaftError::ValueError(
                    "st_length: use_spheroid must be a boolean literal".to_string(),
                ))
            }
        }
    }
}

/// Extract `use_spheroid` from `get_return_field` args (ExprRef level).
fn read_use_spheroid_expr(inputs: &FunctionArgs<ExprRef>) -> DaftResult<bool> {
    let opt = inputs.optional((1usize, "use_spheroid"))?;
    match opt {
        None => Ok(false),
        Some(expr) => expr
            .as_literal()
            .and_then(|l| l.as_bool())
            .ok_or_else(|| {
                DaftError::ValueError(
                    "st_length: use_spheroid must be a boolean literal".to_string(),
                )
            }),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StLength;

#[typetag::serde]
impl ScalarUDF for StLength {
    fn name(&self) -> &'static str { "st_length" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let use_spheroid = read_use_spheroid(&inputs)?;
        unary_geom_to_f64(inputs.required(0)?, self.name(), |g| geom_length(g, use_spheroid))
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        read_use_spheroid_expr(&inputs)?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Length/perimeter. Coordinate units by default; WGS84 geodesic meters when use_spheroid=true (lon/lat input)."
    }
}

#[must_use]
pub fn st_length(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StLength, vec![geom]).into()
}
