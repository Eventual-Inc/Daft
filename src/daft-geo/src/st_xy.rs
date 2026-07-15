use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_f64, validate_geometry_field};

fn point_x(g: &Geometry) -> f64 {
    match g {
        Geometry::Point(p) => p.x(),
        Geometry::MultiPoint(mp) if mp.0.len() == 1 => mp.0[0].x(),
        _ => f64::NAN,
    }
}

fn point_y(g: &Geometry) -> f64 {
    match g {
        Geometry::Point(p) => p.y(),
        Geometry::MultiPoint(mp) if mp.0.len() == 1 => mp.0[0].y(),
        _ => f64::NAN,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StX;

#[typetag::serde]
impl ScalarUDF for StX {
    fn name(&self) -> &'static str { "st_x" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        unary_geom_to_f64(inputs.required(0)?, self.name(), point_x)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the X coordinate of a Point geometry (null for non-point types)."
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StY;

#[typetag::serde]
impl ScalarUDF for StY {
    fn name(&self) -> &'static str { "st_y" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        unary_geom_to_f64(inputs.required(0)?, self.name(), point_y)
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Returns the Y coordinate of a Point geometry (null for non-point types)."
    }
}

#[must_use]
pub fn st_x(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StX, vec![geom]).into()
}

#[must_use]
pub fn st_y(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StY, vec![geom]).into()
}
