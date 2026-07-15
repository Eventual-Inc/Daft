use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Area, algorithm::geodesic_area::GeodesicArea};
use serde::{Deserialize, Serialize};

use crate::utils::{read_bool_arg, read_bool_arg_expr, unary_geom_to_f64, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StArea;

#[typetag::serde]
impl ScalarUDF for StArea {
    fn name(&self) -> &'static str {
        "st_area"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let use_spheroid = read_bool_arg(&inputs, 1, "use_spheroid", self.name())?;
        unary_geom_to_f64(inputs.required(0)?, self.name(), |g| {
            if use_spheroid {
                g.geodesic_area_unsigned()
            } else {
                g.unsigned_area()
            }
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
        "2D area. Coordinate units² by default; WGS84 geodesic m² when use_spheroid=true (lon/lat input)."
    }
}

#[must_use]
pub fn st_area(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StArea, vec![geom]).into()
}
