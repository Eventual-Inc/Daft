use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Geometry, Point};
use serde::{Deserialize, Serialize};
use wkb::geom_to_wkb;

use crate::utils::wkb_opts_to_geometry_series;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StPoint;

#[typetag::serde]
impl ScalarUDF for StPoint {
    fn name(&self) -> &'static str {
        "st_point"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let x_series = inputs.required(0)?.cast(&DataType::Float64)?;
        let y_series = inputs.required(1)?.cast(&DataType::Float64)?;
        let x_arr = x_series.f64()?;
        let y_arr = y_series.f64()?;
        let len = x_arr.len();

        let mut wkb_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
        for i in 0..len {
            let wkb = match (x_arr.get(i), y_arr.get(i)) {
                (Some(x), Some(y)) => {
                    let geom = Geometry::Point(Point::new(x, y));
                    geom_to_wkb(&geom).ok()
                }
                _ => None,
            };
            wkb_values.push(wkb);
        }

        wkb_opts_to_geometry_series(self.name(), wkb_values)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        // Validate that both args are numeric (castable to Float64)
        let x_field = inputs.required(0)?.to_field(schema)?;
        let y_field = inputs.required(1)?.to_field(schema)?;
        if !x_field.dtype.is_numeric() {
            return Err(common_error::DaftError::TypeError(format!(
                "st_point: x must be numeric, got {}",
                x_field.dtype
            )));
        }
        if !y_field.dtype.is_numeric() {
            return Err(common_error::DaftError::TypeError(format!(
                "st_point: y must be numeric, got {}",
                y_field.dtype
            )));
        }
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Constructs a Point geometry from x and y coordinate columns."
    }
}

#[must_use]
pub fn st_point(x: ExprRef, y: ExprRef) -> ExprRef {
    ScalarFn::builtin(StPoint, vec![x, y]).into()
}
