use common_error::{DaftError, DaftResult};
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

use crate::utils::{geom_to_wkb, wkb_opts_to_geometry_series};

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
        let x_len = x_arr.len();
        let y_len = y_arr.len();

        // Scalar broadcast: either side can be length-1 and is broadcast to the other's length.
        // Any other mismatch is an error.
        if x_len != y_len && x_len != 1 && y_len != 1 {
            return Err(DaftError::ValueError(format!(
                "st_point: x and y must have the same length or be scalar; got {} and {}",
                x_len, y_len
            )));
        }
        let n = x_len.max(y_len);

        let mut wkb_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
        for i in 0..n {
            let x_val = if x_len == 1 { x_arr.get(0) } else { x_arr.get(i) };
            let y_val = if y_len == 1 { y_arr.get(0) } else { y_arr.get(i) };
            let wkb = match (x_val, y_val) {
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
