use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use serde::{Deserialize, Serialize};

use crate::relate::{relate_pred, RelatePred};
use crate::utils::{binary_geom_to_bool, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StTouches;

#[typetag::serde]
impl ScalarUDF for StTouches {
    fn name(&self) -> &'static str { "st_touches" }
    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        binary_geom_to_bool(inputs.required(0)?, inputs.required(1)?, self.name(),
            |a, b| relate_pred(a, b, RelatePred::Touches))
    }
    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Boolean))
    }
    fn docstring(&self) -> &'static str { "Returns true if A and B share a boundary but their interiors do not intersect." }
}

#[must_use]
pub fn st_touches(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StTouches, vec![geom_a, geom_b]).into()
}
