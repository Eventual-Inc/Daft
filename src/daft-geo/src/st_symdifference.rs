use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BooleanOps, Geometry};
use serde::{Deserialize, Serialize};

use crate::utils::{as_multipolygon, binary_geom_to_geom, validate_geometry_field};

fn op_symdifference(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    let (amp, bmp) = (as_multipolygon(a)?, as_multipolygon(b)?);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| amp.xor(&bmp)));
    result.ok().map(Geometry::MultiPolygon)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StSymDifference;

#[typetag::serde]
impl ScalarUDF for StSymDifference {
    fn name(&self) -> &'static str {
        "st_symdifference"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_geom_to_geom(
            inputs.required(0)?,
            inputs.required(1)?,
            self.name(),
            op_symdifference,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Returns the symmetric difference (XOR) of two polygon geometries. Non-polygon inputs return null."
    }
}

#[must_use]
pub fn st_symdifference(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StSymDifference, vec![geom_a, geom_b]).into()
}
