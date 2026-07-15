use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Geometry, LineString};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_geom_to_geom, validate_geometry_field};

fn make_line(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    match (a, b) {
        (Geometry::Point(pa), Geometry::Point(pb)) => {
            Some(Geometry::LineString(LineString(vec![pa.0, pb.0])))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StMakeLine;

#[typetag::serde]
impl ScalarUDF for StMakeLine {
    fn name(&self) -> &'static str {
        "st_makeline"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_geom_to_geom(inputs.required(0)?, inputs.required(1)?, self.name(), make_line)
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
        "Constructs a LineString geometry from two Point geometries. Returns null for non-Point inputs."
    }
}

#[must_use]
pub fn st_makeline(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StMakeLine, vec![geom_a, geom_b]).into()
}
