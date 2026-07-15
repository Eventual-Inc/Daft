use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Centroid, Geometry};
use serde::{Deserialize, Serialize};

use crate::utils::validate_geometry_field;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StCentroid;

#[typetag::serde]
impl ScalarUDF for StCentroid {
    fn name(&self) -> &'static str {
        "st_centroid"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        use crate::utils::unary_geom_to_geom;

        unary_geom_to_geom(inputs.required(0)?, self.name(), |g| {
            g.centroid().map(Geometry::Point)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geometric centroid (center of mass) of a geometry as a Point."
    }
}

#[must_use]
pub fn st_centroid(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StCentroid, vec![geom]).into()
}
