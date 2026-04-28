use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Schema};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::Geometry;
use serde::{Deserialize, Serialize};
use wkt::ToWkt;

use crate::utils::{unary_geom_to_utf8, validate_geometry_field};

fn geom_to_wkt(g: &Geometry) -> String {
    g.to_wkt().to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StAsText;

#[typetag::serde]
impl ScalarUDF for StAsText {
    fn name(&self) -> &'static str {
        "st_astext"
    }

    fn call(
        &self,
        inputs: FunctionArgs<daft_core::series::Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<daft_core::series::Series> {
        unary_geom_to_utf8(inputs.required(0)?, self.name(), geom_to_wkt)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the Well-Known Text (WKT) representation of a geometry."
    }
}

#[must_use]
pub fn st_astext(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StAsText, vec![geom]).into()
}
