use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BoundingRect, Geometry};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_geom, validate_geometry_field};

/// Return the bounding-box envelope of a geometry as a Polygon.
///
/// For a geometry with no bounding rect (e.g. an empty geometry collection)
/// this returns null.
fn apply_envelope(g: &Geometry) -> Option<Geometry> {
    g.bounding_rect().map(|r| Geometry::Polygon(r.to_polygon()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StEnvelope;

#[typetag::serde]
impl ScalarUDF for StEnvelope {
    fn name(&self) -> &'static str {
        "st_envelope"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_geom(inputs.required(0)?, self.name(), apply_envelope)
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
        "Returns the minimum bounding rectangle of a geometry as a Polygon. \
         For points returns a degenerate polygon; for geometries with no extent returns null."
    }
}

#[must_use]
pub fn st_envelope(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StEnvelope, vec![geom]).into()
}
