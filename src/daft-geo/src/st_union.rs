use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BooleanOps, Geometry, MultiPolygon};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_geom_to_geom, validate_geometry_field};

fn as_multipolygon(g: &Geometry) -> Option<MultiPolygon> {
    match g {
        Geometry::Polygon(p) => Some(MultiPolygon(vec![p.clone()])),
        Geometry::MultiPolygon(mp) => Some(mp.clone()),
        _ => None,
    }
}

fn op_union(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    let (amp, bmp) = (as_multipolygon(a)?, as_multipolygon(b)?);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| amp.union(&bmp)));
    result.ok().map(Geometry::MultiPolygon)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StUnion;

#[typetag::serde]
impl ScalarUDF for StUnion {
    fn name(&self) -> &'static str {
        "st_union"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_geom_to_geom(inputs.required(0)?, inputs.required(1)?, self.name(), op_union)
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
        "Returns the geometric union of two polygon geometries. Non-polygon inputs return null."
    }
}

#[must_use]
pub fn st_union(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StUnion, vec![geom_a, geom_b]).into()
}
