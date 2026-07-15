use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Geometry, InteriorPoint};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_geom, validate_geometry_field};

/// Compute a representative point guaranteed to lie on the geometry's surface.
///
/// Backed by `geo::InteriorPoint`, which returns `Option<Point>` — `None` for
/// empty geometries. Unlike the centroid, the returned point is always on the
/// geometry itself (matching PostGIS `ST_PointOnSurface`).
fn apply_pointonsurface(g: &Geometry) -> Option<Geometry> {
    g.interior_point().map(Geometry::Point)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StPointOnSurface;

#[typetag::serde]
impl ScalarUDF for StPointOnSurface {
    fn name(&self) -> &'static str {
        "st_pointonsurface"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_geom(inputs.required(0)?, self.name(), apply_pointonsurface)
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
        "Returns a Point guaranteed to lie on the surface of the geometry. \
         Unlike st_centroid, the result always intersects the input geometry. \
         Returns null for empty geometries."
    }
}

#[must_use]
pub fn st_pointonsurface(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StPointOnSurface, vec![geom]).into()
}
