use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Geometry, MakeValid};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_geom, validate_geometry_field};

/// Repair an invalid geometry.
///
/// The pure-Rust `geo` crate only implements `MakeValid` for `Polygon` and
/// `MultiPolygon` (it repairs self-intersections, bowties, etc. and returns a
/// `MultiPolygon`). Unlike PostGIS/GEOS, it cannot repair other geometry types,
/// so non-polygonal inputs are passed through unchanged — a valid Point or
/// LineString is already valid. Returns `None` (null) when the repair fails.
///
/// Wrapped in `catch_unwind` for defensive robustness, matching the overlay/
/// convex-hull ops, since geometric repair can panic on degenerate input.
fn apply_makevalid(g: &Geometry) -> Option<Geometry> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match g {
        Geometry::Polygon(p) => p.make_valid().ok().map(Geometry::MultiPolygon),
        Geometry::MultiPolygon(mp) => mp.make_valid().ok().map(Geometry::MultiPolygon),
        other => Some(other.clone()),
    }));
    result.ok().flatten()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StMakeValid;

#[typetag::serde]
impl ScalarUDF for StMakeValid {
    fn name(&self) -> &'static str {
        "st_makevalid"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_geom(inputs.required(0)?, self.name(), apply_makevalid)
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
        "Repairs invalid polygonal geometries (self-intersections, bowties, etc.), \
         returning a valid MultiPolygon. Non-polygonal geometries (Point, LineString, ...) \
         are returned unchanged, since the pure-Rust engine only repairs areal geometries. \
         Returns null when the geometry cannot be repaired."
    }
}

#[must_use]
pub fn st_makevalid(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StMakeValid, vec![geom]).into()
}
