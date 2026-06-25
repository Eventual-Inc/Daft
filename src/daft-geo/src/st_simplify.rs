use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Schema};
use daft_core::series::Series;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{Geometry, Simplify};
use serde::{Deserialize, Serialize};

use crate::utils::{read_f64_arg, read_f64_arg_expr, unary_geom_to_geom, validate_geometry_field};

/// Apply Ramer–Douglas–Peucker simplification with the given tolerance.
///
/// `geo::Simplify` is implemented for LineString, MultiLineString, Polygon, and
/// MultiPolygon.  For all other geometry types (Point, MultiPoint, etc.) the
/// geometry is returned unchanged.  The call is wrapped in `catch_unwind` for
/// defensive robustness consistent with other geo ops in this crate.
fn apply_simplify(g: &Geometry, tolerance: f64) -> Option<Geometry> {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        match g {
            Geometry::LineString(ls) => Geometry::LineString(ls.simplify(tolerance)),
            Geometry::MultiLineString(mls) => Geometry::MultiLineString(mls.simplify(tolerance)),
            Geometry::Polygon(poly) => Geometry::Polygon(poly.simplify(tolerance)),
            Geometry::MultiPolygon(mp) => Geometry::MultiPolygon(mp.simplify(tolerance)),
            // All other types (Point, MultiPoint, GeometryCollection, etc.) pass through
            other => other.clone(),
        }
    }));
    result.ok()
}

/// Zero-field unit struct; `tolerance` is read from the trailing positional
/// argument at call time (not stored in the struct).
///
/// This is required because the Python `_call_builtin_scalar_fn` path resolves
/// the registered instance by name and cannot propagate struct-field parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StSimplify;

#[typetag::serde]
impl ScalarUDF for StSimplify {
    fn name(&self) -> &'static str {
        "st_simplify"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let tolerance = read_f64_arg(&inputs, 1, "tolerance", self.name())?;
        unary_geom_to_geom(inputs.required(0)?, self.name(), |g| {
            apply_simplify(g, tolerance)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        let _tolerance = read_f64_arg_expr(&inputs, 1, "tolerance", self.name())?;
        Ok(Field::new(self.name(), DataType::Geometry))
    }

    fn docstring(&self) -> &'static str {
        "Simplifies a geometry using the Ramer–Douglas–Peucker algorithm with the given tolerance. \
         Applies to LineString, MultiLineString, Polygon, and MultiPolygon; \
         other geometry types are returned unchanged."
    }
}

#[must_use]
pub fn st_simplify(geom: ExprRef, tolerance: f64) -> ExprRef {
    ScalarFn::builtin(StSimplify, vec![geom, daft_dsl::lit(tolerance)]).into()
}
