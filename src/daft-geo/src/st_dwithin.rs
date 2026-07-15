use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::{
    st_distance::geom_distance,
    utils::{binary_geom_to_bool, read_f64_arg, read_f64_arg_expr, validate_geometry_field},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDwithin;

#[typetag::serde]
impl ScalarUDF for StDwithin {
    fn name(&self) -> &'static str {
        "st_dwithin"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let d = read_f64_arg(&inputs, 2, "distance", self.name())?;
        binary_geom_to_bool(
            inputs.required(0)?,
            inputs.required(1)?,
            self.name(),
            move |a: &Geometry, b: &Geometry| {
                let dist = geom_distance(a, b);
                dist.is_finite() && dist <= d
            },
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        read_f64_arg_expr(&inputs, 2, "distance", self.name())?; // validate numeric literal at plan time
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the planar distance between A and B is <= distance (coordinate units)."
    }
}

#[must_use]
pub fn st_dwithin(geom_a: ExprRef, geom_b: ExprRef, distance: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDwithin, vec![geom_a, geom_b, distance]).into()
}

#[cfg(test)]
mod tests {
    use geo::{Coord, Geometry, LineString, Point};

    use crate::st_distance::geom_distance;

    #[test]
    fn test_dwithin_boundary() {
        let a = Geometry::Point(Point::new(0.0, 0.0));
        let b = Geometry::Point(Point::new(3.0, 4.0)); // distance = 5.0
        assert!(geom_distance(&a, &b) <= 5.0 + 1e-9);
        assert!(geom_distance(&a, &b) > 4.9);
    }

    #[test]
    fn test_dwithin_unsupported_pair_is_not_finite() {
        // geom_distance returns NaN for unsupported geometry-type pairs (its `_ => f64::NAN` arm).
        // st_dwithin's predicate guards with `dist.is_finite() && dist <= d`, so such pairs are never "within".
        let line_a = Geometry::LineString(LineString(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 1.0, y: 1.0 },
        ]));
        let line_b = Geometry::LineString(LineString(vec![
            Coord { x: 5.0, y: 5.0 },
            Coord { x: 6.0, y: 6.0 },
        ]));
        let dist = geom_distance(&line_a, &line_b);
        assert!(
            !dist.is_finite(),
            "expected non-finite distance for unsupported (LineString, LineString) pair, got {dist}"
        );
    }
}
