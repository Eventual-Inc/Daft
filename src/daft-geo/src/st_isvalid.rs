use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use serde::{Deserialize, Serialize};

use crate::utils::{unary_geom_to_bool, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StIsValid;

#[typetag::serde]
impl ScalarUDF for StIsValid {
    fn name(&self) -> &'static str { "st_isvalid" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        unary_geom_to_bool(inputs.required(0)?, self.name(), |g| {
            use geo::algorithm::Validation;
            g.is_valid()
        })
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the geometry is topologically valid according to OGC Simple Feature rules."
    }
}

#[must_use]
pub fn st_isvalid(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StIsValid, vec![geom]).into()
}

#[cfg(test)]
mod tests {
    use geo::algorithm::Validation;

    #[test]
    fn test_simple_square_is_valid() {
        use geo::{Geometry, LineString, Polygon};
        let square = Polygon::new(
            LineString::from(vec![(0.0_f64, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
            vec![],
        );
        let g = Geometry::Polygon(square);
        assert!(g.is_valid(), "simple square should be valid");
    }

    #[test]
    fn test_bowtie_polygon_is_invalid() {
        use geo::{Geometry, LineString, Polygon};
        // A "bowtie" (self-intersecting ring): the ring crosses itself
        // Coords: (0,0)→(2,2)→(2,0)→(0,2)→(0,0) — the edges cross at (1,1)
        let bowtie = Polygon::new(
            LineString::from(vec![
                (0.0_f64, 0.0),
                (2.0, 2.0),
                (2.0, 0.0),
                (0.0, 2.0),
                (0.0, 0.0),
            ]),
            vec![],
        );
        let g = Geometry::Polygon(bowtie);
        assert!(!g.is_valid(), "bowtie polygon should be invalid");
    }
}
