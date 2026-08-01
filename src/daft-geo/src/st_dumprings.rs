use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{scalar::ScalarFn, FunctionArgs, ScalarUDF},
    ExprRef,
};
use geo::{Geometry, Polygon};
use serde::{Deserialize, Serialize};

use crate::utils::{
    dump_item_struct_fields, unary_geom_to_list_dump_item, validate_geometry_field,
};

fn polygon_rings_with_paths(polygon: Polygon) -> Vec<(Vec<i64>, Geometry)> {
    let (exterior, interiors) = polygon.into_inner();
    if exterior.0.is_empty() && interiors.is_empty() {
        return Vec::new();
    }

    let mut rings = Vec::with_capacity(1 + interiors.len());
    rings.push((vec![0], Geometry::Polygon(Polygon::new(exterior, vec![]))));
    for (i, ring) in interiors.into_iter().enumerate() {
        rings.push((
            vec![(i + 1) as i64],
            Geometry::Polygon(Polygon::new(ring, vec![])),
        ));
    }
    rings
}

fn dump_polygon_rings(geom: Geometry) -> Option<Vec<(Vec<i64>, Geometry)>> {
    match geom {
        Geometry::Polygon(polygon) => Some(polygon_rings_with_paths(polygon)),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDumpRings;

#[typetag::serde]
impl ScalarUDF for StDumpRings {
    fn name(&self) -> &'static str {
        "st_dumprings"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_list_dump_item(inputs.required(0)?, self.name(), dump_polygon_rings)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(
            self.name(),
            DataType::List(Box::new(DataType::Struct(dump_item_struct_fields()))),
        ))
    }

    fn docstring(&self) -> &'static str {
        "Returns polygon rings as List[Struct{path, geom}] where geom is a single-ring Polygon and path is [0] for exterior ring then [1..n] for interior rings. Only Polygon inputs are supported; non-polygonal inputs return null."
    }
}

#[must_use]
pub fn st_dumprings(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDumpRings, vec![geom]).into()
}
