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

use crate::utils::{unary_geom_to_utf8, validate_geometry_field};

fn geometry_type_name(g: &Geometry) -> String {
    match g {
        Geometry::Point(_) => "Point",
        Geometry::Line(_) => "Line",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        Geometry::Rect(_) => "Polygon",
        Geometry::Triangle(_) => "Polygon",
    }
    .to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeometryType;

#[typetag::serde]
impl ScalarUDF for StGeometryType {
    fn name(&self) -> &'static str {
        "st_geometrytype"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_geom_to_utf8(inputs.required(0)?, self.name(), geometry_type_name)
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
        "Returns the type name of the geometry as a string (e.g. 'Point', 'LineString', 'Polygon')."
    }
}

#[must_use]
pub fn st_geometrytype(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StGeometryType, vec![geom]).into()
}
