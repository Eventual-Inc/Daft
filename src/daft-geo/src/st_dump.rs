use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{scalar::ScalarFn, FunctionArgs, ScalarUDF},
    ExprRef,
};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::utils::{
    dump_item_struct_fields, dump_items_to_list_struct_series, geom_ref_to_wkb, geom_to_wkb,
    get_geometry_binary, parse_wkb, validate_geometry_field,
};

fn dump_geometry_parts_wkb_with_paths(
    geom: &Geometry,
    prefix: &[i64],
    out: &mut Vec<(Vec<i64>, Option<Vec<u8>>)>,
) {
    match geom {
        Geometry::MultiPoint(multi_point) => {
            for (i, point) in multi_point.0.iter().enumerate() {
                let mut path = prefix.to_vec();
                path.push((i + 1) as i64);
                out.push((path, geom_ref_to_wkb(point).ok()));
            }
        }
        Geometry::MultiLineString(multi_line_string) => {
            for (i, line) in multi_line_string.0.iter().enumerate() {
                let mut path = prefix.to_vec();
                path.push((i + 1) as i64);
                out.push((path, geom_ref_to_wkb(line).ok()));
            }
        }
        Geometry::MultiPolygon(multi_polygon) => {
            for (i, poly) in multi_polygon.0.iter().enumerate() {
                let mut path = prefix.to_vec();
                path.push((i + 1) as i64);
                out.push((path, geom_ref_to_wkb(poly).ok()));
            }
        }
        Geometry::GeometryCollection(collection) => {
            for (i, child) in collection.0.iter().enumerate() {
                let mut child_prefix = prefix.to_vec();
                child_prefix.push((i + 1) as i64);
                dump_geometry_parts_wkb_with_paths(child, &child_prefix, out);
            }
        }
        _ => out.push((prefix.to_vec(), geom_to_wkb(geom).ok())),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDump;

#[typetag::serde]
impl ScalarUDF for StDump {
    fn name(&self) -> &'static str {
        "st_dump"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let series = inputs.required(0)?;
        let binary = get_geometry_binary(series)?;

        let len = binary.len();
        let mut child_paths: Vec<Vec<i64>> = Vec::new();
        let mut child_wkb_values: Vec<Option<Vec<u8>>> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(len + 1);
        let mut outer_validity: Vec<bool> = Vec::with_capacity(len);
        offsets.push(0);

        for opt in binary.into_iter() {
            match opt.and_then(|b| parse_wkb(b).ok()) {
                Some(geom) => {
                    outer_validity.push(true);
                    let mut row_items: Vec<(Vec<i64>, Option<Vec<u8>>)> = Vec::new();
                    dump_geometry_parts_wkb_with_paths(&geom, &[], &mut row_items);
                    for (path, wkb) in row_items {
                        child_paths.push(path);
                        child_wkb_values.push(wkb);
                    }
                }
                None => outer_validity.push(false),
            }
            offsets.push(child_wkb_values.len() as i64);
        }

        dump_items_to_list_struct_series(
            self.name(),
            child_paths,
            child_wkb_values,
            offsets,
            outer_validity,
        )
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
        "Returns the geometry's components as List[Struct{path, geom}]. Atomic geometries return one item with empty path; multi-geometries return one item per member with 1-based path indexes; GeometryCollections are recursively flattened with nested paths."
    }
}

#[must_use]
pub fn st_dump(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDump, vec![geom]).into()
}
