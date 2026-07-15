use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::{
    mbr::wkb_to_mbr,
    utils::{get_geometry_binary, validate_geometry_field},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StBbox;

fn bbox_struct_fields() -> Vec<Field> {
    vec![
        Field::new("min_x", DataType::Float64),
        Field::new("min_y", DataType::Float64),
        Field::new("max_x", DataType::Float64),
        Field::new("max_y", DataType::Float64),
    ]
}

#[typetag::serde]
impl ScalarUDF for StBbox {
    fn name(&self) -> &'static str {
        "st_bbox"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let series = inputs.required(0)?;
        let binary = get_geometry_binary(series)?;
        let len = binary.len();
        let mut min_x: Vec<Option<f64>> = Vec::with_capacity(len);
        let mut min_y: Vec<Option<f64>> = Vec::with_capacity(len);
        let mut max_x: Vec<Option<f64>> = Vec::with_capacity(len);
        let mut max_y: Vec<Option<f64>> = Vec::with_capacity(len);
        for i in 0..len {
            match binary.get(i).and_then(wkb_to_mbr) {
                Some([mnx, mny, mxx, mxy]) => {
                    min_x.push(Some(mnx));
                    min_y.push(Some(mny));
                    max_x.push(Some(mxx));
                    max_y.push(Some(mxy));
                }
                None => {
                    min_x.push(None);
                    min_y.push(None);
                    max_x.push(None);
                    max_y.push(None);
                }
            }
        }
        let children = vec![
            Float64Array::from_iter(Field::new("min_x", DataType::Float64), min_x.into_iter())
                .into_series(),
            Float64Array::from_iter(Field::new("min_y", DataType::Float64), min_y.into_iter())
                .into_series(),
            Float64Array::from_iter(Field::new("max_x", DataType::Float64), max_x.into_iter())
                .into_series(),
            Float64Array::from_iter(Field::new("max_y", DataType::Float64), max_y.into_iter())
                .into_series(),
        ];
        let field = Field::new(self.name(), DataType::Struct(bbox_struct_fields()));
        Ok(StructArray::new(field, children, None).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(
            self.name(),
            DataType::Struct(bbox_struct_fields()),
        ))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geometry's bounding box as a struct {min_x, min_y, max_x, max_y} (Float64)."
    }
}

#[must_use]
pub fn st_bbox(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StBbox, vec![geom]).into()
}

#[cfg(test)]
mod tests {
    use crate::mbr::wkb_to_mbr;

    #[test]
    fn test_wkb_to_mbr_point() {
        // WKB POINT(1 2) little-endian — built inline to avoid adding a hex dev-dependency
        let wkb: &[u8] = &[
            0x01, // little-endian
            0x01, 0x00, 0x00, 0x00, // type = Point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let [min_x, min_y, max_x, max_y] = wkb_to_mbr(wkb).unwrap();
        assert_eq!((min_x, min_y, max_x, max_y), (1.0, 2.0, 1.0, 2.0));
    }
}
