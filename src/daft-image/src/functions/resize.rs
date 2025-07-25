use common_error::{ensure, DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef, LiteralValue,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageResize;

#[typetag::serde]
impl ScalarUDF for ImageResize {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let height = inputs.required(("h", "height")).and_then(|s| {
            ensure!(s.len() == 1, "height must be a scalar");
            Ok(s.cast(&DataType::UInt32)
                .unwrap()
                .u32()
                .unwrap()
                .get(0)
                .unwrap())
        })?;

        let width = inputs.required(("w", "width")).and_then(|s| {
            ensure!(s.len() == 1, "width must be a scalar");
            Ok(s.cast(&DataType::UInt32)
                .unwrap()
                .u32()
                .unwrap()
                .get(0)
                .unwrap())
        })?;

        crate::series::resize(input, width, height)
    }

    fn name(&self) -> &'static str {
        "image_resize"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?;
        let field = input.to_field(schema)?;
        let width = inputs.required(("w", "width")).and_then(|e| {
            let fld = e.to_field(schema)?;
            ensure!(
                fld.dtype.is_numeric() && !fld.dtype.is_floating(),
                "width must be a non floating numeric type"
            );
            e.as_literal()
                .and_then(|lit| {
                    Some(match lit {
                        LiteralValue::Int8(i) if *i > 0i8 => *i as u32,
                        LiteralValue::UInt8(u) => *u as u32,
                        LiteralValue::Int16(i) if *i > 0i16 => *i as u32,
                        LiteralValue::UInt16(u) => *u as u32,
                        LiteralValue::Int32(i) => *i as u32,
                        LiteralValue::UInt32(u) => *u,
                        LiteralValue::Int64(i) => return u32::try_from(*i).ok(),
                        LiteralValue::UInt64(u) => return u32::try_from(*u).ok(),
                        _ => unreachable!(),
                    })
                })
                .ok_or_else(|| {
                    DaftError::ValueError("width must be a positive integer".to_string())
                })
        })?;

        let height = inputs.required(("h", "height")).and_then(|e| {
            let fld = e.to_field(schema)?;
            ensure!(
                fld.dtype.is_numeric() && !fld.dtype.is_floating(),
                "height must be a non floating numeric type"
            );
            e.as_literal()
                .and_then(|lit| {
                    Some(match lit {
                        LiteralValue::Int8(i) if *i > 0i8 => *i as u32,
                        LiteralValue::UInt8(u) => *u as u32,
                        LiteralValue::Int16(i) if *i > 0i16 => *i as u32,
                        LiteralValue::UInt16(u) => *u as u32,
                        LiteralValue::Int32(i) => *i as u32,
                        LiteralValue::UInt32(u) => *u,
                        LiteralValue::Int64(i) => return u32::try_from(*i).ok(),
                        LiteralValue::UInt64(u) => return u32::try_from(*u).ok(),
                        _ => unreachable!(),
                    })
                })
                .ok_or_else(|| {
                    DaftError::ValueError("width must be a positive integer".to_string())
                })
        })?;
        match field.dtype {
            DataType::Image(mode) => match mode {
                Some(mode) => Ok(Field::new(
                    field.name,
                    DataType::FixedShapeImage(mode, height, width),
                )),
                None => Ok(field),
            },
            DataType::FixedShapeImage(mode, ..) => Ok(Field::new(
                field.name,
                DataType::FixedShapeImage(mode, height, width),
            )),
            _ => Err(DaftError::TypeError(format!(
                "ImageResize can only resize ImageArrays and FixedShapeImageArrays, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Resizes an image to the specified width and height."
    }
}
