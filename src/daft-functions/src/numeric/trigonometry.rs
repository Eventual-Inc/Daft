use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::trigonometry::TrigonometricFunction,
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::evaluate_single_numeric;

// super annoying, but using an enum with typetag::serde doesn't work with bincode because it uses Deserializer::deserialize_identifier
macro_rules! trigonometry {
    ($name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &'static str {
                TrigonometricFunction::$variant.fn_name()
            }

            fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
                if inputs.len() != 1 {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    )));
                };
                let field = inputs.first().unwrap().to_field(schema)?;
                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to trigonometry to be numeric, got {}",
                            field.dtype
                        )))
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
                evaluate_single_numeric(inputs, |s| {
                    s.trigonometry(&TrigonometricFunction::$variant)
                })
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFunction::new($variant, vec![input]).into()
        }
    };
}

trigonometry!(sin, Sin);
trigonometry!(cos, Cos);
trigonometry!(tan, Tan);
trigonometry!(cot, Cot);
trigonometry!(arcsin, ArcSin);
trigonometry!(arccos, ArcCos);
trigonometry!(arctan, ArcTan);
trigonometry!(radians, Radians);
trigonometry!(degrees, Degrees);
trigonometry!(arctanh, ArcTanh);
trigonometry!(arccosh, ArcCosh);
trigonometry!(arcsinh, ArcSinh);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Atan2 {}

#[typetag::serde]
impl ScalarUDF for Atan2 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "atan2"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 2 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        }
        let field1 = inputs.first().unwrap().to_field(schema)?;
        let field2 = inputs.get(1).unwrap().to_field(schema)?;
        let dtype = match (field1.dtype, field2.dtype) {
            (DataType::Float32, DataType::Float32) => DataType::Float32,
            (dt1, dt2) if dt1.is_numeric() && dt2.is_numeric() => DataType::Float64,
            (dt1, dt2) => {
                return Err(DaftError::TypeError(format!(
                    "Expected inputs to atan2 to be numeric, got {dt1} and {dt2}"
                )))
            }
        };
        Ok(Field::new(field1.name, dtype))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [x, y] => x.atan2(y),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn atan2(x: ExprRef, y: ExprRef) -> ExprRef {
    ScalarFunction::new(Atan2 {}, vec![x, y]).into()
}
