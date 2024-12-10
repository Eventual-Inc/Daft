use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListFill {}

#[typetag::serde]
impl ScalarUDF for ListFill {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "fill"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [n, elem] => {
                let num_field = n.to_field(schema)?;
                let elem_field = elem.to_field(schema)?;
                if !num_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected num field to be of numeric type, received: {}",
                        num_field.dtype
                    )));
                }
                elem_field.to_list_field()
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [num, elem] => {
                let num = num.cast(&DataType::Int64)?;
                let num_array = num.i64()?;
                elem.list_fill(num_array)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_fill(n: ExprRef, elem: ExprRef) -> ExprRef {
    ScalarFunction::new(ListFill {}, vec![n, elem]).into()
}
