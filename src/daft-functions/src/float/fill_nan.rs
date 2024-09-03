use daft_core::{
    datatypes::Field, schema::Schema, series::Series, utils::supertype::try_get_supertype,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use common_error::{DaftError, DaftResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FillNanFunction;

#[typetag::serde]
impl ScalarUDF for FillNanFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "fill_nan"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, fill_value] => match (data.to_field(schema), fill_value.to_field(schema)) {
                (Ok(data_field), Ok(fill_value_field)) => {
                    match (&data_field.dtype.is_floating(), &fill_value_field.dtype.is_floating(), try_get_supertype(&data_field.dtype, &fill_value_field.dtype)) {
                        (true, true, Ok(dtype)) => Ok(Field::new(data_field.name, dtype)),
                        _ => Err(DaftError::TypeError(format!(
                            "Expects input to fill_nan to be float, but received {data_field} and {fill_value_field}",
                        ))),
                    }
                }
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, fill_value] => data.fill_nan(fill_value),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn fill_nan(data: ExprRef, fill_value: ExprRef) -> ExprRef {
    ScalarFunction::new(FillNanFunction, vec![data, fill_value]).into()
}

#[cfg(feature = "python")]
use {daft_dsl::python::PyExpr, pyo3::prelude::*};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "fill_nan")]
pub fn py_fill_nan(data: PyExpr, fill_value: PyExpr) -> PyExpr {
    let expr: ExprRef =
        ScalarFunction::new(FillNanFunction, vec![data.into(), fill_value.into()]).into();

    expr.into()
}
