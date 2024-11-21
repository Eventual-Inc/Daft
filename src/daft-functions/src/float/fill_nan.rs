use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
    utils::supertype::try_get_supertype,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FillNan {}

#[typetag::serde]
impl ScalarUDF for FillNan {
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
                            "Expects input for fill_nan to be float, but received {data_field} and {fill_value_field}",
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

#[must_use]
pub fn fill_nan(input: ExprRef, fill_value: ExprRef) -> ExprRef {
    ScalarFunction::new(FillNan {}, vec![input, fill_value]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "fill_nan")]
pub fn py_fill_nan(expr: PyExpr, fill_value: PyExpr) -> PyResult<PyExpr> {
    Ok(fill_nan(expr.into(), fill_value.into()).into())
}
