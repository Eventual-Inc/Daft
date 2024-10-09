use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    lit, ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListSort {}

#[typetag::serde]
impl ScalarUDF for ListSort {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_sort"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, desc] => match (data.to_field(schema), desc.to_field(schema)) {
                (Ok(field), Ok(desc_field)) => match (&field.dtype, &desc_field.dtype) {
                    (
                        l @ (DataType::List(_) | DataType::FixedSizeList(_, _)),
                        DataType::Boolean,
                    ) => Ok(Field::new(field.name, l.clone())),
                    (a, b) => Err(DaftError::TypeError(format!(
                        "Expects inputs to list_sort to be list and bool, but received {a} and {b}",
                    ))),
                },
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
            [data, desc] => data.list_sort(desc),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_sort(input: ExprRef, desc: Option<ExprRef>) -> ExprRef {
    let desc = desc.unwrap_or_else(|| lit(false));
    ScalarFunction::new(ListSort {}, vec![input, desc]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_sort")]
pub fn py_list_sort(expr: PyExpr, desc: PyExpr) -> PyResult<PyExpr> {
    Ok(list_sort(expr.into(), Some(desc.into())).into())
}
