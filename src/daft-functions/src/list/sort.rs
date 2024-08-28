use common_error::{DaftError, DaftResult};

use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct ListSortFunction {}

#[typetag::serde]
impl ScalarUDF for ListSortFunction {
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
                    (l @ DataType::List(_), DataType::Boolean)
                    | (l @ DataType::FixedSizeList(_, _), DataType::Boolean) => {
                        Ok(Field::new(field.name, l.clone()))
                    }
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

pub fn list_sort(input: ExprRef, desc: ExprRef) -> ExprRef {
    ScalarFunction::new(ListSortFunction {}, vec![input, desc]).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn list_sort(expr: PyExpr, desc: PyExpr) -> PyResult<PyExpr> {
        let expr = super::list_sort(expr.into(), desc.into());
        Ok(expr.into())
    }
}
