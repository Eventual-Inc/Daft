use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, IntoSeries, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineFunction {}

#[typetag::serde]
impl ScalarUDF for CosineFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "hash"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [from, to] => {
                unimplemented!("implement cosine")
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [from, to] => {
                let from = from.to_field(schema)?;
                let to = to.to_field(schema)?;
                if from.dtype != to.dtype {
                    return Err(DaftError::ValueError("Expected same dtype".to_string()));
                }
                if !(from.dtype.is_fixed_size_numeric() && to.dtype.is_numeric()) {
                    return Err(DaftError::ValueError("Expected numeric dtype".to_string()));
                }
                todo!()
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }
}

pub fn cosine(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFunction::new(CosineFunction {}, vec![a, b]).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn cosine(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
        Ok(super::cosine(a.into(), b.into()).into())
    }
}
