use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, Series};
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct CbrtFunction;

#[typetag::serde]
impl ScalarUDF for CbrtFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cbrt"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                let dtype = field.dtype.to_floating_representation()?;
                Ok(Field::new(field.name, dtype))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.cbrt(),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::{functions::ScalarFunction, python::PyExpr, ExprRef};
    use pyo3::{pyfunction, PyResult};

    use super::CbrtFunction;

    #[pyfunction]
    pub fn cbrt(expr: PyExpr) -> PyResult<PyExpr> {
        let scalar_function = ScalarFunction::new(CbrtFunction, vec![expr.into()]);
        let expr = ExprRef::from(scalar_function);
        Ok(expr.into())
    }
}
