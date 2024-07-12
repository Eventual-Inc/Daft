use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, UInt64Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct HashFunction;

#[typetag::serde]
impl ScalarUDF for HashFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "hash"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.hash(None).map(|s| s.into_series()),
            [input, seed] => {
                match seed.len() {
                    1 => {
                        let seed = seed.cast(&DataType::UInt64)?;
                        // There's no way to natively extend the array, so we extract the element and repeat it.
                        let seed = seed.u64().unwrap();
                        let seed = seed.get(0).unwrap();
                        let seed = UInt64Array::from_iter(
                            "seed",
                            std::iter::repeat(Some(seed)).take(input.len()),
                        );
                        input.hash(Some(&seed)).map(|s| s.into_series())
                    }
                    _ if seed.len() == input.len() => {
                        let seed = seed.cast(&DataType::UInt64)?;
                        let seed = seed.u64().unwrap();

                        input.hash(Some(seed)).map(|s| s.into_series())
                    }
                    _ => Err(DaftError::ValueError(
                        "Seed must be a single value or the same length as the input".to_string(),
                    )),
                }
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] | [input, _] => match input.to_field(schema) {
                Ok(field) => Ok(Field::new(field.name, DataType::UInt64)),
                e => e,
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn hash(input: ExprRef, seed: Option<ExprRef>) -> ExprRef {
    let inputs = match seed {
        Some(seed) => vec![input, seed],
        None => vec![input],
    };

    ScalarFunction::new(HashFunction {}, inputs).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn hash(expr: PyExpr, seed: Option<PyExpr>) -> PyResult<PyExpr> {
        use super::hash;
        Ok(hash(expr.into(), seed.map(|s| s.into())).into())
    }
}
