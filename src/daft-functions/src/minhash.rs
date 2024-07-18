use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct MinHashFunction {
    num_hashes: usize,
    ngram_size: usize,
    seed: u32,
}

#[typetag::serde]
impl ScalarUDF for MinHashFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "minhash"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.minhash(self.num_hashes, self.ngram_size, self.seed),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(field) => match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(
                        field.name,
                        DataType::FixedSizeList(Box::new(DataType::UInt32), self.num_hashes),
                    )),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to minhash to be utf8, but received {field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn minhash(input: ExprRef, num_hashes: usize, ngram_size: usize, seed: u32) -> ExprRef {
    ScalarFunction::new(
        MinHashFunction {
            num_hashes,
            ngram_size,
            seed,
        },
        vec![input],
    )
    .into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

    #[pyfunction]
    pub fn minhash(expr: PyExpr, num_hashes: i64, ngram_size: i64, seed: i64) -> PyResult<PyExpr> {
        if num_hashes <= 0 {
            return Err(PyValueError::new_err(format!(
                "num_hashes must be positive: {num_hashes}"
            )));
        }
        if ngram_size <= 0 {
            return Err(PyValueError::new_err(format!(
                "ngram_size must be positive: {ngram_size}"
            )));
        }
        let cast_seed = seed as u32;

        let expr = super::minhash(
            expr.into(),
            num_hashes as usize,
            ngram_size as usize,
            cast_seed,
        );
        Ok(expr.into())
    }
}
