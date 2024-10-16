use std::hash::{BuildHasher, BuildHasherDefault};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use daft_hash::{MurBuildHasher, Sha1Hasher};
use pyo3::{pyclass, pymethods, types::PyType, PyErr, PyResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinHashFunction {
    pub num_hashes: usize,
    pub ngram_size: usize,
    pub seed: u32,
    pub hash_function: HashFunctionLiteral,
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
        let [input] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        match self.hash_function {
            HashFunctionLiteral::MurmurHash3 => {
                let hasher = MurBuildHasher::new(self.seed);
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
            HashFunctionLiteral::XxHash => {
                let hasher = xxhash_rust::xxh64::Xxh64Builder::new(self.seed);
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
            HashFunctionLiteral::Sha1 => {
                let hasher = BuildHasherDefault::<Sha1Hasher>::default();
                input.minhash(self.num_hashes, self.ngram_size, self.seed, &hasher)
            }
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

#[must_use]
pub fn minhash(
    input: ExprRef,
    num_hashes: usize,
    ngram_size: usize,
    seed: u32,
    hash_function: HashFunctionLiteral,
) -> ExprRef {
    ScalarFunction::new(
        MinHashFunction {
            num_hashes,
            ngram_size,
            seed,
            hash_function,
        },
        vec![input],
    )
    .into()
}

// todo: what
#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashFunctionLiteral {
    MurmurHash3,
    XxHash,
    Sha1,
}

#[pymethods]
impl HashFunctionLiteral {
    // todo: is there an updated way to do this? it says it is using a deprecated method
    #[classmethod]
    fn from_str(_cls: &PyType, s: &str) -> PyResult<Self> {
        match s.to_lowercase().as_str() {
            "murmurhash3" => Ok(Self::MurmurHash3),
            "xxhash" => Ok(Self::XxHash),
            "sha1" => Ok(Self::Sha1),
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid hash function: {}",
                s
            ))),
        }
    }
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{exceptions::PyValueError, pyfunction, PyResult};

    use crate::minhash::HashFunctionLiteral;

    #[pyfunction]
    pub fn minhash(
        expr: PyExpr,
        num_hashes: i64,
        ngram_size: i64,
        seed: i64,
        hash_function: HashFunctionLiteral,
    ) -> PyResult<PyExpr> {
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
            hash_function,
        );
        Ok(expr.into())
    }
}
