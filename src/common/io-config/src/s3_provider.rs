use std::hash::{Hash, Hasher};

use pyo3::{
    types::{PyBytes, PyModule},
    PyAny, PyObject, PyResult, Python, ToPyObject,
};
use serde::{de::Visitor, Serialize};

#[derive(Clone, Debug)]
pub struct S3CredentialsProvider {
    pub provider: PyObject,
    pub hash: isize,
}

impl S3CredentialsProvider {
    pub fn new(py: Python, provider: &PyAny) -> PyResult<Self> {
        Ok(S3CredentialsProvider {
            provider: provider.to_object(py),
            hash: provider.hash()?,
        })
    }
}

impl PartialEq for S3CredentialsProvider {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for S3CredentialsProvider {}

impl Hash for S3CredentialsProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl Serialize for S3CredentialsProvider {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, "daft.pickle").unwrap();
            let dumps = serde_module.getattr("dumps").unwrap();
            let pybytes = dumps
                .call1((self.provider.clone_ref(py).into_ref(py),))
                .unwrap();
            serializer.serialize_bytes(pybytes.downcast::<PyBytes>().unwrap().as_bytes())
        })
    }
}

struct S3CredentialsProviderVisitor;

impl<'de> Visitor<'de> for S3CredentialsProviderVisitor {
    type Value = S3CredentialsProvider;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Python object of type S3CredentialsProvider")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Python::with_gil(|py| {
            let serde_module = PyModule::import(py, "daft.pickle").unwrap();
            let loads = serde_module.getattr("loads").unwrap();
            let py_partial_udf = loads.call1((v,)).unwrap();
            Ok(S3CredentialsProvider {
                provider: py_partial_udf.to_object(py),
                hash: py_partial_udf.hash().unwrap(),
            })
        })
    }
}

impl<'de> serde::Deserialize<'de> for S3CredentialsProvider {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(S3CredentialsProviderVisitor)
    }
}
