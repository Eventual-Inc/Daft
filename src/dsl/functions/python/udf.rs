use indexmap::IndexMap;

use crate::{datatypes::Field, dsl::Expr, error::DaftResult, schema::Schema, series::Series};

use super::super::FunctionEvaluator;
use super::{PyUdfInput, SerializablePyObject};

pub(super) struct PythonUDFEvaluator {
    _pyfunc: SerializablePyObject,
    _args: Vec<PyUdfInput>,
    _kwargs: IndexMap<String, PyUdfInput>,
}

impl PythonUDFEvaluator {
    pub fn new(
        pyfunc: SerializablePyObject,
        args: Vec<PyUdfInput>,
        kwargs: IndexMap<String, PyUdfInput>,
    ) -> PythonUDFEvaluator {
        PythonUDFEvaluator {
            _pyfunc: pyfunc,
            _args: args,
            _kwargs: kwargs,
        }
    }
}

impl FunctionEvaluator for PythonUDFEvaluator {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(&self, _inputs: &[Expr], _schema: &Schema) -> DaftResult<Field> {
        todo!()
    }

    fn evaluate(&self, _inputs: &[Series]) -> DaftResult<Series> {
        todo!()
    }
}
