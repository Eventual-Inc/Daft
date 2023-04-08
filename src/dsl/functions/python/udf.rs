
use indexmap::IndexMap;

use crate::{
    datatypes::{Field},
    dsl::Expr,
    error::{DaftResult},
    schema::Schema,
    series::Series,
};

use super::{SerializablePyObject, PyUdfInput};
use super::super::FunctionEvaluator;

pub(super) struct PythonUDFEvaluator {
    pyfunc: SerializablePyObject,
    args: Vec<PyUdfInput>,
    kwargs: IndexMap<String, PyUdfInput>,
}

impl FunctionEvaluator for PythonUDFEvaluator {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(&self, inputs: &[Expr], schema: &Schema) -> DaftResult<Field> {
        todo!()
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        todo!()
    }
}
