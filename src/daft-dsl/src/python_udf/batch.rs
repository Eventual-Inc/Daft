use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::{prelude::DataType, series::Series};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    Expr, ExprRef,
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    python_udf::PyScalarFn,
};

#[allow(clippy::too_many_arguments)]
pub fn batch_udf(
    name: &str,
    cls: RuntimePyObject,
    method: RuntimePyObject,
    return_dtype: DataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<usize>,
    batch_size: Option<usize>,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::Batch(BatchPyFn {
        function_name: Arc::from(name),
        cls,
        method,
        return_dtype,
        gpus,
        use_process,
        max_concurrency,
        batch_size,
        original_args,
        args,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BatchPyFn {
    pub function_name: Arc<str>,
    pub cls: RuntimePyObject,
    pub method: RuntimePyObject,
    pub return_dtype: DataType,
    pub gpus: usize,
    pub use_process: Option<bool>,
    pub max_concurrency: Option<usize>,
    pub batch_size: Option<usize>,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
}

impl Display for BatchPyFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_str = self.args.iter().map(|expr| expr.to_string()).join(", ");

        write!(f, "{}({})", self.function_name, children_str)
    }
}

impl BatchPyFn {
    pub fn with_new_children(&self, children: Vec<ExprRef>) -> Self {
        assert_eq!(
            children.len(),
            self.args.len(),
            "There must be the same amount of new children as original."
        );

        Self {
            function_name: self.function_name.clone(),
            cls: self.cls.clone(),
            method: self.method.clone(),
            return_dtype: self.return_dtype.clone(),
            gpus: self.gpus,
            use_process: self.use_process,
            max_concurrency: self.max_concurrency,
            batch_size: self.batch_size,
            original_args: self.original_args.clone(),
            args: children,
        }
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series]) -> DaftResult<Series> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let py_args = args
            .iter()
            .map(|s| PySeries::from(s.clone()))
            .collect::<Vec<_>>();

        let result_series = Python::with_gil(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_batch"))?;

            let result = func.call1((
                self.cls.as_ref(),
                self.method.as_ref(),
                self.original_args.as_ref(),
                py_args,
            ))?;

            let result_series = result.extract::<PySeries>()?.series;

            PyResult::Ok(result_series)
        })?;

        let name = args[0].name();

        Ok(result_series.cast(&self.return_dtype)?.rename(name))
    }

    #[cfg(not(feature = "python"))]
    pub fn call(&self, args: &[Series]) -> DaftResult<Series> {
        panic!("Cannot evaluate a BatchPyFn without compiling for Python");
    }
}
