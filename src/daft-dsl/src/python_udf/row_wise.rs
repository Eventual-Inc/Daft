use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    Expr, ExprRef,
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    python_udf::PyScalarFn,
};

#[allow(clippy::too_many_arguments)]
pub fn row_wise_udf(
    name: &str,
    cls: RuntimePyObject,
    method: RuntimePyObject,
    is_async: bool,
    return_dtype: DataType,
    gpus: usize,
    use_process: Option<bool>,
    max_concurrency: Option<usize>,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(RowWisePyFn {
        function_name: Arc::from(name),
        cls,
        method,
        is_async,
        return_dtype,
        original_args,
        args,
        gpus,
        use_process,
        max_concurrency,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowWisePyFn {
    pub function_name: Arc<str>,
    pub cls: RuntimePyObject,
    pub method: RuntimePyObject,
    pub is_async: bool,
    pub return_dtype: DataType,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
    pub gpus: usize,
    pub use_process: Option<bool>,
    pub max_concurrency: Option<usize>,
}

impl Display for RowWisePyFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_str = self.args.iter().map(|expr| expr.to_string()).join(", ");

        write!(f, "{}({})", self.function_name, children_str)
    }
}

impl RowWisePyFn {
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
            is_async: self.is_async,
            return_dtype: self.return_dtype.clone(),
            original_args: self.original_args.clone(),
            args: children,
            gpus: self.gpus,
            use_process: self.use_process,
            max_concurrency: self.max_concurrency,
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn call(&self, _args: &[Series]) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series]) -> DaftResult<Series> {
        let num_rows = args
            .iter()
            .map(Series::len)
            .max()
            .expect("RowWisePyFn should have at least one argument");

        for a in args {
            assert!(
                a.len() == num_rows || a.len() == 1,
                "arg lengths differ: {} vs {}",
                num_rows,
                a.len()
            );
        }

        if self.is_async {
            self.call_async(args, num_rows)
        } else {
            self.call_serial(args, num_rows)
        }
    }

    #[cfg(feature = "python")]
    fn call_async(&self, args: &[Series], num_rows: usize) -> DaftResult<Series> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;
        let py_return_type = daft_core::python::PyDataType::from(self.return_dtype.clone());
        let cls_ref = self.cls.as_ref();
        let method_ref = self.method.as_ref();
        let args_ref = self.original_args.as_ref();

        Ok(pyo3::Python::with_gil(|py| {
            let f = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_async_batch"))?;

            let mut evaluated_args = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let py_args_for_row = args
                    .iter()
                    .map(|s| {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        lit.into_pyobject(py).map_err(|e| e.into())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                evaluated_args.push(py_args_for_row);
            }

            let res = f.call1((
                cls_ref,
                method_ref,
                py_return_type.clone(),
                args_ref,
                evaluated_args,
            ))?;
            let name = args[0].name();

            let result_series = res.extract::<PySeries>()?.series;

            Ok::<_, PyErr>(result_series.rename(name))
        })?)
    }

    #[cfg(feature = "python")]
    fn call_serial(&self, args: &[Series], num_rows: usize) -> DaftResult<Series> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let cls_ref = self.cls.as_ref();
        let method_ref = self.method.as_ref();
        let args_ref = self.original_args.as_ref();
        let name = args[0].name();

        Python::with_gil(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_func"))?;

            // pre-allocating py_args vector so we're not creating a new vector for each iteration
            let mut py_args = Vec::with_capacity(args.len());
            let outputs = (0..num_rows)
                .map(|i| {
                    for s in args {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        let pyarg = lit.into_pyobject(py)?;
                        py_args.push(pyarg);
                    }

                    let result = func.call1((cls_ref, method_ref, args_ref, &py_args))?;
                    py_args.clear();
                    DaftResult::Ok(result)
                })
                .collect::<DaftResult<Vec<_>>>()?;

            Ok(PySeries::from_pylist_impl(name, outputs, self.return_dtype.clone())?.series)
        })
    }
}
