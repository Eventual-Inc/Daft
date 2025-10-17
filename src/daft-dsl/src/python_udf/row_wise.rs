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
    max_retries: Option<usize>,
    on_error: crate::functions::python::OnError,
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
        on_error,
        max_retries,
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
    pub max_retries: Option<usize>,
    pub on_error: crate::functions::python::OnError,
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
            max_retries: self.max_retries,
            on_error: self.on_error,
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
        use common_error::DaftError;
        use daft_core::python::PySeries;
        use indexmap::IndexMap;
        use pyo3::prelude::*;
        let cls_ref = self.cls.as_ref();
        let method_ref = self.method.as_ref();
        let args_ref = self.original_args.as_ref();
        let max_retries = self.max_retries.unwrap_or(0);

        pyo3::Python::attach(|py| {
            let f = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_async_batch"))?;

            let mut evaluated_args = IndexMap::with_capacity(num_rows);
            for i in 0..num_rows {
                let py_args_for_row = args
                    .iter()
                    .map(|s| {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        lit.into_pyobject(py).map_err(|e| e.into())
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                evaluated_args.insert(i, py_args_for_row);
            }

            let res = f.call1((cls_ref, method_ref, args_ref, evaluated_args))?;

            let name = args[0].name();

            let (mut results, mut errs) =
                res.extract::<(IndexMap<usize, Bound<PyAny>>, IndexMap<usize, String>)>()?;
            if errs.is_empty() {
                let results = results.into_values().collect();
                let result_series =
                    PySeries::from_pylist_impl(name, results, self.return_dtype.clone())?;

                return Ok(result_series.series);
            }
            let mut delay_ms: u64 = 100; // Start with 100 ms
            const MAX_DELAY_MS: u64 = 60000; // Max 60 seconds
            for attempt in 0..=max_retries {
                if errs.is_empty() {
                    break;
                }

                let mut evaluated_args = IndexMap::with_capacity(errs.len());
                for (&i, _) in &errs {
                    let py_args_for_row = args
                        .iter()
                        .map(|s| {
                            let idx = if s.len() == 1 { 0 } else { i };
                            let lit = s.get_lit(idx);
                            lit.into_pyobject(py).map_err(|e| e.into())
                        })
                        .collect::<DaftResult<Vec<_>>>()?;
                    evaluated_args.insert(i, py_args_for_row);
                }

                let res = f.call1((cls_ref, method_ref, args_ref, evaluated_args))?;

                let (new_results, new_errs) =
                    res.extract::<(IndexMap<usize, Bound<PyAny>>, IndexMap<usize, String>)>()?;
                results.extend(new_results);

                errs = new_errs;

                // Update our failure map for next iteration
                if attempt < max_retries {
                    use std::{thread, time::Duration};
                    py.detach(|| {
                        thread::sleep(Duration::from_millis(delay_ms));
                    });
                    // Exponential backoff: multiply by 2, cap at MAX_DELAY_MS
                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                }
            }

            match (errs.is_empty(), self.on_error) {
                (true, _) => {
                    results.sort_keys();

                    let result_series = PySeries::from_pylist_impl(
                        name,
                        results.into_values().collect(),
                        self.return_dtype.clone(),
                    )?;

                    Ok(result_series.series)
                }
                (false, crate::functions::python::OnError::Ignore) => {
                    for (i, _) in errs {
                        use pyo3::IntoPyObjectExt;

                        results.insert(i, py.None().into_bound_py_any(py)?);
                    }
                    results.sort_keys();
                    let result_series = PySeries::from_pylist_impl(
                        name,
                        results.into_values().collect(),
                        self.return_dtype.clone(),
                    )?;

                    Ok(result_series.series)
                }
                (false, crate::functions::python::OnError::Raise) => {
                    let err_msg = errs
                        .into_iter()
                        .map(|(i, e)| format!("\t{}: {}", i, e))
                        .join("\n");
                    Err(DaftError::ComputeError(format!(
                        "Failed to execute Python UDF on some rows\n: {}",
                        err_msg
                    )))
                }
                (false, crate::functions::python::OnError::Log) => {
                    let err_msg = errs
                        .iter()
                        .map(|(i, e)| format!("\t{}: {}", i, e))
                        .join("\n");
                    let err = DaftError::ComputeError(format!(
                        "Failed to execute Python UDF on some rows\n: {}",
                        err_msg
                    ));
                    log::warn!("{}", err);

                    for (i, _) in errs {
                        use pyo3::IntoPyObjectExt;

                        results.insert(i, py.None().into_bound_py_any(py)?);
                    }
                    results.sort_keys();
                    let result_series = PySeries::from_pylist_impl(
                        name,
                        results.into_values().collect(),
                        self.return_dtype.clone(),
                    )?;

                    Ok(result_series.series)
                }
            }
        })
    }

    #[cfg(feature = "python")]
    fn call_serial(&self, args: &[Series], num_rows: usize) -> DaftResult<Series> {
        use common_error::DaftError;
        use indexmap::IndexMap;
        use pyo3::prelude::*;

        let cls_ref = self.cls.as_ref();
        let method_ref = self.method.as_ref();
        let args_ref = self.original_args.as_ref();

        let name = args[0].name();

        Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.execution"))?
                .getattr(pyo3::intern!(py, "call_func"))?;

            // pre-allocating py_args vector so we're not creating a new vector for each iteration
            let mut py_args = Vec::with_capacity(args.len());
            let mut outputs = IndexMap::new();
            let mut errs = IndexMap::new();
            for i in 0..num_rows {
                for s in args {
                    let idx = if s.len() == 1 { 0 } else { i };
                    let lit = s.get_lit(idx);
                    let pyarg = lit.into_pyobject(py)?;
                    py_args.push(pyarg);
                }

                let result = func.call1((cls_ref, method_ref, args_ref, &py_args));
                match result {
                    Ok(res) => {
                        let lit = Literal::from_pyobj(&res, None)?;
                        outputs.insert(i, lit);
                    }
                    Err(e) => {
                        let err = DaftError::from(e);
                        errs.insert(i, err);
                    }
                }
                py_args.clear();
            }
            let max_retries = self.max_retries.unwrap_or(0);
            let mut delay_ms: u64 = 100; // Start with 100 ms
            const MAX_DELAY_MS: u64 = 60000; // Max 60 seconds

            for attempt in 0..max_retries {
                if errs.is_empty() {
                    break;
                }
                let mut still_failed = IndexMap::new();

                for (&idx, _) in &errs {
                    let mut py_args = Vec::new();
                    for s in args {
                        let arg_idx = if s.len() == 1 { 0 } else { idx };
                        let lit = s.get_lit(arg_idx);
                        let pyarg = lit.into_pyobject(py)?;
                        py_args.push(pyarg);
                    }

                    match func.call1((cls_ref, method_ref, args_ref, &py_args)) {
                        Ok(result) => {
                            // Success. insert the value in the outputs indexmap
                            let new_value = Literal::from_pyobj(&result, None)?;
                            outputs.insert(idx, new_value);
                        }
                        Err(e) => {
                            still_failed.insert(idx, DaftError::from(e));
                            // If this is not the last attempt, sleep before retrying
                        }
                    }
                }

                // Update our failure map for next iteration
                errs = still_failed;
                if attempt < max_retries {
                    use std::{thread, time::Duration};
                    py.detach(|| {
                        thread::sleep(Duration::from_millis(delay_ms));
                    });
                    // Exponential backoff: multiply by 2, cap at MAX_DELAY_MS
                    delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                }
            }
            // free up the gil while we're creating the series.
            py.detach(|| {
                if errs.is_empty() {
                    let s = Series::from_literals(outputs.into_values().collect())?;
                    let s = s.cast(&self.return_dtype)?;
                    let s = s.rename(name);
                    Ok(s)
                } else {
                    match self.on_error {
                        crate::functions::python::OnError::Raise => {
                            let err_msg = errs
                                .into_iter()
                                .map(|(i, e)| format!("\t{}: {}", i, e))
                                .join("\n");
                            Err(DaftError::ComputeError(format!(
                                "Failed to execute Python UDF on some rows\n: {}",
                                err_msg
                            )))
                        }
                        crate::functions::python::OnError::Log => {
                            let err_msg = errs
                                .iter()
                                .map(|(i, e)| format!("\t{}: {}", i, e))
                                .join("\n");
                            let err = DaftError::ComputeError(format!(
                                "Failed to execute Python UDF on some rows\n: {}",
                                err_msg
                            ));
                            log::warn!("{}", err);
                            let mut result_literals = vec![Literal::Null; num_rows];
                            for (idx, lit) in outputs {
                                result_literals[idx] = lit;
                            }
                            let s = Series::from_literals(result_literals)?;
                            let s = s.cast(&self.return_dtype)?;
                            let s = s.rename(name);
                            Ok(s)
                        }
                        crate::functions::python::OnError::Ignore => {
                            let mut result_literals = vec![Literal::Null; num_rows];
                            for (idx, lit) in outputs {
                                result_literals[idx] = lit;
                            }
                            let s = Series::from_literals(result_literals)?;
                            let s = s.cast(&self.return_dtype)?;
                            let s = s.rename(name);
                            Ok(s)
                        }
                    }
                }
            })
        })
    }
}
