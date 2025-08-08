use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    Expr, ExprRef,
};

#[derive(derive_more::Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[display("{_0}")]
pub enum PyScalarFn {
    RowWise(RowWisePyFn),
}

impl PyScalarFn {
    pub fn name(&self) -> &str {
        match self {
            Self::RowWise(RowWisePyFn { function_name, .. }) => function_name,
        }
    }
    pub fn call(&self, args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        match self {
            Self::RowWise(func) => func.call(args),
        }
    }

    pub fn args(&self) -> Vec<ExprRef> {
        match self {
            Self::RowWise(RowWisePyFn { args, .. }) => args.clone(),
        }
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        match self {
            Self::RowWise(RowWisePyFn {
                function_name: name,
                args,
                return_dtype,
                ..
            }) => {
                let field_name = if let Some(first_child) = args.first() {
                    first_child.get_name(schema)?
                } else {
                    name.to_string()
                };

                Ok(Field::new(field_name, return_dtype.clone()))
            }
        }
    }
}

pub fn row_wise_udf(
    name: &str,
    inner: RuntimePyObject,
    return_dtype: DataType,
    original_args: RuntimePyObject,
    args: Vec<ExprRef>,
) -> Expr {
    Expr::ScalarFn(ScalarFn::Python(PyScalarFn::RowWise(RowWisePyFn {
        function_name: Arc::from(name),
        inner,
        return_dtype,
        original_args,
        args,
    })))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowWisePyFn {
    pub function_name: Arc<str>,
    pub inner: RuntimePyObject,
    pub return_dtype: DataType,
    pub original_args: RuntimePyObject,
    pub args: Vec<ExprRef>,
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
            inner: self.inner.clone(),
            return_dtype: self.return_dtype.clone(),
            original_args: self.original_args.clone(),
            args: children,
        }
    }

    #[cfg(not(feature = "python"))]
    pub fn call(&self, _args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: &[Series]) -> DaftResult<(Series, std::time::Duration)> {
        use pyo3::prelude::*;

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

        let is_async: bool = Python::with_gil(|py| {
            py.import(pyo3::intern!(py, "asyncio"))?
                .getattr(pyo3::intern!(py, "iscoroutinefunction"))?
                .call1((self.inner.as_ref(),))?
                .extract()
        })?;

        if is_async {
            self.call_async(args, num_rows)
        } else {
            self.call_serial(args, num_rows)
        }
    }

    #[cfg(feature = "python")]
    fn call_async(
        &self,
        args: &[Series],
        num_rows: usize,
    ) -> DaftResult<(Series, std::time::Duration)> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;
        let py_return_type = daft_core::python::PyDataType::from(self.return_dtype.clone());
        let inner_ref = self.inner.as_ref();
        let args_ref = self.original_args.as_ref();
        let start_time = std::time::Instant::now();

        Ok(pyo3::Python::with_gil(|py| {
            let gil_contention_time = start_time.elapsed();

            let f = py
                .import(pyo3::intern!(py, "daft.udf.row_wise"))?
                .getattr(pyo3::intern!(py, "__call_async_batch"))?;

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

            let res = f.call1((inner_ref, py_return_type.clone(), args_ref, evaluated_args))?;
            let name = args[0].name();

            let result_series = res.extract::<PySeries>()?.series;

            Ok::<_, PyErr>((result_series.rename(name), gil_contention_time))
        })?)
    }

    #[cfg(feature = "python")]
    fn call_serial(
        &self,
        args: &[Series],
        num_rows: usize,
    ) -> DaftResult<(Series, std::time::Duration)> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        let inner_ref = self.inner.as_ref();
        let args_ref = self.original_args.as_ref();
        let name = args[0].name();
        let start_time = std::time::Instant::now();
        let (outputs, gil_contention_time) = Python::with_gil(|py| {
            let gil_contention_time = start_time.elapsed();
            let func = py
                .import(pyo3::intern!(py, "daft.udf.row_wise"))?
                .getattr(pyo3::intern!(py, "__call_func"))?;

            let mut py_args = Vec::with_capacity(args.len());
            // pre-allocating py_args vector so we're not creating a new vector for each iteration
            let outputs = (0..num_rows)
                .map(|i| {
                    for s in args {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = s.get_lit(idx);
                        let pyarg = lit.into_pyobject(py)?;
                        py_args.push(pyarg);
                    }

                    let result = func.call1((inner_ref, args_ref, &py_args))?;
                    py_args.clear();
                    DaftResult::Ok(result.unbind())
                })
                .collect::<DaftResult<Vec<_>>>()?;
            DaftResult::Ok((outputs, gil_contention_time))
        })?;

        Ok((
            PySeries::from_pylist_impl(name, outputs, self.return_dtype.clone())?.series,
            gil_contention_time,
        ))
    }
}
