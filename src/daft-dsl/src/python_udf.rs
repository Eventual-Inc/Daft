use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    functions::{python::RuntimePyObject, scalar::ScalarFn},
    Expr, ExprRef, LiteralValue,
};

#[derive(derive_more::Display, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[display("{_0}")]
pub enum PyScalarFn {
    RowWise(RowWisePyFn),
}

impl PyScalarFn {
    pub fn call(&self, args: Vec<Series>) -> DaftResult<Series> {
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
    pub fn call(&self, _args: Vec<Series>) -> DaftResult<Series> {
        panic!("Cannot evaluate a RowWisePyFn without compiling for Python");
    }

    #[cfg(feature = "python")]
    pub fn call(&self, args: Vec<Series>) -> DaftResult<Series> {
        use daft_core::python::{PyDataType, PySeries};
        use pyo3::prelude::*;
        use rayon::{iter::ParallelIterator, slice::ParallelSlice};

        let num_rows = args
            .iter()
            .map(Series::len)
            .max()
            .expect("RowWisePyFn should have at least one argument");

        for a in &args {
            assert!(
                a.len() == num_rows || a.len() == 1,
                "arg lengths differ: {} vs {}",
                num_rows,
                a.len()
            );
        }

        let py_return_type = PyDataType::from(self.return_dtype.clone());

        let call_func_with_evaluated_exprs = pyo3::Python::with_gil(|py| {
            Ok::<_, PyErr>(
                py.import(pyo3::intern!(py, "daft.udf.row_wise"))?
                    .getattr(pyo3::intern!(py, "call_func_with_evaluated_exprs"))?
                    .unbind(),
            )
        })?;

        // To minimize gil contention, while also allowing parallelism, we chunk up the rows
        // for now,its just based on the max of (512) and (num rows / (number of CPUs * 4))
        // This may need additional tuning based on usage patterns
        //
        // Instead of running sequentially and acquiring the gil for each row, we instead parallelize based off the chunk size.
        // Each chunk then acquires the gil.
        // Since we're processing data in chunks, there's less thrashing of the gil than if we were to use `.par_iter().map(|row| {Python::with_gil(..)})`
        let n_cpus =
            std::thread::available_parallelism().expect("Failed to get available parallelism");

        let chunk_size = (num_rows / (n_cpus.get() * 4)).clamp(1, 512);

        let indices: Vec<usize> = (0..num_rows).collect();
        let outputs = indices
            .par_chunks(chunk_size)
            .map(|chunk| {
                Python::with_gil(|py| {
                    chunk
                        .iter()
                        .map(|&i| {
                            let args_for_row = args
                                .iter()
                                .map(|a| {
                                    let idx = if a.len() == 1 { 0 } else { i };
                                    LiteralValue::get_from_series(a, idx)
                                })
                                .collect::<DaftResult<Vec<_>>>()?;

                            let py_args = args_for_row
                                .into_iter()
                                .map(|a| a.into_pyobject(py))
                                .collect::<PyResult<Vec<_>>>()?;

                            let result = call_func_with_evaluated_exprs.bind(py).call1((
                                self.inner.clone().unwrap().as_ref(),
                                py_return_type.clone(),
                                self.original_args.clone().unwrap().as_ref(),
                                py_args,
                            ))?;

                            let result_series = result.extract::<PySeries>()?.series;
                            Ok(result_series)
                        })
                        .collect::<DaftResult<Vec<Series>>>()
                })
            })
            .collect::<DaftResult<Vec<Vec<Series>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let outputs_ref = outputs.iter().collect::<Vec<_>>();

        let name = args[0].name();

        Ok(Series::concat(&outputs_ref)?.rename(name))
    }
}
