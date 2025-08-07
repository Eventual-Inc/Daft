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
        use std::time::Instant;

        use daft_core::python::PySeries;
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

        let call_func_with_evaluated_exprs = pyo3::Python::with_gil(|py| {
            Ok::<_, PyErr>(
                py.import(pyo3::intern!(py, "daft.udf.row_wise"))?
                    .getattr(pyo3::intern!(py, "call_func_with_evaluated_exprs"))?
                    .unbind(),
            )
        })?;

        let inner_ref = self.inner.as_ref();
        let args_ref = self.original_args.as_ref();
        let name = args[0].name();
        let start_time = Instant::now();

        let (outputs, gil_contention_time) = Python::with_gil(|py| {
            let gil_contention_time = start_time.elapsed();

            let mut py_args = Vec::with_capacity(args.len());
            // pre-allocating py_args vector so we're not creating a new vector for each iteration
            let outputs = (0..num_rows)
                .map(|i| {
                    for s in args {
                        let idx = if s.len() == 1 { 0 } else { i };
                        let lit = LiteralValue::get_from_series(s, idx)?;
                        let pyarg = lit.into_pyobject(py)?;
                        py_args.push(pyarg);
                    }

                    let result = call_func_with_evaluated_exprs
                        .bind(py)
                        .call1((inner_ref, args_ref, &py_args))?;
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
