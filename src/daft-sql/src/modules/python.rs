use common_error::DaftResult;
use daft_dsl::functions::python::WrappedUDFClass;
#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{
        prelude::*,
        types::{PyDict, PyTuple},
    },
};

use super::SQLModule;
use crate::functions::{SQLFunction, SQLFunctions};
#[cfg(not(feature = "python"))]
use crate::unsupported_sql_err;

pub struct SQLModulePython;

impl SQLModule for SQLModulePython {
    fn register(_parent: &mut SQLFunctions) {
        // use FunctionExpr::Python as f;
        // TODO
    }
}

impl SQLFunction for WrappedUDFClass {
    fn to_expr(
        &self,
        inputs: &[sqlparser::ast::FunctionArg],
        planner: &crate::SQLPlanner,
    ) -> crate::error::SQLPlannerResult<daft_dsl::ExprRef> {
        #[cfg(feature = "python")]
        {
            let e: DaftResult<PyExpr> = pyo3::Python::with_gil(|py| {
                let mut args = Vec::with_capacity(inputs.len());
                let kwargs = PyDict::new(py);

                for input in inputs {
                    match input {
                        sqlparser::ast::FunctionArg::Named {
                            name,
                            arg,
                            operator: _,
                        } => {
                            let expr = planner.try_unwrap_function_arg_expr(arg)?;

                            let py_expr = PyExpr { expr };
                            let pyobj = py_expr.into_expr_cls(py)?;

                            kwargs.set_item(name.to_string(), pyobj)?;
                        }
                        sqlparser::ast::FunctionArg::Unnamed(arg) => {
                            let expr = planner.try_unwrap_function_arg_expr(arg)?;

                            let py_expr = PyExpr { expr };
                            args.push(py_expr.into_expr_cls(py)?);
                        }
                    }
                }
                let args = PyTuple::new(py, &args)?;

                Ok(self.call(py, args, Some(&kwargs))?)
            });
            let e = e?;

            Ok(e.expr)
        }

        #[cfg(not(feature = "python"))]
        {
            unsupported_sql_err!("Python UDFs")
        }
    }
}
