use common_error::DaftResult;
use daft_dsl::{Expr, functions::python::WrappedUDFClass};
#[cfg(feature = "python")]
use {
    common_error::DaftError,
    daft_dsl::python::PyExpr,
    pyo3::{
        IntoPyObjectExt,
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

#[cfg(feature = "python")]
fn lit_to_py_any(py: Python, expr: &daft_dsl::Expr) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    match expr {
        Expr::List(exprs) => {
            let literals = exprs
                .iter()
                .map(|e| lit_to_py_any(py, e))
                .collect::<PyResult<Vec<pyo3::Py<pyo3::PyAny>>>>()?;
            literals.into_py_any(py)
        }
        Expr::Literal(lit) => lit.clone().into_py_any(py),
        _ => Err(
            DaftError::InternalError("expected a literal, found an expression".to_string()).into(),
        ),
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
            let e: DaftResult<PyExpr> = pyo3::Python::attach(|py| {
                let mut args = Vec::with_capacity(inputs.len());
                let kwargs = PyDict::new(py);

                for input in inputs {
                    match input {
                        // all named args (kwargs) are parsed as python literals instead of exprs
                        // This is because in sql there's no way to properly distinguish between a `lit` value and a literal value.
                        // In python you can do `daft.lit(1)` and `1` but in sql there is no `lit`.
                        // So we need some way to distinguish between python literals of `1` and a `lit` of `1`.
                        // So all named exprs are parsed as literals such as `1` and all positional are parsed as exprs `lit(1)`.
                        // TODO: consider making udfs take in only exprs so we don't need to handle this in the sql parser.
                        sqlparser::ast::FunctionArg::Named {
                            name,
                            arg,
                            operator: _,
                        } => {
                            let expr = planner.try_unwrap_function_arg_expr(arg)?;

                            let pyany = lit_to_py_any(py, expr.as_ref())?;
                            kwargs.set_item(name.to_string(), pyany)?;
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
