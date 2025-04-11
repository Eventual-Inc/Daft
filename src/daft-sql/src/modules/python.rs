use common_error::DaftResult;
use daft_dsl::{functions::python::WrappedUDFClass, Expr};
#[cfg(feature = "python")]
use {
    common_error::DaftError,
    daft_dsl::python::PyExpr,
    pyo3::{
        prelude::*,
        types::{PyDict, PyTuple},
        IntoPyObjectExt,
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
fn lit_to_py_any(py: Python, lit: &daft_dsl::LiteralValue) -> PyResult<PyObject> {
    match lit {
        daft_dsl::LiteralValue::Null => Ok(py.None()),
        daft_dsl::LiteralValue::Boolean(b) => b.into_py_any(py),
        daft_dsl::LiteralValue::Utf8(s) => s.into_py_any(py),
        daft_dsl::LiteralValue::Binary(items) => items.into_py_any(py),
        daft_dsl::LiteralValue::Int8(i) => i.into_py_any(py),
        daft_dsl::LiteralValue::UInt8(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::Int16(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::UInt16(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::Int32(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::UInt32(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::Int64(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::UInt64(u) => u.into_py_any(py),
        daft_dsl::LiteralValue::Timestamp(..) => todo!(),
        daft_dsl::LiteralValue::Date(_) => todo!(),
        daft_dsl::LiteralValue::Time(..) => todo!(),
        daft_dsl::LiteralValue::Duration(..) => todo!(),
        daft_dsl::LiteralValue::Interval(..) => todo!(),
        daft_dsl::LiteralValue::Float64(..) => todo!(),
        daft_dsl::LiteralValue::Decimal(..) => todo!(),
        daft_dsl::LiteralValue::Series(series) => daft_core::python::PySeries {
            series: series.clone(),
        }
        .into_py_any(py),
        daft_dsl::LiteralValue::Python(_) => todo!(),
        daft_dsl::LiteralValue::Struct(_) => todo!(),
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
                        // all named (kwargs are parsed as python literals instead of exprs)
                        sqlparser::ast::FunctionArg::Named {
                            name,
                            arg,
                            operator: _,
                        } => {
                            let expr = planner.try_unwrap_function_arg_expr(arg)?;
                            if let Expr::Literal(lit) = expr.as_ref() {
                                let pyany = lit_to_py_any(py, lit)?;
                                kwargs.set_item(name.to_string(), pyany)?;
                            } else {
                                return Err(DaftError::InternalError(
                                    "expected a literal, found an expression".to_string(),
                                ));
                            }
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
