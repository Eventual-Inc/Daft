use common_error::DaftResult;
use daft_core::lit::Literal;
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
fn lit_to_py_any(py: Python, expr: &daft_dsl::Expr) -> PyResult<PyObject> {
    macro_rules! unreachable_variant {
        ($variant:ident) => {
            unreachable!("{} can't be created directly in SQL", stringify!($variant))
        };
    }

    match expr {
        Expr::List(exprs) => {
            let literals = exprs
                .iter()
                .map(|e| lit_to_py_any(py, e))
                .collect::<PyResult<Vec<PyObject>>>()?;
            literals.into_py_any(py)
        }
        Expr::Literal(lit) => match lit {
            Literal::Null => Ok(py.None()),
            Literal::Boolean(b) => b.into_py_any(py),
            Literal::Utf8(s) => s.into_py_any(py),
            Literal::Binary(_) => unreachable_variant!(Binary),
            Literal::Int8(_) => unreachable_variant!(Int8),
            Literal::UInt8(_) => unreachable_variant!(UInt8),
            Literal::Int16(_) => unreachable_variant!(Int16),
            Literal::UInt16(_) => unreachable_variant!(UInt16),
            Literal::Int32(_) => unreachable_variant!(Int32),
            Literal::UInt32(_) => unreachable_variant!(UInt32),
            Literal::Int64(u) => u.into_py_any(py),
            Literal::UInt64(_) => unreachable_variant!(UInt64),
            Literal::Timestamp(..) => unreachable_variant!(Timestamp),
            Literal::Date(_) => unreachable_variant!(Date),
            Literal::Time(..) => unreachable_variant!(Time),
            Literal::Duration(..) => unreachable_variant!(Duration),
            Literal::Interval(..) => todo!(),
            Literal::Float32(f) => f.into_py_any(py),
            Literal::Float64(f) => f.into_py_any(py),
            Literal::Decimal(..) => unreachable_variant!(Decimal),
            Literal::List(series) => daft_core::python::PySeries {
                series: series.clone(),
            }
            .into_py_any(py),
            Literal::Python(_) => unreachable_variant!(Python),
            Literal::Struct(_) => todo!(),
            Literal::Tensor { .. } => todo!(),
            Literal::SparseTensor { .. } => todo!(),
            Literal::Embedding { .. } => todo!(),
            Literal::Map { .. } => todo!(),
            Literal::Image(_) => todo!(),
        },
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
            let e: DaftResult<PyExpr> = pyo3::Python::with_gil(|py| {
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
