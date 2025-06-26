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
            daft_dsl::LiteralValue::Null => Ok(py.None()),
            daft_dsl::LiteralValue::Boolean(b) => b.into_py_any(py),
            daft_dsl::LiteralValue::Utf8(s) => s.into_py_any(py),
            daft_dsl::LiteralValue::Binary(_) => unreachable_variant!(Binary),
            daft_dsl::LiteralValue::FixedSizeBinary(_, _) => unreachable_variant!(FixedSizeBinary),
            daft_dsl::LiteralValue::Int8(_) => unreachable_variant!(Int8),
            daft_dsl::LiteralValue::UInt8(_) => unreachable_variant!(UInt8),
            daft_dsl::LiteralValue::Int16(_) => unreachable_variant!(Int16),
            daft_dsl::LiteralValue::UInt16(_) => unreachable_variant!(UInt16),
            daft_dsl::LiteralValue::Int32(_) => unreachable_variant!(Int32),
            daft_dsl::LiteralValue::UInt32(_) => unreachable_variant!(UInt32),
            daft_dsl::LiteralValue::Int64(u) => u.into_py_any(py),
            daft_dsl::LiteralValue::UInt64(_) => unreachable_variant!(UInt64),
            daft_dsl::LiteralValue::Timestamp(..) => unreachable_variant!(Timestamp),
            daft_dsl::LiteralValue::Date(_) => unreachable_variant!(Date),
            daft_dsl::LiteralValue::Time(..) => unreachable_variant!(Time),
            daft_dsl::LiteralValue::Duration(..) => unreachable_variant!(Duration),
            daft_dsl::LiteralValue::Interval(..) => todo!(),
            daft_dsl::LiteralValue::Float64(f) => f.into_py_any(py),
            daft_dsl::LiteralValue::Decimal(..) => unreachable_variant!(Decimal),
            daft_dsl::LiteralValue::Series(series) => daft_core::python::PySeries {
                series: series.clone(),
            }
            .into_py_any(py),
            daft_dsl::LiteralValue::Python(_) => unreachable_variant!(Python),
            daft_dsl::LiteralValue::Struct(_) => todo!(),
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
