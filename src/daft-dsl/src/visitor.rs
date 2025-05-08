use daft_core::{
    prelude::DataType,
    python::{PyDataType, PySeries},
    utils::display::display_decimal128,
};
use pyo3::{
    exceptions::PyValueError, types::{PyAnyMethods, PyBool, PyDict, PyList, PyListMethods, PyModule}, Bound, IntoPyObject, IntoPyObjectExt, PyAny, PyResult, Python
};

use crate::{
    functions::{FunctionExpr, ScalarFunction}, python::PyExpr, AggExpr, Column, Expr, ExprRef, LiteralValue, Operator, Subquery, WindowExpr, WindowSpec
};

/// Helper to downcast some specific pyo3 type to a PyAny.
macro_rules! into_pyany {
    ($value:expr, $py:expr) => {{
        let obj = $value.into_pyobject_or_pyerr($py)?;
        let any = obj.into_any();
        Ok(any)
    }};
}

/// The generic `R` of the py visitor implementation.
type PyVisitorResult<'py> = PyResult<Bound<'py, PyAny>>;

/// We switch on the rust enum to figure out which py visitor method to call unlike a traditional accept.
pub fn accept<'py>(expr: &PyExpr, visitor: Bound<'py, PyAny>) -> PyVisitorResult<'py> {
    let visitor = PyVisitor::new(visitor)?;
    match expr.expr.as_ref() {
        Expr::Column(column) => visitor.visit_column(column),
        Expr::Alias(expr, alias) => visitor.visit_alias(expr, alias.to_string()),
        Expr::Agg(agg_expr) => visitor.visit_agg(agg_expr),
        Expr::BinaryOp { op, left, right } => visitor.visit_binary_op(op, left, right),
        Expr::Cast(expr, data_type) => visitor.visit_cast(expr, data_type),
        Expr::Function { func, inputs } => visitor.visit_function(func, inputs),
        Expr::Over(window_expr, window_spec) => visitor.visit_over(window_expr, window_spec),
        Expr::WindowFunction(window_expr) => visitor.visit_window_function(window_expr),
        Expr::Not(expr) => visitor.visit_not(expr),
        Expr::IsNull(expr) => visitor.visit_is_null(expr),
        Expr::NotNull(expr) => visitor.visit_not_null(expr),
        Expr::FillNull(expr, expr1) => visitor.visit_fill_null(expr, expr1),
        Expr::IsIn(expr, exprs) => visitor.visit_is_in(expr, exprs),
        Expr::Between(expr, expr1, expr2) => visitor.visit_between(expr, expr1, expr2),
        Expr::List(exprs) => visitor.visit_list(exprs),
        Expr::Literal(literal_value) => visitor.visit_literal(literal_value),
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => visitor.visit_if_else(if_true, if_false, predicate),
        Expr::ScalarFunction(scalar_function) => visitor.visit_scalar_function(scalar_function),
        Expr::Subquery(subquery) => visitor.visit_subquery(subquery),
        Expr::InSubquery(expr, subquery) => visitor.visit_in_subquery(expr, subquery),
        Expr::Exists(subquery) => visitor.visit_exists(subquery),
    }
}

/// PyVisitor is a short-lived wrapper which holds the GIL to simplify py object building.
pub(crate) struct PyVisitor<'py> {
    py: Python<'py>,
    visitor: Bound<'py, PyAny>,
}


impl<'py> PyVisitor<'py> {

    fn new(visitor: Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self { py: visitor.py(), visitor })
    }

    fn visit_column(&self, column: &Column) -> PyVisitorResult<'py> {
        let meth = "visit_col";
        let args = (column.name(),);
        self.visitor.call_method1(meth, args)
    }

    fn visit_alias(&self, expr: &ExprRef, alias: String) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support alias expressions",
        ))
    }

    fn visit_agg(&self, agg_expr: &AggExpr) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support aggregate expressions",
        ))
    }

    fn visit_binary_op(
        self,
        op: &Operator,
        left: &ExprRef,
        right: &ExprRef,
    ) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support binary operations",
        ))
    }

    fn visit_cast(&self, expr: &ExprRef, data_type: &DataType) -> PyVisitorResult<'py> {
        let meth = "visit_cast";
        let args = (
            self.to_expr(expr)?,
            self.to_data_type(data_type)?,
        );
        self.visitor.call_method1(meth, args)
    }

    fn visit_function(&self, func: &FunctionExpr, inputs: &[ExprRef]) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support function expressions",
        ))
    }

    fn visit_over(
        self,
        window_expr: &WindowExpr,
        window_spec: &WindowSpec,
    ) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not yet support window function expressions",
        ))
    }

    #[allow(unused_attributes)]
    fn visit_window_function(&self, window_expr: &WindowExpr) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not yet support window function expressions",
        ))
    }

    fn visit_not(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let meth = "visit_not";
        let args = (self.to_expr(expr)?,);
        self.visitor.call_method1(meth, args)
    }

    fn visit_is_null(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let meth = "visit_is_null";
        let args = (self.to_expr(expr)?,);
        self.visitor.call_method1(meth, args)
    }

    fn visit_not_null(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let meth = "visit_not_null";
        let args = (self.to_expr(expr)?,);
        self.visitor.call_method1(meth, args)
    }

    fn visit_fill_null(&self, expr: &ExprRef, expr1: &ExprRef) -> PyVisitorResult<'py> {
        let meth = "visit_fill_null";
        let args = (self.to_expr(expr)?, self.to_expr(expr1)?);
        self.visitor.call_method1(meth, args)
    }

    fn visit_is_in(&self, expr: &ExprRef, exprs: &[ExprRef]) -> PyVisitorResult<'py> {
        let meth = "visit_is_in";
        let mut args_vec = vec![self.to_expr(expr)?];
        for expr in exprs {
            args_vec.push(self.to_expr(expr)?);
        }
        let args = (args_vec,);
        self.visitor.call_method1(meth, args)
    }

    fn visit_between(
        self,
        expr: &ExprRef,
        expr1: &ExprRef,
        expr2: &ExprRef,
    ) -> PyVisitorResult<'py> {
        let meth = "visit_between";
        let args = (self.to_expr(expr)?, self.to_expr(expr1)?, self.to_expr(expr2)?);
        self.visitor.call_method1(meth, args)
    }

    fn visit_list(&self, exprs: &[ExprRef]) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support list expressions",
        ))
    }

    fn visit_literal(&self, literal_value: &LiteralValue) -> PyVisitorResult<'py> {
        let meth = "visit_lit";
        let args = (self.to_lit(literal_value)?,);
        self.visitor.call_method1(meth, args)
    }

    fn visit_if_else(
        self,
        if_true: &ExprRef,
        if_false: &ExprRef,
        predicate: &ExprRef,
    ) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support if_else expressions",
        ))
    }

    fn visit_scalar_function(&self, scalar_function: &ScalarFunction) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support scalar function expressions",
        ))
    }

    fn visit_subquery(&self, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support subquery expressions",
        ))
    }

    fn visit_in_subquery(&self, expr: &ExprRef, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support in_subquery expressions",
        ))
    }

    fn visit_exists(&self, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support exists expressions",
        ))
    }

    // Developer Note
    // ==============
    // These *could* be factored out as IntoPyObject implementations,
    // but there are few caveats, and I wish to keep these conversions
    // scoped to interactions with the visitor for now. For types like
    // Expr and DataType we cannot implement the trait on the external
    // type. We can implement IntoPyObject for LiteralValue, but again
    // I wish to have these conversions limited in scope and usage.
    // We don't want to implement IntoPyObject for PyExpr etc. since
    // these types are already py objects, just different than the
    // conversions we want here.

    /// Converts an ExprRef (rs) to an Expression (py)
    fn to_expr(&self, expr: &ExprRef) -> PyResult<Bound<'py, PyAny>> {
        let expr_mod = self.py.import("daft.expressions.expressions")?;
        let expr_cls = expr_mod.getattr("Expression")?;
        expr_cls.call_method1("_from_pyexpr", (PyExpr { expr: expr.clone() },))
    }

    /// Converts a DataType (rs) to DataType (py)
    fn to_data_type(&self, data_type: &DataType) -> PyResult<Bound<'py, PyAny>> {
        let dt_mod = self.py.import("daft.datatype")?;
        let dt_cls = dt_mod.getattr("DataType")?;
        dt_cls.call_method1(
            "_from_pydatatype",
            (PyDataType {
                dtype: data_type.clone(),
            },),
        )
    }

    /// Converts a LiteralValue (rs) to an object (py).
    fn to_lit(&self, lit: &LiteralValue) -> PyResult<Bound<'py, PyAny>> {
        let py = self.py;
        match lit {
            LiteralValue::Null => into_pyany!(py.None(), py),
            LiteralValue::Boolean(b) => {
                let obj = PyBool::new(py, *b).to_owned();
                let any = obj.into_any();
                Ok(any)
            }
            LiteralValue::Utf8(s) => into_pyany!(s, py),
            LiteralValue::Int8(i) => into_pyany!(i, py),
            LiteralValue::UInt8(i) => into_pyany!(i, py),
            LiteralValue::Int16(i) => into_pyany!(i, py),
            LiteralValue::UInt16(i) => into_pyany!(i, py),
            LiteralValue::Int32(i) => into_pyany!(i, py),
            LiteralValue::UInt32(i) => into_pyany!(i, py),
            LiteralValue::Int64(i) => into_pyany!(i, py),
            LiteralValue::UInt64(i) => into_pyany!(i, py),
            LiteralValue::Binary(b) => into_pyany!(b.as_slice(), py),
            LiteralValue::Float64(f) => into_pyany!(f, py),
            LiteralValue::Decimal(value, precision, scale) => {
                let decimal = display_decimal128(*value, *precision, *scale);
                let decimal = py
                    .import("decimal")?
                    .getattr("Decimal")?
                    .call1((decimal,))?;
                Ok(decimal)
            }
            LiteralValue::Series(series) => {
                let series = PySeries {
                    series: series.clone(),
                };
                let series = py
                    .import("daft.series")?
                    .getattr("Series")?
                    .getattr("_from_pyseries")?
                    .call1((series,))?;
                Ok(series)
            }
            LiteralValue::Python(obj) => {
                let any = obj.0.clone_ref(py);
                let any = any.bind(py).to_owned();
                Ok(any)
            }
            LiteralValue::Struct(entries) => {
                let dict = PyDict::new(py);
                for (key, value) in entries {
                    dict.set_item(&key.name, self.to_lit(value)?)?;
                }
                Ok(dict.into_any())
            }
            _ => Err(PyValueError::new_err(format!(
                "Cannot convert literal to python object, `{}`",
                lit,
            ))),
        }
    }
}
