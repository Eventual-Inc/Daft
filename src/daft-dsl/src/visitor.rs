use daft_core::{lit::Literal, prelude::DataType, python::PyDataType};
use pyo3::{
    Bound, PyAny, PyResult, Python,
    exceptions::PyValueError,
    types::{PyAnyMethods, PyList, PyListMethods},
};

use crate::{
    AggExpr, Column, Expr, ExprRef, Operator, Subquery, WindowExpr, WindowSpec,
    functions::{BuiltinScalarFn, FunctionExpr, scalar::ScalarFn},
    python::PyExpr,
    python_udf::{BatchPyFn, PyScalarFn, RowWisePyFn},
};

/// The generic `R` of the py visitor implementation.
type PyVisitorResult<'py> = PyResult<Bound<'py, PyAny>>;

/// We switch on the rust enum to figure out which py visitor method to call unlike a traditional accept.
pub fn accept<'py>(expr: &PyExpr, visitor: Bound<'py, PyAny>) -> PyVisitorResult<'py> {
    let visitor = PyVisitor::new(visitor)?;
    match expr.expr.as_ref() {
        Expr::Column(column) => visitor.visit_col(column),
        Expr::Alias(expr, alias) => visitor.visit_alias(expr, alias.to_string()),
        Expr::Agg(agg_expr) => visitor.visit_agg(agg_expr),
        Expr::BinaryOp { op, left, right } => visitor.visit_binary_op(op, left, right),
        Expr::Cast(expr, data_type) => visitor.visit_cast(expr, data_type),
        Expr::Function { func, inputs } => visitor.visit_function_expr(func, inputs),
        Expr::Over(window_expr, window_spec) => visitor.visit_over(window_expr, window_spec),
        Expr::WindowFunction(window_expr) => visitor.visit_window_function(window_expr),
        Expr::Not(expr) => visitor.visit_not(expr),
        Expr::IsNull(expr) => visitor.visit_is_null(expr),
        Expr::NotNull(expr) => visitor.visit_not_null(expr),
        Expr::FillNull(expr, expr1) => visitor.visit_fill_null(expr, expr1),
        Expr::IsIn(expr, exprs) => visitor.visit_is_in(expr, exprs),
        Expr::Between(expr, expr1, expr2) => visitor.visit_between(expr, expr1, expr2),
        Expr::List(exprs) => visitor.visit_list(exprs),
        Expr::Literal(literal_value) => visitor.visit_lit(literal_value),
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => visitor.visit_if_else(if_true, if_false, predicate),
        Expr::ScalarFn(ScalarFn::Builtin(scalar_function)) => {
            visitor.visit_builtin_scalar_func(scalar_function)
        }
        Expr::ScalarFn(ScalarFn::Python(scalar_function)) => {
            visitor.visit_python_scalar_func(scalar_function)
        }
        Expr::Subquery(subquery) => visitor.visit_subquery(subquery),
        Expr::InSubquery(expr, subquery) => visitor.visit_in_subquery(expr, subquery),
        Expr::Exists(subquery) => visitor.visit_exists(subquery),
        Expr::VLLM(..) => todo!(),
    }
}

/// PyVisitor is a short-lived wrapper which holds the GIL to simplify py object building.
pub(crate) struct PyVisitor<'py> {
    py: Python<'py>,
    visitor: Bound<'py, PyAny>,
}

impl<'py> PyVisitor<'py> {
    fn new(visitor: Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self {
            py: visitor.py(),
            visitor,
        })
    }

    fn visit_col(&self, col: &Column) -> PyVisitorResult<'py> {
        let attr = "visit_col";
        let args = (col.name(),);
        self.visitor.call_method1(attr, args)
    }

    fn visit_lit(&self, lit: &Literal) -> PyVisitorResult<'py> {
        let attr = "visit_lit";
        self.visitor.call_method1(attr, (lit.clone(),))
    }

    fn visit_alias(&self, expr: &ExprRef, alias: String) -> PyVisitorResult<'py> {
        let attr = "visit_alias";
        let args = (self.to_expr(expr)?, alias);
        self.visitor.call_method1(attr, args)
    }

    fn visit_cast(&self, expr: &ExprRef, data_type: &DataType) -> PyVisitorResult<'py> {
        let attr = "visit_cast";
        let args = (self.to_expr(expr)?, self.to_data_type(data_type)?);
        self.visitor.call_method1(attr, args)
    }

    #[allow(unused_variables)]
    fn visit_agg(&self, agg_expr: &AggExpr) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support aggregate expressions",
        ))
    }

    fn visit_function(&self, name: &str, args: Vec<Bound<'py, PyAny>>) -> PyVisitorResult<'py> {
        self.visitor.call_method1("visit_", (name, args))
    }

    fn visit_binary_op(self, op: &Operator, lhs: &ExprRef, rhs: &ExprRef) -> PyVisitorResult<'py> {
        let name = match &op {
            Operator::And => "and",
            Operator::Or => "or",
            Operator::Eq => "equal",
            Operator::NotEq => "not_equal",
            Operator::Lt => "less_than",
            Operator::LtEq => "less_than_or_equal",
            Operator::Gt => "greater_than",
            Operator::GtEq => "greater_than_or_equal",
            Operator::EqNullSafe => "eq_null_safe",
            Operator::Plus => "plus",
            Operator::Minus => "minus",
            Operator::Multiply => "multiply",
            Operator::TrueDivide => "true_divide",
            Operator::FloorDivide => "floor_divide",
            Operator::Modulus => "modulus",
            Operator::Xor => "xor",
            Operator::ShiftLeft => "shift_left",
            Operator::ShiftRight => "shift_right",
        };
        let args = vec![self.to_expr(lhs)?, self.to_expr(rhs)?];
        self.visit_function(name, args)
    }

    fn visit_function_expr(
        &self,
        function: &FunctionExpr,
        inputs: &[ExprRef],
    ) -> PyVisitorResult<'py> {
        use crate::functions::partitioning::PartitioningExpr;

        let mut args: Vec<_> = inputs
            .iter()
            .map(|arg| self.to_expr(arg))
            .collect::<PyResult<_>>()?;

        let name = match function {
            FunctionExpr::Python(python_udf) => &python_udf.name,
            FunctionExpr::Partitioning(partitioning_expr) => match partitioning_expr {
                PartitioningExpr::Years => "years",
                PartitioningExpr::Months => "month",
                PartitioningExpr::Days => "days",
                PartitioningExpr::Hours => "hours",
                PartitioningExpr::IcebergBucket(b) => {
                    let b = Expr::Literal(Literal::Int32(*b)).arced();
                    let b = self.to_expr(&b)?;
                    args.push(b);
                    "iceberg_bucket"
                }
                PartitioningExpr::IcebergTruncate(w) => {
                    let w = Expr::Literal(Literal::Int64(*w)).arced();
                    let w = self.to_expr(&w)?;
                    args.push(w);
                    "iceberg_truncate"
                }
            },
            _ => {
                return Err(PyValueError::new_err(
                    "Visitor does not support function expressions",
                ));
            }
        };

        self.visit_function(name, args)
    }

    fn visit_python_scalar_func(&self, udf: &PyScalarFn) -> PyVisitorResult<'py> {
        match udf {
            PyScalarFn::RowWise(RowWisePyFn {
                function_name: name,
                args: children,
                ..
            })
            | PyScalarFn::Batch(BatchPyFn {
                function_name: name,
                args: children,
                ..
            }) => {
                let args = children
                    .iter()
                    .map(|expr| self.to_expr(expr))
                    .collect::<PyResult<Vec<_>>>()?;

                self.visit_function(name, args)
            }
        }
    }

    #[allow(unused_variables)]
    fn visit_over(
        self,
        window_expr: &WindowExpr,
        window_spec: &WindowSpec,
    ) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not yet support window function expressions",
        ))
    }

    #[allow(unused_variables)]
    fn visit_window_function(&self, window_expr: &WindowExpr) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not yet support window function expressions",
        ))
    }

    fn visit_not(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let name = "not";
        let args = vec![self.to_expr(expr)?];
        self.visit_function(name, args)
    }

    fn visit_is_null(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let name = "is_null";
        let args = vec![self.to_expr(expr)?];
        self.visit_function(name, args)
    }

    fn visit_not_null(&self, expr: &ExprRef) -> PyVisitorResult<'py> {
        let name = "not_null";
        let args = vec![self.to_expr(expr)?];
        self.visit_function(name, args)
    }

    fn visit_fill_null(&self, expr: &ExprRef, expr1: &ExprRef) -> PyVisitorResult<'py> {
        let name = "fill_null";
        let args = vec![self.to_expr(expr)?, self.to_expr(expr1)?];
        self.visit_function(name, args)
    }

    fn visit_is_in(&self, expr: &ExprRef, exprs: &[ExprRef]) -> PyVisitorResult<'py> {
        let name = "is_in";
        let expr = self.to_expr(expr)?;
        let items = PyList::empty(self.py);
        for item in exprs {
            items.append(self.to_expr(item)?)?;
        }
        let args = vec![expr, items.into_any()];
        self.visit_function(name, args)
    }

    fn visit_between(
        self,
        expr: &ExprRef,
        expr1: &ExprRef,
        expr2: &ExprRef,
    ) -> PyVisitorResult<'py> {
        let name = "between";
        let args = vec![
            self.to_expr(expr)?,
            self.to_expr(expr1)?,
            self.to_expr(expr2)?,
        ];
        self.visit_function(name, args)
    }

    fn visit_list(&self, exprs: &[ExprRef]) -> PyVisitorResult<'py> {
        let name = "list";
        let items = PyList::empty(self.py);
        for item in exprs {
            items.append(self.to_expr(item)?)?;
        }
        let args = vec![items.into_any()];
        self.visit_function(name, args)
    }

    fn visit_if_else(
        self,
        if_true: &ExprRef,
        if_false: &ExprRef,
        predicate: &ExprRef,
    ) -> PyVisitorResult<'py> {
        let name = "if_else";
        let args = vec![
            self.to_expr(predicate)?,
            self.to_expr(if_true)?,
            self.to_expr(if_false)?,
        ];
        self.visit_function(name, args)
    }

    fn visit_builtin_scalar_func(&self, scalar_function: &BuiltinScalarFn) -> PyVisitorResult<'py> {
        let name = scalar_function.name();
        let args: Vec<_> = scalar_function
            .inputs
            .iter()
            .map(|arg| self.to_expr(arg.inner()))
            .collect::<PyResult<_>>()?;
        self.visit_function(name, args)
    }

    #[allow(unused_variables)]
    fn visit_subquery(&self, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support subquery expressions",
        ))
    }

    #[allow(unused_variables)]
    fn visit_in_subquery(&self, expr: &ExprRef, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support subquery expressions",
        ))
    }

    #[allow(unused_variables)]
    fn visit_exists(&self, subquery: &Subquery) -> PyVisitorResult<'py> {
        Err(PyValueError::new_err(
            "Visitor does not support subquery expressions",
        ))
    }

    // Developer Note
    // ==============
    // These *could* be factored out as IntoPyObject implementations,
    // but there are few caveats, and I wish to keep these conversions
    // scoped to interactions with the visitor for now. For types like
    // Expr and DataType we cannot implement the trait on the external
    // type. We don't want to implement IntoPyObject for PyExpr etc. since
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
}
