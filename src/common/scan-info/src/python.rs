use daft_dsl::{
    functions::{partitioning::PartitioningExpr, FunctionExpr},
    Column, Expr, LiteralValue, Operator, ResolvedColumn,
};
use daft_schema::schema::SchemaRef;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyList, PyTuple},
};

pub mod pylib {
    use std::sync::Arc;

    use daft_dsl::python::PyExpr;
    use daft_schema::python::{field::PyField, schema::PySchema};
    use pyo3::{prelude::*, pyclass};
    use serde::{Deserialize, Serialize};

    use super::TermBuilder;
    use crate::{PartitionField, PartitionTransform, Pushdowns};

    #[pyclass(module = "daft.daft", name = "PartitionField", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionField(pub Arc<PartitionField>);

    #[pymethods]
    impl PyPartitionField {
        #[new]
        #[pyo3(signature = (field, source_field=None, transform=None))]
        fn new(
            field: PyField,
            source_field: Option<PyField>,
            transform: Option<PyPartitionTransform>,
        ) -> PyResult<Self> {
            let p_field = PartitionField::new(
                field.field,
                source_field.map(std::convert::Into::into),
                transform.map(|e| e.0),
            )?;
            Ok(Self(Arc::new(p_field)))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }

        #[getter]
        pub fn field(&self) -> PyResult<PyField> {
            Ok(self.0.field.clone().into())
        }
    }

    #[pyclass(module = "daft.daft", name = "PartitionTransform", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPartitionTransform(pub PartitionTransform);

    #[pymethods]
    impl PyPartitionTransform {
        #[staticmethod]
        pub fn identity() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Identity))
        }

        #[staticmethod]
        pub fn year() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Year))
        }

        #[staticmethod]
        pub fn month() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Month))
        }

        #[staticmethod]
        pub fn day() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Day))
        }

        #[staticmethod]
        pub fn hour() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Hour))
        }

        #[staticmethod]
        pub fn void() -> PyResult<Self> {
            Ok(Self(PartitionTransform::Void))
        }

        #[staticmethod]
        pub fn iceberg_bucket(n: u64) -> PyResult<Self> {
            Ok(Self(PartitionTransform::IcebergBucket(n)))
        }

        #[staticmethod]
        pub fn iceberg_truncate(n: u64) -> PyResult<Self> {
            Ok(Self(PartitionTransform::IcebergTruncate(n)))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.0))
        }
    }

    #[pyclass(module = "daft.daft", name = "Pushdowns", frozen)]
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PyPushdowns(pub Arc<Pushdowns>);

    #[pymethods]
    impl PyPushdowns {
        #[new]
        #[pyo3(signature = (
            filters = None,
            partition_filters = None,
            columns = None,
            limit = None,
        ))]
        pub fn new(
            filters: Option<PyExpr>,
            partition_filters: Option<PyExpr>,
            columns: Option<Vec<String>>,
            limit: Option<usize>,
        ) -> Self {
            let pushdowns = Pushdowns::new(
                filters.map(|f| f.expr),
                partition_filters.map(|f| f.expr),
                columns.map(Arc::new),
                limit,
            );
            Self(Arc::new(pushdowns))
        }

        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{:#?}", self.0))
        }

        #[getter]
        #[must_use]
        pub fn limit(&self) -> Option<usize> {
            self.0.limit
        }

        #[getter]
        #[must_use]
        pub fn filters(&self) -> Option<PyExpr> {
            self.0.filters.as_ref().map(|e| PyExpr { expr: e.clone() })
        }

        #[getter]
        #[must_use]
        pub fn partition_filters(&self) -> Option<PyExpr> {
            self.0
                .partition_filters
                .as_ref()
                .map(|e| PyExpr { expr: e.clone() })
        }

        #[getter]
        #[must_use]
        pub fn columns(&self) -> Option<Vec<String>> {
            self.0.columns.as_deref().cloned()
        }

        pub fn filter_required_column_names(&self) -> Option<Vec<String>> {
            self.0
                .filters
                .as_ref()
                .map(daft_dsl::optimization::get_required_columns)
        }

        #[staticmethod]
        #[pyo3(signature = (expr,schema=None))]
        pub fn _to_term(
            py: Python<'_>,
            expr: PyExpr,
            schema: Option<PySchema>,
        ) -> PyResult<PyObject> {
            let schema = schema.map(|s| s.schema);
            let tb = TermBuilder::new(py, schema)?;
            let term = tb.to_term(expr.expr.as_ref())?;
            Ok(term.unbind())
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<pylib::PyPartitionField>()?;
    parent.add_class::<pylib::PyPartitionTransform>()?;
    parent.add_class::<pylib::PyPushdowns>()?;
    Ok(())
}

//-------------------------------
// Produce python pushdown terms.
//-------------------------------

/// Terms during building are bound, then we unbind once done.
type Term<'py> = Bound<'py, PyAny>;

/// Holds the py GIL and module to make bound term building easy.
struct TermBuilder<'py> {
    py: Python<'py>,
    schema: Option<SchemaRef>,
    module: Bound<'py, PyModule>,
}

impl<'py> TermBuilder<'py> {
    fn new(py: Python<'py>, schema: Option<SchemaRef>) -> PyResult<Self> {
        Ok(Self {
            py,
            schema,
            module: PyModule::import(py, "daft.io.pushdowns")?,
        })
    }

    fn to_term(&self, expr: &Expr) -> PyResult<Term> {
        if let Expr::Column(col) = expr {
            return self.to_reference(col);
        }
        if let Expr::Literal(lit) = expr {
            return self.to_literal(lit);
        }
        let proc: &str;
        let args = PyList::empty(self.py);
        match &expr {
            Expr::Alias(expr, alias) => {
                // (alias <alias> <expr>)
                proc = "alias";
                args.append(alias.to_string())?;
                args.append(self.to_term(expr)?)?;
            }
            Expr::Not(expr) => {
                // (not <expr>)
                proc = "not";
                args.append(self.to_term(expr)?)?;
            }
            Expr::BinaryOp { op, left, right } => {
                // (operator <lhs> <rhs>)
                proc = Self::proc_of_predicate(op);
                args.append(self.to_term(left)?)?;
                args.append(self.to_term(right)?)?;
            }
            Expr::IsNull(expr) => {
                // (is_null <expr>)
                proc = "is_null";
                args.append(self.to_term(expr)?)?;
            }
            Expr::NotNull(expr) => {
                // (not_null <expr>)
                proc = "not_null";
                args.append(self.to_term(expr)?)?;
            }
            Expr::Between(expr, expr1, expr2) => {
                // (between <expr> <lower> <upper>)
                proc = "between";
                args.append(self.to_term(expr)?)?;
                args.append(self.to_term(expr1)?)?;
                args.append(self.to_term(expr2)?)?;
            }
            Expr::Function { func, inputs } => {
                // (<proc> <args>...)
                proc = Self::proc_of_function(func)?;
                for arg in inputs {
                    args.append(self.to_term(arg.as_ref())?)?;
                }
                // ineligant .. add args from the two iceberg partitioning functions
                if let FunctionExpr::Partitioning(pexpr) = func {
                    match pexpr {
                        PartitioningExpr::IcebergBucket(buckets) => {
                            let lit = LiteralValue::Int32(*buckets);
                            let arg = self.to_literal(&lit)?;
                            args.append(arg)?;
                        }
                        PartitioningExpr::IcebergTruncate(size) => {
                            let lit = LiteralValue::Int64(*size);
                            let arg = self.to_literal(&lit)?;
                            args.append(arg)?;
                        }
                        _ => {}
                    };
                }
            }
            Expr::ScalarFunction(scalar_function) => {
                // (<proc> <args>...)
                proc = scalar_function.name();
                for arg in scalar_function.inputs.iter().as_ref() {
                    args.append(self.to_term(arg.as_ref())?)?;
                }
            }
            Expr::List(items) => {
                // (list <items>...)
                proc = "list";
                for arg in items.iter().as_ref() {
                    args.append(self.to_term(arg.as_ref())?)?;
                }
            }
            Expr::IsIn(item, items) => {
                // (is_in item (list <items>))
                proc = "is_in";
                let items = Expr::List(items.clone());
                args.append(self.to_term(item)?)?;
                args.append(self.to_term(items.as_ref())?)?;
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported pushdown expression: {}",
                    expr
                )))
            }
        };
        self.to_expr(proc, args)
    }

    /// Reference(<path> [, <index>])
    fn to_reference(&self, column: &Column) -> PyResult<Term> {
        // we allow unbound columns in construction.
        let name = match column {
            Column::Unresolved(col) => col.name.to_string(),
            Column::Resolved(col) => match col {
                ResolvedColumn::Basic(col) => col.to_string(),
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "Unexpected or unresolved column found in pushdown: {}",
                        column
                    )))
                }
            },
        };
        // if we have a schema, create a bound reference by setting its index.
        let index = if let Some(schema) = &self.schema {
            Some(schema.get_index(&name)?)
        } else {
            None
        };
        self.module.getattr("Reference")?.call1((name, index))
    }

    /// Literal(<value>)
    fn to_literal(&self, literal: &LiteralValue) -> PyResult<Term> {
        let cls = self.module.getattr("Literal")?;
        match literal {
            // null
            LiteralValue::Null => cls.call1((None::<bool>,)),
            // bool
            LiteralValue::Boolean(b) => cls.call1((b,)),
            // int
            LiteralValue::Int8(i) => cls.call1((i,)),
            LiteralValue::UInt8(i) => cls.call1((i,)),
            LiteralValue::Int16(i) => cls.call1((i,)),
            LiteralValue::UInt16(i) => cls.call1((i,)),
            LiteralValue::Int32(i) => cls.call1((i,)),
            LiteralValue::UInt32(i) => cls.call1((i,)),
            LiteralValue::Int64(i) => cls.call1((i,)),
            LiteralValue::UInt64(i) => cls.call1((i,)),
            // float
            LiteralValue::Float64(f) => cls.call1((f,)),
            // str
            LiteralValue::Utf8(s) => cls.call1((s,)),
            // unsupported literals
            _ => Err(PyValueError::new_err(format!(
                "Unsupported pushdown literal: {}",
                literal
            ))),
        }
    }

    /// Expr(<proc>, *args, **kwargs)
    fn to_expr(&self, proc: &str, args: Bound<'py, PyList>) -> PyResult<Term> {
        args.insert(0, proc.to_string())?;
        let args = PyTuple::new(self.py, args)?;
        self.module.getattr("Expr")?.call1(args)
    }

    /// Term procedure names for binary operator predicates inpushdowns.
    fn proc_of_predicate(operator: &Operator) -> &'static str {
        match operator {
            // only works for logical connectives!
            Operator::And => "and",
            Operator::Or => "or",
            Operator::Xor => "xor",
            // use scheme-like comparison operators (please don't use display).
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            // use schema-like arithmetic operators.
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::TrueDivide => "/",
            // use scheme-like names (please don't use display).
            Operator::FloorDivide => "quotient",
            Operator::Modulus => "mod",
            Operator::EqNullSafe => "eq_null_safe",
            // ash is too arcane and more complicated
            Operator::ShiftLeft => "lshift",
            Operator::ShiftRight => "rshift",
        }
    }

    /// Term procedure names for functions
    fn proc_of_function(function: &FunctionExpr) -> PyResult<&str> {
        match function {
            FunctionExpr::Python(udf) => Ok(&*udf.name),
            FunctionExpr::Partitioning(pexpr) => match pexpr {
                PartitioningExpr::Years => Ok("years"),
                PartitioningExpr::Months => Ok("months"),
                PartitioningExpr::Days => Ok("days"),
                PartitioningExpr::Hours => Ok("hours"),
                // TODO parameters
                PartitioningExpr::IcebergBucket(_) => Ok("iceberg_bucket"),
                PartitioningExpr::IcebergTruncate(_) => Ok("iceberg_truncate"),
            },
            _ => Err(PyValueError::new_err(format!(
                "Unsupported pushdown function: {}",
                function
            ))),
        }
    }
}
