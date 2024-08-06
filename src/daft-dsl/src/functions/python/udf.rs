use daft_core::DataType;

#[cfg(feature = "python")]
use pyo3::{types::PyModule, PyAny, PyResult};

use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::ExprRef;

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;
use super::{StatefulPythonUDF, StatelessPythonUDF};
use crate::functions::FunctionExpr;

impl FunctionEvaluator for StatelessPythonUDF {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        _schema: &Schema,
        _: &FunctionExpr,
    ) -> DaftResult<Field> {
        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }
        match inputs {
            [] => Err(DaftError::ValueError(
                "Cannot run UDF with 0 expression arguments".into(),
            )),
            [first, ..] => Ok(Field::new(first.name(), self.return_dtype.clone())),
        }
    }

    #[cfg(feature = "python")]
    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        self.call_udf(inputs)
    }

    #[cfg(not(feature = "python"))]
    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        panic!("Cannot evaluate a StatelessPythonUDF without compiling for Python");
    }
}

#[cfg(feature = "python")]
fn run_udf(
    py: pyo3::Python,
    inputs: &[Series],
    func: pyo3::Py<PyAny>,
    bound_args: pyo3::Py<PyAny>,
    return_dtype: &DataType,
) -> DaftResult<Series> {
    use daft_core::python::{PyDataType, PySeries};

    // Convert input Rust &[Series] to wrapped Python Vec<&PyAny>
    let py_series_module = PyModule::import(py, pyo3::intern!(py, "daft.series"))?;
    let py_series_class = py_series_module.getattr(pyo3::intern!(py, "Series"))?;
    let pyseries: PyResult<Vec<&PyAny>> = inputs
        .iter()
        .map(|s| {
            py_series_class.call_method(
                pyo3::intern!(py, "_from_pyseries"),
                (PySeries { series: s.clone() },),
                None,
            )
        })
        .collect();
    let pyseries = pyseries?;

    // Run the function on the converted Vec<&PyAny>
    let py_udf_module = PyModule::import(py, pyo3::intern!(py, "daft.udf"))?;
    let run_udf_func = py_udf_module.getattr(pyo3::intern!(py, "run_udf"))?;
    let result = run_udf_func.call1((
        func,                                   // Function to run
        bound_args,                             // Arguments bound to the function
        pyseries,                               // evaluated_expressions
        PyDataType::from(return_dtype.clone()), // Returned datatype
    ));

    match result {
        Ok(pyany) => {
            let pyseries = pyany.extract::<PySeries>();
            match pyseries {
                Ok(pyseries) => Ok(pyseries.series),
                Err(e) => Err(DaftError::ValueError(format!("Internal error occurred when coercing the results of running UDF to Series:\n\n{e}"))),
            }
        }
        Err(e) => Err(e.into()),
    }
}

impl StatelessPythonUDF {
    #[cfg(feature = "python")]
    pub fn call_udf(&self, inputs: &[Series]) -> DaftResult<Series> {
        use pyo3::Python;

        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }

        Python::with_gil(|py| {
            // Extract the required Python objects to call our run_udf helper
            let func = self.partial_func.0.getattr(py, pyo3::intern!(py, "func"))?;
            let bound_args = self
                .partial_func
                .0
                .getattr(py, pyo3::intern!(py, "bound_args"))?;

            run_udf(py, inputs, func, bound_args, &self.return_dtype)
        })
    }
}

impl FunctionEvaluator for StatefulPythonUDF {
    fn fn_name(&self) -> &'static str {
        "pyclass_udf"
    }

    fn to_field(
        &self,
        inputs: &[ExprRef],
        _schema: &Schema,
        _: &FunctionExpr,
    ) -> DaftResult<Field> {
        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }
        match inputs {
            [] => Err(DaftError::ValueError(
                "Cannot run UDF with 0 expression arguments".into(),
            )),
            [first, ..] => Ok(Field::new(first.name(), self.return_dtype.clone())),
        }
    }

    #[cfg(feature = "python")]
    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        use pyo3::Python;

        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }

        Python::with_gil(|py| {
            // Extract the required Python objects to call our run_udf helper
            let func = self
                .stateful_partial_func
                .0
                .getattr(py, pyo3::intern!(py, "func_cls"))?;
            let bound_args = self
                .stateful_partial_func
                .0
                .getattr(py, pyo3::intern!(py, "bound_args"))?;

            // HACK: This is the naive initialization of the class. It is performed once-per-evaluate which is not ideal.
            // Ideally we need to allow evaluate to somehow take in the **initialized** Python class that is provided by the Actor.
            // Either that, or the code-path to evaluate a StatefulUDF should bypass `evaluate` entirely and do its own thing.
            let func = func.call0(py)?;

            run_udf(py, inputs, func, bound_args, &self.return_dtype)
        })
    }

    #[cfg(not(feature = "python"))]
    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        panic!("Cannot evaluate a StatelessPythonUDF without compiling for Python");
    }
}
