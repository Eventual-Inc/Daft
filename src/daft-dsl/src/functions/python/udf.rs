use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
#[cfg(feature = "python")]
use pyo3::{
    types::{PyAnyMethods, PyModule},
    Bound, PyAny, PyResult,
};

use super::{super::FunctionEvaluator, StatefulPythonUDF, StatelessPythonUDF};
use crate::{functions::FunctionExpr, ExprRef};

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

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot evaluate a StatelessPythonUDF without compiling for Python");
        }
        #[cfg(feature = "python")]
        {
            self.call_udf(inputs)
        }
    }
}

#[cfg(feature = "python")]
fn run_udf(
    py: pyo3::Python,
    inputs: &[Series],
    func: pyo3::Py<PyAny>,
    bound_args: pyo3::Py<PyAny>,
    return_dtype: &DataType,
    batch_size: Option<usize>,
) -> DaftResult<Series> {
    use daft_core::python::{PyDataType, PySeries};

    // Convert input Rust &[Series] to wrapped Python Vec<Bound<PyAny>>
    let py_series_module = PyModule::import_bound(py, pyo3::intern!(py, "daft.series"))?;
    let py_series_class = py_series_module.getattr(pyo3::intern!(py, "Series"))?;
    let pyseries: PyResult<Vec<Bound<PyAny>>> = inputs
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

    // Run the function on the converted Vec<Bound<PyAny>>
    let py_udf_module = PyModule::import_bound(py, pyo3::intern!(py, "daft.udf"))?;
    let run_udf_func = py_udf_module.getattr(pyo3::intern!(py, "run_udf"))?;
    let result = run_udf_func.call1((
        func,                                   // Function to run
        bound_args,                             // Arguments bound to the function
        pyseries,                               // evaluated_expressions
        PyDataType::from(return_dtype.clone()), // Returned datatype
        batch_size,
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
            let func = self
                .partial_func
                .as_ref()
                .getattr(py, pyo3::intern!(py, "func"))?;
            let bound_args = self
                .partial_func
                .as_ref()
                .getattr(py, pyo3::intern!(py, "bound_args"))?;

            run_udf(
                py,
                inputs,
                func,
                bound_args,
                &self.return_dtype,
                self.batch_size,
            )
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

    fn evaluate(&self, inputs: &[Series], _: &FunctionExpr) -> DaftResult<Series> {
        #[cfg(not(feature = "python"))]
        {
            panic!("Cannot evaluate a StatelessPythonUDF without compiling for Python");
        }

        #[cfg(feature = "python")]
        {
            use pyo3::{
                types::{PyDict, PyTuple},
                Python,
            };

            use crate::functions::python::udf_runtime_binding::UDFRuntimeBinding;

            if inputs.len() != self.num_expressions {
                return Err(DaftError::SchemaMismatch(format!(
                    "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                    self.num_expressions,
                    inputs.len()
                )));
            }

            if let UDFRuntimeBinding::Bound(func) = &self.runtime_binding {
                Python::with_gil(|py| {
                    // Extract the required Python objects to call our run_udf helper
                    let bound_args = self
                        .stateful_partial_func
                        .as_ref()
                        .getattr(py, pyo3::intern!(py, "bound_args"))?;
                    run_udf(
                        py,
                        inputs,
                        pyo3::Py::clone_ref(func.as_ref(), py),
                        bound_args,
                        &self.return_dtype,
                        self.batch_size,
                    )
                })
            } else {
                // NOTE: This branch of evaluation performs a naive initialization of the class. It is performed once-per-evaluate
                // which is not ideal. Callers trying to .evaluate a StatefulPythonUDF should first bind it to initialized classes.
                Python::with_gil(|py| {
                    // Extract the required Python objects to call our run_udf helper
                    let func = self
                        .stateful_partial_func
                        .as_ref()
                        .getattr(py, pyo3::intern!(py, "func_cls"))?;
                    let bound_args = self
                        .stateful_partial_func
                        .as_ref()
                        .getattr(py, pyo3::intern!(py, "bound_args"))?;

                    let func = match &self.init_args {
                        None => func.call0(py)?,
                        Some(init_args) => {
                            let init_args = init_args
                                .as_ref()
                                .bind(py)
                                .downcast::<PyTuple>()
                                .expect("init_args should be a Python tuple");
                            let (args, kwargs) = (
                                init_args
                                    .get_item(0)?
                                    .downcast::<PyTuple>()
                                    .expect("init_args[0] should be a tuple of *args")
                                    .clone(),
                                init_args
                                    .get_item(1)?
                                    .downcast::<PyDict>()
                                    .expect("init_args[1] should be a dict of **kwargs")
                                    .clone(),
                            );
                            func.call_bound(py, args, Some(&kwargs))?
                        }
                    };

                    run_udf(
                        py,
                        inputs,
                        func,
                        bound_args,
                        &self.return_dtype,
                        self.batch_size,
                    )
                })
            }
        }
    }
}
