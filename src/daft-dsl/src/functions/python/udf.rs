use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
#[cfg(feature = "python")]
use pyo3::{
    types::{PyAnyMethods, PyModule},
    Bound, PyAny, PyResult,
};

use super::{super::FunctionEvaluator, PythonUDF};
use crate::{functions::FunctionExpr, ExprRef};

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
    let py_series_module = PyModule::import(py, pyo3::intern!(py, "daft.series"))?;
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
    let py_udf_module = PyModule::import(py, pyo3::intern!(py, "daft.udf"))?;
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

impl PythonUDF {
    #[cfg(feature = "python")]
    pub fn call_udf(&self, inputs: &[Series]) -> DaftResult<Series> {
        use pyo3::Python;

        use crate::functions::python::{py_udf_initialize, MaybeInitializedUDF};

        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }

        Python::with_gil(|py| {
            let func = match &self.func {
                MaybeInitializedUDF::Initialized(func) => func.clone().unwrap().clone_ref(py),
                MaybeInitializedUDF::Uninitialized { inner, init_args } => {
                    // TODO(Kevin): warn user if initialization is taking too long and ask them to use actor pool UDFs

                    py_udf_initialize(py, inner.clone().unwrap(), init_args.clone().unwrap())?
                }
            };

            run_udf(
                py,
                inputs,
                func,
                self.bound_args.clone().unwrap().clone_ref(py),
                &self.return_dtype,
                self.batch_size,
            )
        })
    }
}

impl FunctionEvaluator for PythonUDF {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(&self, inputs: &[ExprRef], _: &Schema, _: &FunctionExpr) -> DaftResult<Field> {
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
            panic!("Cannot evaluate a PythonUDF without compiling for Python");
        }
        #[cfg(feature = "python")]
        {
            self.call_udf(inputs)
        }
    }
}
