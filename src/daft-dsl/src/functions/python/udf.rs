use pyo3::{types::PyModule, PyAny, PyResult};

use daft_core::{datatypes::Field, schema::Schema, series::Series};

use crate::ExprRef;

use common_error::{DaftError, DaftResult};

use super::super::FunctionEvaluator;
use super::PythonUDF;
use crate::functions::FunctionExpr;
use daft_core::python::PySeries;

impl FunctionEvaluator for PythonUDF {
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
        self.call_udf(inputs)
    }
}

impl PythonUDF {
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

            // Call function on the converted Vec<&PyAny>
            let func = self.func.0.clone_ref(py).into_ref(py);
            let result = func.call1((pyseries,));

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
        })
    }
}
