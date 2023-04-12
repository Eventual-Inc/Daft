use crate::{
    datatypes::Field,
    dsl::Expr,
    error::{DaftError, DaftResult},
    schema::Schema,
    series::Series,
};

use super::super::FunctionEvaluator;
use super::PythonUDF;
use crate::python::PySeries;

impl FunctionEvaluator for PythonUDF {
    fn fn_name(&self) -> &'static str {
        "py_udf"
    }

    fn to_field(&self, inputs: &[Expr], _schema: &Schema) -> DaftResult<Field> {
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
            [first, ..] => Ok(Field::new(first.name()?, self.return_dtype.clone())),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        use pyo3::Python;

        if inputs.len() != self.num_expressions {
            return Err(DaftError::SchemaMismatch(format!(
                "Number of inputs required by UDF {} does not match number of inputs provided: {}",
                self.num_expressions,
                inputs.len()
            )));
        }

        Python::with_gil(|py| {
            let func = self.func.0.clone_ref(py).into_ref(py);
            let pyseries: Vec<PySeries> = inputs
                .iter()
                .map(|s| PySeries { series: s.clone() })
                .collect();
            let result = func.call1((pyseries,));

            match result {
                Ok(pyany) => {
                    let pyseries = pyany.extract::<PySeries>();
                    match pyseries {
                        Ok(pyseries) => Ok(pyseries.series),
                        Err(e) => Err(DaftError::ValueError(format!("Internal error occurred when coercing the results of running UDF to Series:\n\n{e}"))),
                    }
                }
                Err(e) => Err(DaftError::ComputeError(format!(
                    "Error occurred while running UDF:\n\n{e}"
                ))),
            }
        })
    }
}
