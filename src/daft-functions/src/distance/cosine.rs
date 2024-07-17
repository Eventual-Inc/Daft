use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{Field, Float32Array},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineFunction {}

#[typetag::serde]
impl ScalarUDF for CosineFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cosine"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [source, query] => {
                let source_name = source.name();
                if query.len() != 1 {
                    return Err(DaftError::ValueError(
                        "Expected query to be a single value".to_string(),
                    ));
                }
                let query = &query.fixed_size_list()?.flat_child;
                let query = query.f32()?.as_slice();
                let source = source.fixed_size_list()?;

                let iter = source
                    .into_iter()
                    .map(|list_opt| {
                        let list = list_opt?;
                        let list = list.f32().unwrap().as_slice();
                        let cosine = cosine_impl(list, query);
                        Some(cosine)
                    })
                    .collect::<Vec<_>>(); // is it possible to avoid this 'collect'?

                let output = Float32Array::from_iter(source_name, iter.into_iter());

                Ok(output.into_series())
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [source, query] => {
                let source = source.to_field(schema)?;
                let query = query.to_field(schema)?;
                let source_is_numeric = source.dtype.is_fixed_size_numeric();
                let query_is_numeric = query.dtype.is_fixed_size_numeric();

                if source_is_numeric && query_is_numeric {
                    Ok(Field::new(source.name, DataType::Float32))
                } else {
                    Err(DaftError::ValueError(format!(
                        "Expected nested list for source and numeric list for query, instead got {} and {}",
                        source.dtype, query.dtype
                    )))
                }
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }
}

// TODO: this is a placeholder implementation
fn cosine_impl(x: &[f32], y: &[f32]) -> f32 {
    let xy = x
        .iter()
        .zip(y.iter())
        .map(|(&xi, &yi)| xi * yi)
        .sum::<f32>();
    let x_sq = x.iter().map(|&xi| xi * xi).sum::<f32>().sqrt();
    let y_sq = y.iter().map(|&yi| yi * yi).sum::<f32>().sqrt();
    1.0 - xy / x_sq / y_sq
}

pub fn cosine(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFunction::new(CosineFunction {}, vec![a, b]).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn cosine(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
        Ok(super::cosine(a.into(), b.into()).into())
    }
}
