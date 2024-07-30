use common_error::{DaftError, DaftResult};
use daft_core::{
    array::FixedSizeListArray,
    datatypes::{DaftNumericType, Field, Float64Array, NumericNative},
    schema::Schema,
    DataType, IntoSeries, Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};
use simsimd::SpatialSimilarity;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineDistanceFunction {}

fn compute_cosine_distance<T>(
    source: &FixedSizeListArray,
    query: &Series,
) -> DaftResult<Vec<Option<f64>>>
where
    T: NumericNative,
    <T::DAFTTYPE as DaftNumericType>::Native: SpatialSimilarity,
{
    let query = &query.fixed_size_list()?.flat_child;
    let query = query.try_as_slice::<T>()?;

    Ok(source
        .into_iter()
        .map(|list_opt| {
            let list = list_opt.as_ref()?;
            let list = list.try_as_slice::<T>();
            debug_assert!(
                list.is_ok(),
                "types should already be checked at this point"
            );
            let list = list.unwrap();
            SpatialSimilarity::cosine(list, query)
        })
        .collect::<Vec<_>>())
}

#[typetag::serde]
impl ScalarUDF for CosineDistanceFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "cosine_distance"
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

                let source = source.fixed_size_list()?;

                let res = match query.data_type() {
                    DataType::FixedSizeList(dtype, _) => match dtype.as_ref() {
                        DataType::Int8 => compute_cosine_distance::<i8>(source, query),
                        DataType::Float32 => compute_cosine_distance::<f32>(source, query),
                        DataType::Float64 => compute_cosine_distance::<f64>(source, query),
                        _ => {
                            return Err(DaftError::ValueError(
                                "'cosine_distance' only supports Int8|Float32|Float32 datatypes"
                                    .to_string(),
                            ));
                        }
                    },
                    _ => {
                        return Err(DaftError::ValueError(
                            "Expected query to be a nested list".to_string(),
                        ));
                    }
                }?;

                let output = Float64Array::from_iter(source_name, res.into_iter());

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

                if let Some((source_size, query_size)) = source
                    .dtype
                    .fixed_size()
                    .and_then(|source| query.dtype.fixed_size().map(|q| (source, q)))
                {
                    if source_size != query_size {
                        return Err(DaftError::ValueError(format!(
                            "Expected source and query to have the same size, instead got {} and {}",
                            source_size, query_size
                        )));
                    }
                } else {
                    return Err(DaftError::ValueError(format!(
                        "Expected source and query to be fixed size, instead got {} and {}",
                        source.dtype, query.dtype
                    )));
                }

                if source_is_numeric && query_is_numeric {
                    Ok(Field::new(source.name, DataType::Float64))
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

pub fn cosine_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFunction::new(CosineDistanceFunction {}, vec![a, b]).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn cosine_distance(a: PyExpr, b: PyExpr) -> PyResult<PyExpr> {
        Ok(super::cosine_distance(a.into(), b.into()).into())
    }
}
