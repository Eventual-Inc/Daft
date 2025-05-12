use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

trait SpatialSimilarity {
    fn cosine(a: &[Self], b: &[Self]) -> Option<f64>
    where
        Self: Sized;
}

impl SpatialSimilarity for f64 {
    fn cosine(a: &[Self], b: &[Self]) -> Option<f64> {
        let xy = a.iter().zip(b).map(|(a, b)| a * b).sum::<Self>();
        let x_sq = a.iter().map(|x| x.powi(2)).sum::<Self>().sqrt();
        let y_sq = b.iter().map(|x| x.powi(2)).sum::<Self>().sqrt();
        Some(1.0 - xy / (x_sq * y_sq))
    }
}

impl SpatialSimilarity for f32 {
    fn cosine(a: &[Self], b: &[Self]) -> Option<f64> {
        let xy = a
            .iter()
            .zip(b)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>();
        let x_sq = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        let y_sq = b.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        Some(1.0 - xy / (x_sq * y_sq))
    }
}

impl SpatialSimilarity for i8 {
    fn cosine(a: &[Self], b: &[Self]) -> Option<f64> {
        let xy = a
            .iter()
            .zip(b)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>();
        let x_sq = a.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        let y_sq = b.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
        Some(1.0 - xy / (x_sq * y_sq))
    }
}

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
    // Check if query is a single literal or a series.
    if query.len() == 1 {
        let query = &query.fixed_size_list()?.flat_child;
        let query = query.try_as_slice::<T>()?;

        Ok(source
            .into_iter()
            .map(|list_opt| {
                // Safe to unwrap after try_as_slice because types should be checked at this point.
                let list_slice = list_opt.as_ref()?.try_as_slice::<T>().ok()?;
                SpatialSimilarity::cosine(list_slice, query)
            })
            .collect::<Vec<_>>())
    } else {
        if query.len() != source.len() {
            return Err(DaftError::ValueError(format!(
                "Query length ({}) must match source length ({})",
                query.len(),
                source.len()
            )));
        }
        let query = query.fixed_size_list()?;

        Ok(source
            .into_iter()
            .zip(query.into_iter())
            .map(|(list_opt, query_opt)| {
                // Safe to unwrap after try_as_slice because types should be checked at this point.
                let list_slice = list_opt.as_ref()?.try_as_slice::<T>().ok()?;
                let query_slice = query_opt.as_ref()?.try_as_slice::<T>().ok()?;
                SpatialSimilarity::cosine(list_slice, query_slice)
            })
            .collect::<Vec<_>>())
    }
}

#[typetag::serde]
impl ScalarUDF for CosineDistanceFunction {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "cosine_distance"
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [source, query] => {
                let source_name = source.name();
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

                let output = Float64Array::from_iter(
                    Field::new(source_name, DataType::Float64),
                    res.into_iter(),
                );

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

                if let Ok((source_size, query_size)) = source
                    .dtype
                    .fixed_size()
                    .and_then(|source| query.dtype.fixed_size().map(|q| (source, q)))
                {
                    if source_size != query_size {
                        return Err(DaftError::ValueError(format!(
                            "Expected source and query to have the same size, instead got {source_size} and {query_size}"
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

#[must_use]
pub fn cosine_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFunction::new(CosineDistanceFunction {}, vec![a, b]).into()
}
