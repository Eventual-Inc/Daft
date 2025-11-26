use common_error::{DaftError, DaftResult, value_err};
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    query: T,
}
#[typetag::serde]
impl ScalarUDF for CosineDistanceFunction {
    fn name(&self) -> &'static str {
        "cosine_distance"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let source_name = source.name();

        let res = match (source.data_type(), query.data_type()) {
            (
                DataType::FixedSizeList(source_dtype, source_size)
                | DataType::Embedding(source_dtype, source_size),
                DataType::FixedSizeList(query_dtype, query_size)
                | DataType::Embedding(query_dtype, query_size),
            ) if source_dtype == query_dtype && source_size == query_size => {
                let source_physical = source.as_physical()?;
                let source_fixed_size_list = source_physical.fixed_size_list()?;
                match source_dtype.as_ref() {
                    DataType::Int8 => compute_cosine_distance::<i8>(source_fixed_size_list, &query),
                    DataType::Float32 => {
                        compute_cosine_distance::<f32>(source_fixed_size_list, &query)
                    }
                    DataType::Float64 => {
                        compute_cosine_distance::<f64>(source_fixed_size_list, &query)
                    }
                    _ => {
                        unreachable!(
                            "Expected inputs to 'cosine_distance' to have Int8|Float32|Float32 datatypes, instead got {}",
                            source_dtype
                        )
                    }
                }
            }
            _ => {
                unreachable!(
                    "Expected inputs to 'cosine_distance' to be fixed size list or embedding with the same dtype and size, instead got {} and {}",
                    source.data_type(),
                    query.data_type()
                )
            }
        }?;

        let output =
            Float64Array::from_iter(Field::new(source_name, DataType::Float64), res.into_iter());

        Ok(output.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args {
            input: source,
            query,
        } = inputs.try_into()?;
        let source = source.to_field(schema)?;
        let query = query.to_field(schema)?;

        match (&source.dtype, &query.dtype) {
            (
                DataType::FixedSizeList(source_inner_dtype, source_size)
                | DataType::Embedding(source_inner_dtype, source_size),
                DataType::FixedSizeList(query_inner_dtype, query_size)
                | DataType::Embedding(query_inner_dtype, query_size),
            ) => {
                if source_inner_dtype != query_inner_dtype {
                    value_err!(
                        "Expected inputs to 'cosine_distance' to have the same inner dtype, instead got {source_inner_dtype} and {query_inner_dtype}"
                    )
                }
                if !matches!(
                    source_inner_dtype.as_ref(),
                    DataType::Int8 | DataType::Float32 | DataType::Float64
                ) {
                    value_err!(
                        "Expected inputs to 'cosine_distance' to have Int8|Float32|Float64 inner dtype, instead got {source_inner_dtype}"
                    )
                }
                if source_size != query_size {
                    value_err!(
                        "Expected inputs to 'cosine_distance' to have the same size, instead got {source_size} and {query_size}"
                    )
                }
                Ok(Field::new(source.name, DataType::Float64))
            }
            _ => {
                value_err!(
                    "Expected inputs to 'cosine_distance' to be fixed size list or embedding, instead got {} and {}",
                    source.dtype,
                    query.dtype
                )
            }
        }
    }
}

#[must_use]
pub fn cosine_distance(a: ExprRef, b: ExprRef) -> ExprRef {
    ScalarFn::builtin(CosineDistanceFunction {}, vec![a, b]).into()
}

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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CosineDistanceFunction;

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
        let query_physical = query.as_physical()?;
        let query_fixed_size_list = query_physical.fixed_size_list()?;
        let query = &query_fixed_size_list.flat_child;
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
        let query_physical = query.as_physical()?;
        let query_fixed_size_list = query_physical.fixed_size_list()?;

        Ok(source
            .into_iter()
            .zip(query_fixed_size_list.into_iter())
            .map(|(list_opt, query_opt)| {
                // Safe to unwrap after try_as_slice because types should be checked at this point.
                let list_slice = list_opt.as_ref()?.try_as_slice::<T>().ok()?;
                let query_slice = query_opt.as_ref()?.try_as_slice::<T>().ok()?;
                SpatialSimilarity::cosine(list_slice, query_slice)
            })
            .collect::<Vec<_>>())
    }
}
