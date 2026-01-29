use common_error::{DaftError, DaftResult, value_err};
use daft_core::{datatypes::NumericNative, prelude::*};
use daft_dsl::functions::prelude::*;

#[derive(FunctionArgs)]
pub(crate) struct Args<T> {
    pub(crate) input: T,
    pub(crate) query: T,
}

pub(crate) fn validate_vector_inputs(
    function_name: &str,
    source: &Field,
    query: &Field,
) -> DaftResult<Field> {
    match (&source.dtype, &query.dtype) {
        (
            DataType::FixedSizeList(source_inner_dtype, source_size)
            | DataType::Embedding(source_inner_dtype, source_size),
            DataType::FixedSizeList(query_inner_dtype, query_size)
            | DataType::Embedding(query_inner_dtype, query_size),
        ) => {
            if source_inner_dtype != query_inner_dtype {
                value_err!(
                    "Expected inputs to '{}' to have the same inner dtype, instead got {source_inner_dtype} and {query_inner_dtype}",
                    function_name
                )
            }
            if !matches!(
                source_inner_dtype.as_ref(),
                DataType::Int8 | DataType::Float32 | DataType::Float64
            ) {
                value_err!(
                    "Expected inputs to '{}' to have Int8|Float32|Float64 inner dtype, instead got {source_inner_dtype}",
                    function_name
                )
            }
            if source_size != query_size {
                value_err!(
                    "Expected inputs to '{}' to have the same size, instead got {source_size} and {query_size}",
                    function_name
                )
            }
            Ok(Field::new(source.name.clone(), DataType::Float64))
        }
        _ => {
            value_err!(
                "Expected inputs to '{}' to be fixed size list or embedding, instead got {} and {}",
                function_name,
                source.dtype,
                query.dtype
            )
        }
    }
}

pub(crate) trait VectorMetric {
    fn metric<T: NumericNative>(a: &[T], b: &[T]) -> Option<f64>;
}

fn compute_metric<T, M>(source: &FixedSizeListArray, query: &Series) -> DaftResult<Vec<Option<f64>>>
where
    T: NumericNative,
    M: VectorMetric,
{
    // Check if a query is a single literal or a series.
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
                M::metric(list_slice, query)
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
                M::metric(list_slice, query_slice)
            })
            .collect::<Vec<_>>())
    }
}

pub(crate) fn compute_vector_metric<M: VectorMetric>(
    function_name: &str,
    source: &Series,
    query: &Series,
) -> DaftResult<Vec<Option<f64>>> {
    match (source.data_type(), query.data_type()) {
        (
            DataType::FixedSizeList(source_dtype, source_size)
            | DataType::Embedding(source_dtype, source_size),
            DataType::FixedSizeList(query_dtype, query_size)
            | DataType::Embedding(query_dtype, query_size),
        ) if source_dtype == query_dtype && source_size == query_size => {
            let source_physical = source.as_physical()?;
            let source_fixed_size_list = source_physical.fixed_size_list()?;
            match source_dtype.as_ref() {
                DataType::Int8 => compute_metric::<i8, M>(source_fixed_size_list, query),
                DataType::Float32 => compute_metric::<f32, M>(source_fixed_size_list, query),
                DataType::Float64 => compute_metric::<f64, M>(source_fixed_size_list, query),
                _ => {
                    unreachable!(
                        "Expected inputs to '{}' to have Int8|Float32|Float64 datatypes, instead got {}",
                        function_name, source_dtype
                    )
                }
            }
        }
        _ => {
            unreachable!(
                "Expected inputs to '{}' to be fixed size list or embedding with the same dtype and size, instead got {} and {}",
                function_name,
                source.data_type(),
                query.data_type()
            )
        }
    }
}
