use arrow::buffer::OffsetBuffer;
use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Series, UInt64Array},
    series::IntoSeries,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListSeq;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for ListSeq {
    fn name(&self) -> &'static str {
        "list_seq"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Args { input } = inputs.try_into()?;
        list_seq_impl(&input)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input } = inputs.try_into()?;
        let input_field = input.to_field(schema)?;

        if !input_field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected input to seq to be of integer type, received: {}",
                input_field.dtype
            )));
        }

        Ok(Field::new(
            input_field.name,
            DataType::List(Box::new(DataType::UInt64)),
        ))
    }
}

fn list_seq_impl(input: &Series) -> DaftResult<Series> {
    let input = input.cast(&DataType::Int64)?;
    let n_array = input.i64()?;

    let mut offsets = Vec::with_capacity(n_array.len() + 1);
    offsets.push(0i64);
    let mut values: Vec<u64> = Vec::new();
    let mut current_offset = 0i64;

    for i in 0..n_array.len() {
        if let Some(n) = n_array.get(i) {
            if n < 0 {
                return Err(DaftError::ValueError(format!(
                    "seq() requires non-negative n, received: {}",
                    n
                )));
            }
            for j in 0..n {
                values.push(j as u64);
            }
            current_offset += n;
        }
        offsets.push(current_offset);
    }

    let values_array = UInt64Array::from_values("item", values).into_series();

    let field = Field::new(input.name(), DataType::List(Box::new(DataType::UInt64)));

    let validity = input.nulls().cloned();

    let list_array = daft_core::array::ListArray::new(
        field,
        values_array,
        OffsetBuffer::new(offsets.into()),
        validity,
    );

    Ok(list_array.into_series())
}

#[must_use]
pub fn list_seq(n: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListSeq {}, vec![n]).into()
}

#[cfg(test)]
mod tests {
    use daft_core::{prelude::Int64Array, series::IntoSeries};
    use daft_dsl::functions::scalar::EvalContext;

    use super::*;

    #[test]
    fn test_seq_basic() {
        let n = Int64Array::from_values("n", [3i64, 5, 0]).into_series();
        let ctx = EvalContext { row_count: n.len() };
        let seq = ListSeq;
        let args = FunctionArgs::new_unnamed(vec![n]);
        let result = seq.call(args, &ctx).unwrap();

        assert_eq!(result.len(), 3);
        let list = result.list().unwrap();

        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 3);
        assert_eq!(row0.u64().unwrap().get(0), Some(0));
        assert_eq!(row0.u64().unwrap().get(1), Some(1));
        assert_eq!(row0.u64().unwrap().get(2), Some(2));

        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 5);
        assert_eq!(row1.u64().unwrap().get(0), Some(0));
        assert_eq!(row1.u64().unwrap().get(4), Some(4));

        let row2 = list.get(2).unwrap();
        assert_eq!(row2.len(), 0);
    }

    #[test]
    fn test_seq_negative_errors() {
        let n = Int64Array::from_values("n", [-1i64]).into_series();
        let ctx = EvalContext { row_count: n.len() };
        let seq = ListSeq;
        let args = FunctionArgs::new_unnamed(vec![n]);
        let result = seq.call(args, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_seq_single() {
        let n = Int64Array::from_values("n", [1i64]).into_series();
        let ctx = EvalContext { row_count: n.len() };
        let seq = ListSeq;
        let args = FunctionArgs::new_unnamed(vec![n]);
        let result = seq.call(args, &ctx).unwrap();

        let list = result.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 1);
        assert_eq!(row0.u64().unwrap().get(0), Some(0));
    }
}
