use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListFill;

#[derive(FunctionArgs)]
struct Args<T> {
    elem: T,
    input: T,
}

#[typetag::serde]
impl ScalarUDF for ListFill {
    fn name(&self) -> &'static str {
        "list_fill"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { elem, input: num } = inputs.try_into()?;

        let num = num.cast(&DataType::Int64)?;
        let num_array = num.i64()?;
        elem.list_fill(num_array)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { elem, input: n } = inputs.try_into()?;

        let num_field = n.to_field(schema)?;
        let elem_field = elem.to_field(schema)?;

        if !num_field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected num field to be of numeric type, received: {}",
                num_field.dtype
            )));
        }
        Ok(elem_field.to_list_field())
    }
}

#[must_use]
pub fn list_fill(elem: ExprRef, n: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListFill {}, vec![elem, n]).into()
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use daft_arrow::offset::OffsetsBuffer;
    use daft_core::{
        array::ListArray,
        datatypes::{Int8Array, Utf8Array},
        series::IntoSeries,
    };
    use daft_dsl::{lit, null_lit};

    use super::*;

    #[test]
    fn test_to_field() {
        let col0_null = null_lit().alias("c0");
        let col0_num = lit(10).alias("c0");
        let col1_null = null_lit().alias("c1");
        let col1_str = lit("abc").alias("c1");

        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int32),
            Field::new("c1", DataType::Utf8),
        ]);

        let fill = ListFill {};
        let args = FunctionArgs::new_unnamed(vec![col1_str.clone(), col0_null.clone()]);
        let DaftError::TypeError(e) = fill.get_return_field(args, &schema).unwrap_err() else {
            panic!("Expected TypeError error");
        };
        assert_eq!(
            e,
            "Expected num field to be of numeric type, received: Null"
        );
        let args = FunctionArgs::new_unnamed(vec![col1_str.clone(), col0_null]);
        let DaftError::TypeError(e) = fill.get_return_field(args, &schema).unwrap_err() else {
            panic!("Expected TypeError error");
        };
        assert_eq!(
            e,
            "Expected num field to be of numeric type, received: Null"
        );

        let args = FunctionArgs::new_unnamed(vec![col1_null, col0_num.clone()]);

        let list_of_null = fill.get_return_field(args, &schema).unwrap();
        let expected = Field::new("c1", DataType::List(Box::new(DataType::Null)));
        assert_eq!(list_of_null, expected);
        let args = FunctionArgs::new_unnamed(vec![col1_str, col0_num]);

        let list_of_str = fill.get_return_field(args, &schema).unwrap();
        let expected = Field::new("c1", DataType::List(Box::new(DataType::Utf8)));
        assert_eq!(list_of_str, expected);
    }

    #[test]
    fn test_evaluate_with_invalid_input() {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(10)].into_iter(),
        )
        .into_series();

        let args = FunctionArgs::new_unnamed(vec![num]);

        let error = fill.call(args).unwrap_err();
        assert_eq!(
            error.to_string(),
            "DaftError::ValueError Required argument `input` not found"
        );
    }

    #[test]
    fn test_evaluate_mismatched_len() {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(10), Some(11), Some(7)].into_iter(),
        )
        .into_series();
        let str = Utf8Array::from_iter("s2", vec![None, Some("hello"), Some("world")].into_iter())
            .into_series();
        let args = FunctionArgs::new_unnamed(vec![str, num]);
        let error =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fill.call(args).unwrap()));
        assert!(error.is_err());
    }

    #[test]
    fn test_evaluate() -> DaftResult<()> {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(3)].into_iter(),
        )
        .into_series();
        let str = Utf8Array::from_iter("s2", vec![None, Some("hello"), Some("world")].into_iter())
            .into_series();
        let args = FunctionArgs::new_unnamed(vec![str, num]);
        let result = fill.call(args)?;
        // the expected result should be a list of strings: [[None], [], ["world", "world", "world"]]
        let flat_child = Utf8Array::from_iter(
            "s2",
            vec![None, Some("world"), Some("world"), Some("world")].into_iter(),
        )
        .into_series();
        let offsets = vec![0, 1, 1, 4];
        let offsets = OffsetsBuffer::try_from(offsets).unwrap();
        let expected = ListArray::new(
            Field::new("s2", DataType::List(Box::new(DataType::Utf8))),
            flat_child,
            offsets,
            None,
        );
        assert_eq!(result.field(), expected.field.as_ref());
        assert_eq!(result.len(), expected.len());
        let result_list = result.list()?;
        assert_eq!(result_list.offsets(), expected.offsets());
        assert_eq!(result_list.validity(), expected.validity());
        assert_eq!(
            result_list
                .flat_child
                .utf8()?
                .into_iter()
                .collect::<Vec<_>>(),
            expected.flat_child.utf8()?.into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }
}
