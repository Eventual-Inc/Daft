use arrow2::array::Array;
use common_error::{DaftError, DaftResult};
use lazy_static::lazy_static;
use serde_arrow::{
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::{Item, Items},
};
use sketches_ddsketch::DDSketch;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to deserialize from arrow2"))]
    DeserializationError { source: serde_arrow::Error },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        use Error::*;
        match value {
            DeserializationError { source } => {
                DaftError::ComputeError(format!("Deserialization error: {}", source))
            }
        }
    }
}

lazy_static! {
    static ref ARROW2_DDSKETCH_ITEM_FIELDS: Vec<arrow2::datatypes::Field> =
        SerdeArrowSchema::from_type::<Item<Option<DDSketch>>>(TracingOptions::default())
            .unwrap()
            .to_arrow2_fields()
            .unwrap();

    /// The corresponding arrow2 DataType of Vec<DDSketch> when serialized as an arrow2 array
    pub static ref ARROW2_DDSKETCH_DTYPE: arrow2::datatypes::DataType = ARROW2_DDSKETCH_ITEM_FIELDS.first().unwrap().data_type().clone();
}

/// Converts a Vec<Option<DDSketch>> into an arrow2 Array
pub fn into_arrow2(sketches: Vec<Option<DDSketch>>) -> Box<dyn arrow2::array::Array> {
    if sketches.is_empty() {
        return arrow2::array::StructArray::new_empty(ARROW2_DDSKETCH_DTYPE.clone()).to_boxed();
    }

    let wrapped_sketches: Items<Vec<Option<DDSketch>>> = Items(sketches);
    let mut arrow2_arrays =
        serde_arrow::to_arrow2(ARROW2_DDSKETCH_ITEM_FIELDS.as_slice(), &wrapped_sketches).unwrap();

    arrow2_arrays.pop().unwrap()
}

/// Converts an arrow2 Array into a Vec<Option<DDSketch>>
pub fn from_arrow2(
    arrow_array: Box<dyn arrow2::array::Array>,
) -> DaftResult<Vec<Option<DDSketch>>> {
    if arrow_array.is_empty() {
        return Ok(vec![]);
    }

    let item_vec = serde_arrow::from_arrow2::<Vec<Item<Option<DDSketch>>>, _>(
        &ARROW2_DDSKETCH_ITEM_FIELDS,
        &[arrow_array],
    );
    item_vec
        .map(|item_vec| item_vec.into_iter().map(|item| item.0).collect())
        .with_context(|_| DeserializationSnafu {})
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use crate::{from_arrow2, into_arrow2};
    use common_error::DaftResult;
    use sketches_ddsketch::{Config, DDSketch};

    #[test]
    fn test_roundtrip_single() -> DaftResult<()> {
        let mut sketch = DDSketch::new(Config::default());

        for i in 0..10 {
            sketch.add(i as f64);
        }

        let expected_min = sketch.min();
        let expected_max = sketch.max();
        let expected_sum = sketch.sum();
        let expected_count = sketch.count();
        let expected_length = sketch.length();
        let expected_quantile = sketch.quantile(0.5);

        let sketches = vec![Some(sketch)];
        let mut round_tripped = from_arrow2(into_arrow2(sketches))?;

        assert_eq!(round_tripped.len(), 1);
        let received = round_tripped.pop().unwrap().unwrap();
        assert_eq!(received.min(), expected_min);
        assert_eq!(received.max(), expected_max);
        assert_eq!(received.sum(), expected_sum);
        assert_eq!(received.count(), expected_count);
        assert_eq!(received.length(), expected_length);
        assert_eq!(received.quantile(0.5).unwrap(), expected_quantile.unwrap());

        Ok(())
    }

    #[test]
    fn test_roundtrip_null() -> DaftResult<()> {
        let sketches = vec![None];
        let mut round_tripped = from_arrow2(into_arrow2(sketches))?;
        assert_eq!(round_tripped.len(), 1);
        assert!(round_tripped.pop().unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_roundtrip_some_null() -> DaftResult<()> {
        let sketches = vec![Some(DDSketch::new(Config::default())), None];
        let mut round_tripped = from_arrow2(into_arrow2(sketches))?;
        assert_eq!(round_tripped.len(), 2);

        assert!(round_tripped.pop().unwrap().is_none());
        assert!(round_tripped.pop().unwrap().is_some());
        Ok(())
    }

    #[test]
    fn test_roundtrip_empty() -> DaftResult<()> {
        let sketches = vec![];
        let round_tripped = from_arrow2(into_arrow2(sketches))?;
        assert_eq!(round_tripped.len(), 0);
        Ok(())
    }
}
