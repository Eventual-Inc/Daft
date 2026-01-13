use std::sync::{Arc, LazyLock};

use arrow_schema::DataType;
use common_error::{DaftError, DaftResult};
use serde_arrow::{
    schema::{SchemaLike, TracingOptions},
    utils::{Item, Items},
};
use sketches_ddsketch::DDSketch;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to deserialize from arrow"))]
    DeserializationError { source: serde_arrow::Error },
}

impl From<Error> for DaftError {
    fn from(value: Error) -> Self {
        use Error::DeserializationError;
        match value {
            DeserializationError { source } => {
                Self::ComputeError(format!("Deserialization error: {source}"))
            }
        }
    }
}

// Expected to be a vector of length 1
static ARROW_DDSKETCH_ITEM_FIELDS: LazyLock<Vec<arrow_schema::FieldRef>> = LazyLock::new(|| {
    Vec::<arrow_schema::FieldRef>::from_type::<Item<Option<DDSketch>>>(TracingOptions::default())
        .unwrap()
});

/// The corresponding Arrow DataType of Vec<DDSketch> when serialized as an Arrow array
pub static ARROW_DDSKETCH_DTYPE: LazyLock<arrow_schema::DataType> = LazyLock::new(|| {
    ARROW_DDSKETCH_ITEM_FIELDS
        .first()
        .unwrap()
        .data_type()
        .clone()
});

static ARROW_DDSKETCH_FIELDS: LazyLock<arrow_schema::Fields> = LazyLock::new(|| {
    let DataType::Struct(fields) = ARROW_DDSKETCH_DTYPE.clone() else {
        panic!("Expected StructDataType");
    };

    fields
});

/// Converts a Vec<Option<DDSketch>> into an Arrow Array
#[must_use]
pub fn into_arrow(sketches: Vec<Option<DDSketch>>) -> arrow_array::ArrayRef {
    if sketches.is_empty() {
        return Arc::new(arrow_array::StructArray::new_null(
            ARROW_DDSKETCH_FIELDS.clone(),
            0,
        ));
    }

    let wrapped_sketches: Items<Vec<Option<DDSketch>>> = Items(sketches);
    let mut arrow_arrays =
        serde_arrow::to_arrow(ARROW_DDSKETCH_ITEM_FIELDS.as_slice(), &wrapped_sketches).unwrap();

    arrow_arrays.pop().unwrap()
}

/// Converts an Arrow Array into a Vec<Option<DDSketch>>
pub fn from_arrow(arrow_array: arrow_array::ArrayRef) -> DaftResult<Vec<Option<DDSketch>>> {
    if arrow_array.is_empty() {
        return Ok(vec![]);
    }

    let item_vec = serde_arrow::from_arrow::<Vec<Item<Option<DDSketch>>>, _>(
        &ARROW_DDSKETCH_ITEM_FIELDS,
        &[arrow_array],
    );
    item_vec
        .map(|item_vec| item_vec.into_iter().map(|item| item.0).collect())
        .with_context(|_| DeserializationSnafu {})
        .map_err(std::convert::Into::into)
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use sketches_ddsketch::{Config, DDSketch};

    use crate::{from_arrow, into_arrow};

    #[test]
    fn test_roundtrip_single() -> DaftResult<()> {
        let mut sketch = DDSketch::new(Config::default());

        for i in 0..10 {
            sketch.add(f64::from(i));
        }

        let expected_min = sketch.min();
        let expected_max = sketch.max();
        let expected_sum = sketch.sum();
        let expected_count = sketch.count();
        let expected_length = sketch.length();
        let expected_quantile = sketch.quantile(0.5);

        let sketches = vec![Some(sketch)];
        let mut round_tripped = from_arrow(into_arrow(sketches))?;

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
        let mut round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 1);
        assert!(round_tripped.pop().unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_roundtrip_some_null() -> DaftResult<()> {
        let sketches = vec![Some(DDSketch::new(Config::default())), None];
        let mut round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 2);

        assert!(round_tripped.pop().unwrap().is_none());
        assert!(round_tripped.pop().unwrap().is_some());
        Ok(())
    }

    #[test]
    fn test_roundtrip_empty() -> DaftResult<()> {
        let sketches = vec![];
        let round_tripped = from_arrow(into_arrow(sketches))?;
        assert_eq!(round_tripped.len(), 0);
        Ok(())
    }
}
