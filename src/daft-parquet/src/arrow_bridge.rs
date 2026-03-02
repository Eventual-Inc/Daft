use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch as ArrowRecordBatch},
    compute::cast,
};
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_recordbatch::RecordBatch;

/// Cast an arrow-rs array to the type Daft expects, if needed.
///
/// The arrow-rs parquet reader produces small-offset types (Utf8, Binary, List)
/// while Daft expects large-offset types (LargeUtf8, LargeBinary, LargeList).
/// This function casts to the expected type when there's a mismatch.
/// For extension types (Tensor, etc.) where `to_arrow()` is unsupported,
/// the array is returned as-is and downstream conversion handles it.
fn coerce_to_daft_compatible(array: &ArrayRef, daft_field: &Field) -> DaftResult<ArrayRef> {
    let expected = match daft_field.dtype.to_arrow() {
        Ok(dt) => dt,
        Err(_) => return Ok(array.clone()),
    };
    if array.data_type() == &expected {
        return Ok(array.clone());
    }
    cast(array.as_ref(), &expected).map_err(|e| {
        common_error::DaftError::ComputeError(format!(
            "Failed to cast column '{}' from {:?} to {:?}: {}",
            daft_field.name,
            array.data_type(),
            expected,
            e
        ))
    })
}

/// Convert an arrow-rs `RecordBatch` to a Daft `RecordBatch`.
///
/// Each column in the arrow-rs batch is paired with the corresponding field from the
/// Daft schema (matched by column index), cast to the expected Daft arrow type if needed,
/// and converted via `Series::from_arrow`.
/// The Daft schema must have the same number of fields as the arrow-rs batch columns.
pub fn arrowrs_to_daft_recordbatch(
    arrow_rb: &ArrowRecordBatch,
    daft_schema: &Schema,
) -> DaftResult<RecordBatch> {
    let num_rows = arrow_rb.num_rows();

    if daft_schema.len() != arrow_rb.num_columns() {
        return Err(common_error::DaftError::SchemaMismatch(format!(
            "Arrow-rs RecordBatch has {} columns but Daft schema has {} fields",
            arrow_rb.num_columns(),
            daft_schema.len(),
        )));
    }

    let columns: Vec<Series> = daft_schema
        .into_iter()
        .zip(arrow_rb.columns())
        .map(|(field, array)| {
            let coerced = coerce_to_daft_compatible(array, field)?;
            Series::from_arrow(Arc::new(field.clone()), coerced)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(RecordBatch::new_unchecked(
        Arc::new(daft_schema.clone()),
        columns,
        num_rows,
    ))
}

/// Convert multiple arrow-rs `RecordBatch`es into a single Daft `RecordBatch`
/// by converting each batch and concatenating the results.
pub fn arrowrs_batches_to_daft_recordbatch(
    arrow_batches: &[ArrowRecordBatch],
    daft_schema: &Schema,
) -> DaftResult<RecordBatch> {
    if arrow_batches.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(daft_schema.clone()))));
    }

    if arrow_batches.len() == 1 {
        return arrowrs_to_daft_recordbatch(&arrow_batches[0], daft_schema);
    }

    let daft_batches: Vec<RecordBatch> = arrow_batches
        .iter()
        .map(|batch| arrowrs_to_daft_recordbatch(batch, daft_schema))
        .collect::<DaftResult<Vec<_>>>()?;

    RecordBatch::concat(daft_batches.iter().collect::<Vec<_>>().as_slice())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, RecordBatch as ArrowRecordBatch, StringArray},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    };
    use daft_core::prelude::*;

    use super::*;

    #[test]
    fn test_basic_conversion() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, true),
            ArrowField::new("b", ArrowDataType::Utf8, true),
        ]));

        let arrow_rb = ArrowRecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let daft_schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Utf8),
        ]);

        let result = arrowrs_to_daft_recordbatch(&arrow_rb, &daft_schema).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_empty_batch_conversion() {
        let daft_schema = Schema::new(vec![Field::new("a", DataType::Int32)]);
        let result = arrowrs_batches_to_daft_recordbatch(&[], &daft_schema).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_schema_mismatch_error() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            ArrowDataType::Int32,
            true,
        )]));

        let arrow_rb =
            ArrowRecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();

        let daft_schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Utf8),
        ]);

        let result = arrowrs_to_daft_recordbatch(&arrow_rb, &daft_schema);
        assert!(result.is_err());
    }
}
