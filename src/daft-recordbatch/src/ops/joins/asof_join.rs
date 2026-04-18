use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, Field, UInt64Array},
    kernels::search_sorted::search_sorted,
};

use crate::RecordBatch;

pub fn asof_join_backward(
    left: &RecordBatch,
    right: &RecordBatch,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    if left.num_columns() != right.num_columns() {
        return Err(DaftError::ValueError(format!(
            "Mismatch of asof key columns: left: {:?} vs right: {:?}",
            left.num_columns(),
            right.num_columns()
        )));
    }
    if left.num_columns() == 0 {
        return Err(DaftError::ValueError(
            "No columns were passed in to asof join on".to_string(),
        ));
    }

    let types_not_match = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .any(|(l, r)| l.data_type() != r.data_type());
    if types_not_match {
        return Err(DaftError::SchemaMismatch(
            "Types between left and right asof keys do not match".to_string(),
        ));
    }

    if left.is_empty() {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
        ));
    }
    if right.is_empty() {
        return Ok((
            UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect()),
            UInt64Array::full_null("right_indices", &DataType::UInt64, left.len()),
        ));
    }

    let left_on = left.get_column(0);
    let right_on = right.get_column(0);

    let left_on_arrow = left_on.to_arrow()?;
    let right_on_arrow = right_on.to_arrow()?;

    let positions = search_sorted(right_on_arrow.as_ref(), left_on_arrow.as_ref(), false)?;

    let left_indices = UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect());
    let right_indices = UInt64Array::from_iter(
        Field::new("right_indices", DataType::UInt64),
        (0..left.len()).map(|i| {
            if !left_on_arrow.is_valid(i) {
                return None;
            }
            let pos = positions.value(i) as usize;
            if pos > 0 {
                Some((pos - 1) as u64)
            } else {
                None
            }
        }),
    );
    Ok((left_indices, right_indices))
}
