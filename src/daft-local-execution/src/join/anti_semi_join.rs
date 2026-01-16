use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{IntoSeries, UInt64Array};
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, RecordBatch};
use futures::{StreamExt, stream};

use crate::join::{
    hash_join::{HashJoinParams, HashJoinProbeState},
    index_bitmap::IndexBitmapBuilder,
};

pub(crate) fn probe_anti_semi(
    input: &Arc<MicroPartition>,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<Arc<MicroPartition>> {
    let is_semi = params.join_type == JoinType::Semi;

    let input_tables = input.record_batches();
    let mut input_idxs = vec![vec![]; input_tables.len()];
    for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
        let join_keys = table.eval_expression_list(&params.probe_on)?;
        let iter = probe_state.probe_exists(&join_keys)?;

        for (probe_row_idx, matched) in iter.enumerate() {
            // 1. If this is a semi join, we keep the row if it matches.
            // 2. If this is an anti join, we keep the row if it doesn't match.
            match (is_semi, matched) {
                (true, true) | (false, false) => {
                    input_idxs[probe_side_table_idx].push(probe_row_idx as u64);
                }
                _ => {}
            }
        }
    }
    let probe_side_tables = input_idxs
        .into_iter()
        .zip(input_tables.iter())
        .map(|(idxs, table)| {
            let idxs_arr = UInt64Array::from(("idxs", idxs));
            table.take(&idxs_arr)
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(Arc::new(MicroPartition::new_loaded(
        probe_side_tables[0].schema.clone(),
        Arc::new(probe_side_tables),
        None,
    )))
}

pub(crate) fn probe_anti_semi_with_bitmap(
    input: &Arc<MicroPartition>,
    bitmap_builder: &mut IndexBitmapBuilder,
    probe_state: &ProbeState,
    params: &HashJoinParams,
) -> DaftResult<()> {
    for table in input.record_batches() {
        let join_keys = table.eval_expression_list(&params.probe_on)?;
        let idx_iter = probe_state.probe_indices(&join_keys)?;

        for inner_iter in idx_iter.flatten() {
            for (build_table_idx, build_row_idx) in inner_iter {
                bitmap_builder.mark_used(build_table_idx as usize, build_row_idx as usize);
            }
        }
    }
    Ok(())
}

pub(crate) async fn finalize_anti_semi(
    states: Vec<HashJoinProbeState>,
    is_semi: bool,
) -> DaftResult<Option<Arc<MicroPartition>>> {
    let mut states_iter = states.into_iter();
    let first_state = states_iter
        .next()
        .expect("At least one state should be present for anti/semi join finalize");
    let first_tables = first_state.probe_state.get_record_batches();
    let first_bitmap = first_state
        .bitmap_builder
        .expect("Bitmap builder should be set for anti/semi join finalize")
        .build();

    let mut merged_bitmap = {
        let bitmaps = stream::once(async move { first_bitmap })
            .chain(stream::iter(states_iter).map(|s| {
                s.bitmap_builder
                    .expect("Bitmap builder should be set for anti/semi join finalize")
                    .build()
            }))
            .collect::<Vec<_>>()
            .await;

        bitmaps.into_iter().fold(None, |acc, x| match acc {
            None => Some(x),
            Some(acc) => Some(acc.merge(&x)),
        })
    }
    .expect("At least one bitmap should be present for anti/semi join finalize");

    // The bitmap marks matched rows as 0, so we need to negate it if we are doing semi join, i.e. the matched rows become 1 so that
    // we can we can keep them in the final result.
    if is_semi {
        merged_bitmap = merged_bitmap.negate();
    }

    let leftovers = merged_bitmap
        .convert_to_boolean_arrays()
        .zip(first_tables.iter())
        .map(|(bitmap, table)| table.mask_filter(&bitmap.into_series()))
        .collect::<DaftResult<Vec<_>>>()?;
    let build_side_table = RecordBatch::concat(&leftovers)?;

    Ok(Some(Arc::new(MicroPartition::new_loaded(
        build_side_table.schema.clone(),
        Arc::new(vec![build_side_table]),
        None,
    ))))
}
