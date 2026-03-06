use std::sync::Arc;

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_micropartition::MicroPartition;

use super::{
    SortMergeJoinParams, SortMergeJoinProbeState, compute_probe_slice_len,
    emit_unmatched_probe_as_right_or_outer, evict_build_blocks, find_overlapping_build_blocks,
    get_keys, load_build_blocks, remove_or_split_build_blocks_after_join,
};
use crate::join::join_operator::ProbeOutput;

/// Probe for Right sort-merge join.
///
/// Right join preserves all probe-side (right) rows. Matched rows come from
/// merge_right_join. Unmatched probe rows (no build match) are emitted inline
/// during probe — no finalization needed for the build side.
///
/// Evicted build blocks are discarded (right join doesn't preserve unmatched
/// build rows).
pub(crate) fn probe_right(
    params: &SortMergeJoinParams,
    input: Arc<MicroPartition>,
    mut state: SortMergeJoinProbeState,
) -> DaftResult<(SortMergeJoinProbeState, ProbeOutput)> {
    let mut current_input = input;
    let mut results: Vec<Arc<MicroPartition>> = Vec::new();

    while !current_input.is_empty() {
        let probe_keys = get_keys(&current_input, &params.right_on)?;

        // 1. Evict build blocks below probe range — discard (right join).
        let _evicted = evict_build_blocks(params, &mut state, &probe_keys)?;

        // 2. Load more build blocks.
        load_build_blocks(params, &mut state, &probe_keys)?;

        if state.buffer.is_empty() {
            state.exhausted = true;
            // No build data — emit remaining probe rows as unmatched.
            results.push(emit_unmatched_probe_as_right_or_outer(
                params,
                &current_input,
            )?);
            break;
        }

        // 3. Slice probe to what the buffer can cover.
        let slice_len = compute_probe_slice_len(params, &state, &probe_keys)?;
        let input_slice = Arc::new(current_input.slice(0, slice_len)?);
        let input_remainder = current_input.slice(slice_len, current_input.len())?;

        // 4. Join overlapping blocks with Right type.
        //    merge_right_join preserves all right (probe) rows.
        if !input_slice.is_empty() {
            let probe_slice_keys = get_keys(&input_slice, &params.right_on)?;
            let overlap = find_overlapping_build_blocks(
                params,
                &state,
                &probe_slice_keys,
                input_slice.len(),
            )?;

            let joined = MicroPartition::sort_merge_join(
                &overlap.left_mp,
                &input_slice,
                &params.left_on,
                &params.right_on,
                JoinType::Right,
                true,
            )?;
            results.push(Arc::new(joined));

            remove_or_split_build_blocks_after_join(
                params,
                &mut state,
                &probe_slice_keys,
                input_slice.len() - 1,
                overlap.start_block,
                overlap.end_block,
            )?;
        }

        current_input = Arc::new(input_remainder);
    }

    Ok((
        state,
        ProbeOutput::NeedMoreInput(Some(
            MicroPartition::concat_or_empty(&results, params.output_schema.clone())?.into(),
        )),
    ))
}
