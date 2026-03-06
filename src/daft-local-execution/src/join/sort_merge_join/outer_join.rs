use std::sync::Arc;

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_micropartition::MicroPartition;

use super::{
    SortMergeJoinParams, SortMergeJoinProbeState, compute_probe_slice_len,
    emit_unmatched_build_as_left_or_outer, emit_unmatched_probe_as_right_or_outer,
    evict_build_blocks, find_overlapping_build_blocks, get_keys, load_build_blocks,
    remove_or_split_build_blocks_after_join,
};
use crate::join::join_operator::ProbeOutput;

/// Probe for Outer (Full) sort-merge join.
///
/// Outer join preserves ALL rows from both sides. This is the join type that
/// had the ordering bug in the old implementation.
///
/// Use merge_full_join (the REAL join type) for overlapping blocks,
/// which produces matched + unmatched rows from BOTH sides in correct sorted
/// order. Combined with block-splitting, this ensures:
///
/// 1. Evicted blocks (keys < current probe) → emit as unmatched build rows
/// 2. Overlapping blocks → merge_full_join produces correctly ordered output
///    including both unmatched build and unmatched probe rows interleaved
/// 3. Build exhausted → emit remaining probe rows as unmatched
/// 4. Finalize → emit remaining build blocks as unmatched
///
/// Order is maintained because:
/// evicted_build_keys < overlapping_keys < remaining_probe_keys < finalize_build_keys
pub(crate) fn probe_outer(
    params: &SortMergeJoinParams,
    input: Arc<MicroPartition>,
    mut state: SortMergeJoinProbeState,
) -> DaftResult<(SortMergeJoinProbeState, ProbeOutput)> {
    let mut current_input = input;
    let mut results: Vec<Arc<MicroPartition>> = Vec::new();

    while !current_input.is_empty() {
        let probe_keys = get_keys(&current_input, &params.right_on)?;

        // 1. Evict build blocks below probe range — emit as unmatched.
        let evicted = evict_build_blocks(params, &mut state, &probe_keys)?;
        for block in &evicted {
            results.push(emit_unmatched_build_as_left_or_outer(params, block)?);
        }

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

        // 4. Join overlapping blocks with Outer (Full) type.
        //    merge_full_join produces ALL rows (matched + unmatched from both
        //    sides) in correct sorted order. This is the key fix for the
        //    ordering bug: interleaved unmatched rows from both sides are
        //    correctly interleaved in the output.
        if !input_slice.is_empty() {
            let probe_slice_keys = get_keys(&input_slice, &params.right_on)?;
            let overlap = find_overlapping_build_blocks(
                params,
                &state,
                &probe_slice_keys,
                input_slice.len(),
            )?;

            let joined = MicroPartition::sort_merge_join(
                &overlap.left_mp_for_join,
                &input_slice,
                &params.left_on,
                &params.right_on,
                JoinType::Outer,
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

/// Finalize for Outer sort-merge join.
///
/// Streams remaining build blocks (in buffer + iterator) as unmatched rows with
/// NULLs on the right side. Each block is sent individually to the output channel
/// so that memory can be released incrementally (prevents OOM for large build-side data).
pub(crate) async fn finalize_outer(
    params: &SortMergeJoinParams,
    state: SortMergeJoinProbeState,
    sender: &crate::channel::Sender<Arc<MicroPartition>>,
    runtime_stats: &Arc<dyn crate::runtime_stats::RuntimeStats>,
) -> DaftResult<()> {
    let SortMergeJoinProbeState {
        buffer, iterator, ..
    } = state;

    for block in &buffer {
        if !block.is_empty() {
            let mp = emit_unmatched_build_as_left_or_outer(params, block)?;
            runtime_stats.add_rows_out(mp.len() as u64);
            let _ = sender.send(mp).await;
        }
    }
    drop(buffer);

    loop {
        let next_block = {
            let mut guard = iterator.lock().unwrap();
            guard.next()
        };
        match next_block {
            Some(Ok(block)) => {
                if !block.is_empty() {
                    let mp = emit_unmatched_build_as_left_or_outer(params, &block)?;
                    runtime_stats.add_rows_out(mp.len() as u64);
                    let _ = sender.send(mp).await;
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    Ok(())
}
