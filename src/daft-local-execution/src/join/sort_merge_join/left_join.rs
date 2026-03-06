use std::sync::Arc;

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_micropartition::MicroPartition;

use super::{
    SortMergeJoinParams, SortMergeJoinProbeState, compute_probe_slice_len,
    emit_unmatched_build_as_left_or_outer, evict_build_blocks, find_overlapping_build_blocks,
    get_keys, load_build_blocks, remove_or_split_build_blocks_after_join,
};
use crate::join::join_operator::ProbeOutput;

/// Probe for Left sort-merge join.
///
/// Left join preserves all build-side (left) rows. Matched rows come from the
/// merge join against the probe slice. Unmatched build rows are emitted:
/// - At eviction time (build blocks whose keys < current probe range)
/// - At finalize time (remaining build blocks after all probes)
///
/// The block-splitting mechanism ensures no double-counting: rows consumed by
/// a join are removed from the buffer, so eviction/finalize only see rows that
/// were never part of any overlapping join.
pub(crate) fn probe_left(
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
            // No build data left — no matches possible for remaining probe rows.
            // Left join only preserves build (left) rows, not probe (right) rows.
            break;
        }

        // 3. Slice probe to what the buffer can cover.
        let slice_len = compute_probe_slice_len(params, &state, &probe_keys)?;
        let input_slice = Arc::new(current_input.slice(0, slice_len)?);
        let input_remainder = current_input.slice(slice_len, current_input.len())?;

        // 4. Join overlapping blocks with Left type.
        //    merge_left_join preserves all left (build) rows in the overlapping
        //    range, emitting NULLs for unmatched build rows. After splitting,
        //    those rows are removed from the buffer so they won't be re-emitted.
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
                JoinType::Left,
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

/// Finalize for Left sort-merge join.
///
/// Streams remaining build blocks (in buffer + iterator) as unmatched rows with
/// NULLs on the right side. Each block is sent individually to the output channel
/// so that memory can be released incrementally (prevents OOM for large build-side data).
pub(crate) async fn finalize_left(
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
    // Drop buffer so its blocks can be freed before draining the iterator.
    drop(buffer);

    // Drain the build iterator one block at a time.
    // We must not hold the MutexGuard across .await, so we lock/unlock per iteration.
    loop {
        let next_block = {
            let mut guard = iterator.lock().unwrap();
            guard.next()
        }; // guard dropped here
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
