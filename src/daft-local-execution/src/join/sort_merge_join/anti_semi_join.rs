use std::sync::Arc;

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_micropartition::MicroPartition;

use super::{
    SortMergeJoinParams, SortMergeJoinProbeState, compute_probe_slice_len, evict_build_blocks,
    find_overlapping_build_blocks, get_keys, load_build_blocks,
    remove_or_split_build_blocks_after_join,
};
use crate::join::join_operator::ProbeOutput;

/// Probe for Semi sort-merge join.
///
/// Semi join returns build (left) rows that have at least one match in the
/// probe (right) side. Evicted and remaining build blocks are discarded
/// (they have no probe match).
pub(crate) fn probe_semi(
    params: &SortMergeJoinParams,
    input: Arc<MicroPartition>,
    mut state: SortMergeJoinProbeState,
) -> DaftResult<(SortMergeJoinProbeState, ProbeOutput)> {
    let mut current_input = input;
    let mut results: Vec<Arc<MicroPartition>> = Vec::new();

    while !current_input.is_empty() {
        let probe_keys = get_keys(&current_input, &params.right_on)?;

        // 1. Evict build blocks below probe range — discard (semi join).
        let _evicted = evict_build_blocks(params, &mut state, &probe_keys)?;

        // 2. Load more build blocks.
        load_build_blocks(params, &mut state, &probe_keys)?;

        if state.buffer.is_empty() {
            state.exhausted = true;
            break; // No more build data — no matches possible.
        }

        // 3. Slice probe to what the buffer can cover.
        let slice_len = compute_probe_slice_len(params, &state, &probe_keys)?;
        let input_slice = Arc::new(current_input.slice(0, slice_len)?);
        let input_remainder = current_input.slice(slice_len, current_input.len())?;

        // 4. Join overlapping blocks with Semi type.
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
                JoinType::Semi,
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

/// Probe for Anti sort-merge join.
///
/// Anti join returns build (left) rows that have NO match in the probe (right)
/// side. The strategy:
/// - Evicted build blocks (keys < probe range) → all rows are unmatched → emit
/// - Overlapping blocks → merge_anti_join returns unmatched build rows
/// - Remaining build blocks at finalize → all rows are unmatched → emit
pub(crate) fn probe_anti(
    params: &SortMergeJoinParams,
    input: Arc<MicroPartition>,
    mut state: SortMergeJoinProbeState,
) -> DaftResult<(SortMergeJoinProbeState, ProbeOutput)> {
    let mut current_input = input;
    let mut results: Vec<Arc<MicroPartition>> = Vec::new();

    while !current_input.is_empty() {
        let probe_keys = get_keys(&current_input, &params.right_on)?;

        // 1. Evict build blocks below probe range — emit all (anti join: no match = output).
        let evicted = evict_build_blocks(params, &mut state, &probe_keys)?;
        for block in &evicted {
            if !block.is_empty() {
                // Anti join output schema = left schema (build side only).
                results.push(Arc::new(MicroPartition::new_loaded(
                    params.output_schema.clone(),
                    Arc::new(block.record_batches().to_vec()),
                    None,
                )));
            }
        }

        // 2. Load more build blocks.
        load_build_blocks(params, &mut state, &probe_keys)?;

        if state.buffer.is_empty() {
            state.exhausted = true;
            // Build exhausted. Anti join only returns build rows, not probe rows.
            break;
        }

        // 3. Slice probe to what the buffer can cover.
        let slice_len = compute_probe_slice_len(params, &state, &probe_keys)?;
        let input_slice = Arc::new(current_input.slice(0, slice_len)?);
        let input_remainder = current_input.slice(slice_len, current_input.len())?;

        // 4. Join overlapping blocks with Anti type.
        //    merge_anti_join returns build rows that DON'T match any probe row.
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
                JoinType::Anti,
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

/// Finalize for Anti sort-merge join.
///
/// Streams remaining build blocks (in buffer + iterator) — all rows are unmatched.
/// Each block is sent individually to the output channel so that memory can be
/// released incrementally (prevents OOM for large build-side data).
pub(crate) async fn finalize_anti(
    _params: &SortMergeJoinParams,
    state: SortMergeJoinProbeState,
    sender: &crate::channel::Sender<Arc<MicroPartition>>,
    runtime_stats: &Arc<dyn crate::runtime_stats::RuntimeStats>,
) -> DaftResult<()> {
    let SortMergeJoinProbeState {
        buffer, iterator, ..
    } = state;

    for block in &buffer {
        if !block.is_empty() {
            runtime_stats.add_rows_out(block.len() as u64);
            let _ = sender.send(block.clone()).await;
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
                    runtime_stats.add_rows_out(block.len() as u64);
                    let _ = sender.send(block).await;
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    Ok(())
}
