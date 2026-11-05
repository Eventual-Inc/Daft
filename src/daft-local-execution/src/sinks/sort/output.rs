//! Keep final merge reclamation live even when the pipeline stops polling its output.

use tokio::sync::{mpsc, oneshot};

use super::*;

fn spill_error(error: impl std::fmt::Display) -> DaftError {
    DaftError::ComputeError(error.to_string())
}

struct OutputHandoff {
    partition: MicroPartition,
    memory: Option<MemoryPermit>,
}

/// Selected rows are contiguous within each cursor, even when the merge order
/// alternates between cursors. Slice once per contributing cursor, concatenate
/// those ranges, then apply the merge permutation in one columnar take.
///
/// The admitted workspace covers the concatenated data, final output, selection
/// indices (including Vec capacity growth), and nested take scratch space. No
/// input frame may be replaced until this selection has been materialized.
pub(super) fn materialize_selection(
    cursors: &[MergeCursor],
    starts: &[usize],
    mut selection: Vec<u64>,
) -> DaftResult<RecordBatch> {
    let mut slices = Vec::with_capacity(cursors.len());
    let mut offsets = Vec::with_capacity(cursors.len());
    let mut offset = 0;
    for (cursor, &start) in cursors.iter().zip(starts) {
        offsets.push(offset);
        if cursor.row > start {
            slices.push(
                cursor
                    .batch
                    .as_ref()
                    .expect("selected cursor is loaded")
                    .slice(start, cursor.row)?,
            );
            offset += (cursor.row - start) as u64;
        }
    }
    debug_assert_eq!(offset as usize, selection.len());
    for index in &mut selection {
        let next = &mut offsets[*index as usize];
        *index = *next;
        *next += 1;
    }
    let grouped = RecordBatch::concat(&slices)?;
    drop(slices);
    grouped.take(&UInt64Array::from_vec("", selection))
}

/// A single run is already ordered. Size contiguous output batches column-wise,
/// without row comparisons, a tournament, or per-row descriptor accounting.
/// The cursor and prepaid workspace stay live across each output handoff, just
/// as in a multi-run merge; reclaim_output can drain this stream to disk.
pub(super) fn single_run_output(
    mut cursor: MergeCursor,
    schema: SchemaRef,
    params: Arc<SortParams>,
    spawner: ExecutionTaskSpawner,
    workspace: MemoryPermit,
) -> SortStream {
    Box::pin(async_stream::try_stream! {
        let working_bytes = workspace.bytes();
        let output_bytes = working_bytes / 3;
        let _workspace = workspace;
        loop {
            if cursor.exhausted() && !cursor.load_next(&params, &spawner).await? {
                break;
            }
            let batch = cursor.batch.as_ref().expect("loaded sort cursor");
            let rows = run_builder::select_rows(
                &cursor.arrays, cursor.row, batch.len() - cursor.row, working_bytes,
            )?;
            if run_builder::selection_working_bytes(&cursor.arrays, cursor.row, rows)? > working_bytes {
                Err(DaftError::ComputeError("Sort row exceeds its admitted working set".to_string()))?;
            }
            let owned = if cursor.row == 0 && rows == batch.len() {
                // Transfer a whole batch without copying. No cursor view of its
                // buffers remains, but its permit stays live until the next poll.
                cursor.keys = None;
                cursor.arrays.clear();
                cursor.batch.take().unwrap()
            } else {
                // Partial output must not keep the entire retained run alive
                // downstream after a cooperative spill releases this cursor.
                let slice = batch.slice(cursor.row, cursor.row + rows)?;
                let indices = UInt64Array::from_values("", 0..rows as u64);
                let owned = slice.take(&indices)?;
                cursor.row += rows;
                owned
            };
            if batch_bytes(&owned)? > output_bytes {
                Err(DaftError::ComputeError("Sort output exceeds its materialization budget".to_string()))?;
            }
            yield MicroPartition::new_loaded(schema.clone(), Arc::new(vec![owned]), None);
        }
    })
}

/// The producer only advances on demand. While the consumer is blocked downstream,
/// it can drain the remaining merge to disk and release the entire merge admission.
/// Dropping the output stream cancels the producer through its RuntimeTask handle.
pub(super) fn reclaim_output(
    mut stream: SortStream,
    schema: SchemaRef,
    scope: SpillScopeId,
    spawner: ExecutionTaskSpawner,
    reclaim_bytes: u64,
) -> SortStream {
    let mut target = spawner.register_release_target("sort-output");
    target.set_reclaim_bytes(reclaim_bytes);
    let (requests, mut demand) = mpsc::channel::<oneshot::Sender<Option<OutputHandoff>>>(1);
    let task = spawner.clone().spawn(
        async move {
            let mut files = VecDeque::new();
            let mut read_bytes = 0;
            loop {
                tokio::select! {
                    biased;
                    request = target.recv() => {
                        let result = async {
                            let mut writer = SpillStreamWriter::new(
                                spawner.spill_manager.clone(), scope.clone(), schema.clone(),
                            );
                            while let Some(partition) = stream.next().await {
                                writer = writer.append(partition?).await.map_err(spill_error)?;
                            }
                            (files, read_bytes) = writer.finish().await.map_err(spill_error)?;
                            Ok::<_, DaftError>(())
                        }.await;
                        drop(stream);
                        target.withdraw();
                        if let Err(error) = result {
                            request.fail(error.to_string());
                            return Err(error);
                        }
                        request.complete(reclaim_bytes);
                        break;
                    }
                    next = demand.recv() => {
                        let Some(reply) = next else { return Ok(()) };
                        match stream.next().await.transpose()? {
                            Some(partition) => {
                                if reply.send(Some(OutputHandoff { partition, memory: None })).is_err() {
                                    return Ok(())
                                }
                            }
                            None => {
                                drop(stream);
                                target.withdraw();
                                let _ = reply.send(None);
                                return Ok(());
                            }
                        }
                    }
                }
            }

            // Disk output is already sorted. Reopen at a saved frame boundary for each
            // handoff, so neither a reader nor a prepaid pool survives downstream backpressure.
            let mut position = None;
            while let Some(reply) = demand.recv().await {
                let partition = loop {
                    let Some(file) = files.front() else {
                        break None;
                    };
                    // Reserve schema and frame together, avoiding hold-and-wait during decoding.
                    let memory = spawner
                        .reserve_memory(read_bytes.saturating_add(64 * 1024))
                        .await?;
                    let reader_spawner =
                        spawner.with_memory_pool(memory.into_pool("sort-output-read"));
                    let mut reader = spawner
                        .spill_manager
                        .open_micropartitions(file, |bytes| {
                            reserve_merge_memory(&reader_spawner, bytes)
                        })
                        .await
                        .map_err(spill_error)?;
                    if let Some(position) = position {
                        reader.seek_to(position).map_err(spill_error)?;
                    }
                    match reader
                        .next_batch(|bytes| reserve_merge_memory(&reader_spawner, bytes))
                        .await
                        .map_err(spill_error)?
                    {
                        Some((batch, memory, mut reader)) => {
                            position = Some(reader.position().map_err(spill_error)?);
                            drop(reader);
                            break Some(OutputHandoff {
                                partition: MicroPartition::new_loaded(
                                    schema.clone(),
                                    Arc::new(vec![batch]),
                                    None,
                                ),
                                memory: Some(memory),
                            });
                        }
                        None => {
                            files.pop_front();
                            position = None;
                        }
                    }
                };
                let finished = partition.is_none();
                if reply.send(partition).is_err() || finished {
                    return Ok(());
                }
            }
            Ok(())
        },
        Span::current(),
    );

    Box::pin(async_stream::try_stream! {
        // RuntimeTask aborts on drop, including cancellation while spilling or reading.
        let task = task;
        loop {
            let (reply, response) = oneshot::channel();
            if requests.send(reply).await.is_err() { break }
            match response.await {
                Ok(Some(OutputHandoff { partition, memory })) => {
                    // Keep restored output accounted while a downstream send is blocked.
                    yield partition;
                    drop(memory);
                }
                Ok(None) | Err(_) => break,
            }
        }
        task.await??;
    })
}
