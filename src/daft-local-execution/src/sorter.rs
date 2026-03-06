use std::{cmp::Ordering, collections::BinaryHeap, fs::File, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch as ArrowRecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_row::{RowConverter, Rows, SortField};
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::spill::{SpillFile, SpillManager};

pub struct SortParams {
    pub sort_by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
    pub nulls_first: Vec<bool>,
}

pub struct ExternalSorter {
    params: Arc<SortParams>,
    spill_manager: Arc<SpillManager>,
    in_memory_buffer: Vec<Arc<MicroPartition>>,
    spill_files: Vec<SpillFile>,
    mem_threshold: u64,
    current_mem: u64,
    schema: SchemaRef,
    spill_batch_size: usize,
}

impl ExternalSorter {
    pub fn new(
        params: Arc<SortParams>,
        spill_manager: Arc<SpillManager>,
        mem_threshold: u64,
        schema: SchemaRef,
        spill_batch_size: usize,
    ) -> Self {
        Self {
            params,
            spill_manager,
            in_memory_buffer: Vec::new(),
            spill_files: Vec::new(),
            mem_threshold,
            current_mem: 0,
            schema,
            spill_batch_size,
        }
    }

    pub fn push(&mut self, part: Arc<MicroPartition>) -> DaftResult<()> {
        let size = part.size_bytes() as u64;
        if self.current_mem + size > self.mem_threshold && !self.in_memory_buffer.is_empty() {
            self.spill()?;
        }
        self.current_mem += size;
        self.in_memory_buffer.push(part);

        if self.current_mem > self.mem_threshold {
            self.spill()?;
        }
        Ok(())
    }

    pub fn spill(&mut self) -> DaftResult<()> {
        if self.in_memory_buffer.is_empty() {
            return Ok(());
        }
        let buffer = std::mem::take(&mut self.in_memory_buffer);
        self.current_mem = 0;
        let concated = MicroPartition::concat(buffer)?;
        let sorted = concated.sort(
            &self.params.sort_by,
            &self.params.descending,
            &self.params.nulls_first,
        )?;
        drop(concated); // Free memory before writing to disk

        let path = self.spill_manager.get_temp_spill_file();
        let spill_file = SpillFile::new(path.clone());
        let file = File::create(&path)?;

        let mut writer = StreamWriter::try_new(file, &sorted.schema().to_arrow()?)?;
        for batch in sorted.record_batches() {
            let mut offset = 0;
            while offset < batch.len() {
                let len = std::cmp::min(self.spill_batch_size, batch.len() - offset);
                let sliced = batch.slice(offset, offset + len)?;
                let arrow_batch: ArrowRecordBatch = sliced.try_into()?;
                writer.write(&arrow_batch)?;
                offset += len;
            }
        }
        writer.finish()?;

        self.spill_files.push(spill_file);
        Ok(())
    }

    pub fn finish(
        mut self,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>> {
        if self.spill_files.is_empty() {
            if self.in_memory_buffer.is_empty() {
                // Return a single empty MicroPartition with the correct schema
                // so downstream operators always receive at least one partition.
                let empty = Arc::new(MicroPartition::empty(Some(self.schema)));
                return Ok(Box::new(std::iter::once(Ok(empty))));
            }
            let concated = MicroPartition::concat(self.in_memory_buffer)?;
            let sorted = concated.sort(
                &self.params.sort_by,
                &self.params.descending,
                &self.params.nulls_first,
            )?;
            return Ok(Box::new(std::iter::once(Ok(Arc::new(sorted)))));
        }

        self.spill()?;

        let iter = MergeIterator::new(
            self.spill_files,
            self.params,
            self.schema,
            self.spill_batch_size,
        )?;
        Ok(Box::new(iter))
    }
}

struct MergeIterator {
    _spill_files: Vec<SpillFile>,
    streams: Vec<StreamReader<File>>,
    heap: BinaryHeap<HeapItem>,
    converter: RowConverter,
    params: Arc<SortParams>,
    schema: SchemaRef,
    current_batches: Vec<Option<RecordBatch>>,
    streams_needing_reload: Vec<usize>,
    batch_size: usize,
}

struct HeapItem {
    stream_idx: usize,
    row_idx: usize,
    rows: Arc<Rows>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap: BinaryHeap is a max-heap, so we reverse to get
        // ascending order output. Sort direction is encoded in the RowConverter,
        // so the byte comparison already accounts for descending/nulls_first.
        self.rows
            .row(self.row_idx)
            .cmp(&other.rows.row(other.row_idx))
            .reverse()
    }
}

/// Convert key columns from a RecordBatch into byte-comparable `Rows` using the given `RowConverter`.
/// The converter encodes sort direction (descending, nulls_first) into the byte representation,
/// so downstream comparisons are simple byte memcmp.
fn keys_to_rows(keys: &RecordBatch, converter: &RowConverter) -> DaftResult<Rows> {
    let arrays: Vec<ArrayRef> = (0..keys.num_columns())
        .map(|i| keys.get_column(i).to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    converter
        .convert_columns(&arrays)
        .map_err(common_error::DaftError::from)
}

impl MergeIterator {
    fn new(
        spill_files: Vec<SpillFile>,
        params: Arc<SortParams>,
        schema: SchemaRef,
        batch_size: usize,
    ) -> DaftResult<Self> {
        let mut streams = Vec::with_capacity(spill_files.len());
        let mut heap = BinaryHeap::with_capacity(spill_files.len());
        let mut current_batches = Vec::with_capacity(spill_files.len());

        // We build the RowConverter lazily from the first batch's key column types,
        // since we need Arrow DataTypes which are only known after evaluating expressions.
        let mut converter: Option<RowConverter> = None;
        let num_keys = params.sort_by.len();

        for (idx, spill_file) in spill_files.iter().enumerate() {
            let file = File::open(spill_file.path())?;
            let mut reader = StreamReader::try_new(file, None)?;

            if let Some(batch) = reader.next() {
                let arrow_batch = batch?;
                let rb = RecordBatch::from_arrow(schema.clone(), arrow_batch.columns().to_vec())?;
                let keys = rb.eval_expression_list(&params.sort_by)?;

                // Build converter on first batch
                let conv = match converter.as_ref() {
                    Some(c) => c,
                    None => {
                        let sort_fields: Vec<SortField> = (0..num_keys)
                            .map(|i| {
                                let arrow_dtype = keys.get_column(i).field().dtype.to_arrow()?;
                                Ok(SortField::new_with_options(
                                    arrow_dtype,
                                    arrow_schema::SortOptions {
                                        descending: params.descending[i],
                                        nulls_first: params.nulls_first[i],
                                    },
                                ))
                            })
                            .collect::<DaftResult<Vec<_>>>()?;
                        converter = Some(
                            RowConverter::new(sort_fields)
                                .map_err(common_error::DaftError::from)?,
                        );
                        converter.as_ref().unwrap()
                    }
                };

                let rows = Arc::new(keys_to_rows(&keys, conv)?);
                heap.push(HeapItem {
                    stream_idx: idx,
                    row_idx: 0,
                    rows,
                });

                current_batches.push(Some(rb));
            } else {
                current_batches.push(None);
            }
            streams.push(reader);
        }

        // If no batches were loaded (all streams empty), create a dummy converter
        let converter = converter.unwrap_or_else(|| RowConverter::new(vec![]).unwrap());

        Ok(Self {
            _spill_files: spill_files,
            streams,
            heap,
            converter,
            params,
            schema,
            current_batches,
            streams_needing_reload: Vec::new(),
            batch_size,
        })
    }

    /// Read the next IPC batch from the given stream, convert its keys to Rows,
    /// and push a new HeapItem. Returns Ok(true) if a batch was loaded, Ok(false)
    /// if the stream is exhausted.
    fn reload_stream(&mut self, stream_idx: usize) -> DaftResult<bool> {
        let Some(arrow_batch) = self.streams[stream_idx].next() else {
            self.current_batches[stream_idx] = None;
            return Ok(false);
        };
        let arrow_batch = arrow_batch?;
        let rb = RecordBatch::from_arrow(self.schema.clone(), arrow_batch.columns().to_vec())?;
        let keys = rb.eval_expression_list(&self.params.sort_by)?;
        let rows = Arc::new(keys_to_rows(&keys, &self.converter)?);
        self.current_batches[stream_idx] = Some(rb);
        self.heap.push(HeapItem {
            stream_idx,
            row_idx: 0,
            rows,
        });
        Ok(true)
    }
}

impl Iterator for MergeIterator {
    type Item = DaftResult<Arc<MicroPartition>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Phase 1: Reload any streams that were deferred from the previous call.
        // This ensures we don't replace current_batches while result_rows still references them.
        let streams_to_reload: Vec<usize> = self.streams_needing_reload.drain(..).collect();
        for stream_idx in streams_to_reload {
            match self.reload_stream(stream_idx) {
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }

        if self.heap.is_empty() {
            return None;
        }

        // Phase 2: Collect rows from the heap up to batch_size.
        // Uses peek-drain optimization: after popping the min item from stream S,
        // compare S's next row against heap.peek() (the min of all OTHER streams).
        // If S's next row <= peek (via byte comparison on pre-computed Rows),
        // emit it directly without push/pop — avoiding O(log K) heap operations
        // for consecutive runs from the same stream.
        let mut result_rows: Vec<(usize, usize)> = Vec::with_capacity(self.batch_size);

        while result_rows.len() < self.batch_size && !self.heap.is_empty() {
            let item = self.heap.pop().unwrap();
            let stream_idx = item.stream_idx;
            let mut current_row = item.row_idx;
            let batch_len = self.current_batches[stream_idx].as_ref().unwrap().len();

            // Output the popped row.
            result_rows.push((stream_idx, current_row));
            current_row += 1;

            // Peek-drain: try to emit consecutive rows from the same stream
            // without going through the heap.
            if current_row < batch_len {
                if self.heap.is_empty() {
                    // Only stream left — drain directly, no comparisons needed.
                    while current_row < batch_len && result_rows.len() < self.batch_size {
                        result_rows.push((stream_idx, current_row));
                        current_row += 1;
                    }
                } else {
                    // Compare current stream's rows against heap peek using pre-computed Rows.
                    // Sort direction is encoded in the RowConverter, so byte comparison is sufficient.
                    let peek = self.heap.peek().unwrap();
                    let peek_row = peek.rows.row(peek.row_idx);

                    // Drain consecutive rows where S[current] <= peek in sort order.
                    // Since sort direction is encoded in the row bytes,
                    // `!= Greater` means "current row should be output before or
                    // at the same position as peek" in the final sorted output.
                    while current_row < batch_len && result_rows.len() < self.batch_size {
                        if item.rows.row(current_row).cmp(&peek_row) == Ordering::Greater {
                            break;
                        }
                        result_rows.push((stream_idx, current_row));
                        current_row += 1;
                    }
                }
            }

            // Push remaining row back into heap, or handle batch exhaustion.
            if current_row < batch_len {
                self.heap.push(HeapItem {
                    stream_idx,
                    row_idx: current_row,
                    rows: item.rows,
                });
            } else {
                // Batch exhausted — defer reload to next call so current_batches
                // stays valid (Phase 3 references them by stream_idx + row_idx).
                self.streams_needing_reload.push(stream_idx);
            }
        }

        if result_rows.is_empty() {
            return None;
        }

        // Phase 3: Build output from result_rows.
        // All referenced batches are still alive in current_batches.
        // Collect unique stream indices and map them to growable batch indices.
        let mut batch_index_map: Vec<Option<usize>> = vec![None; self.streams.len()];
        let mut all_batches: Vec<&RecordBatch> = Vec::new();

        for &(stream_idx, _) in &result_rows {
            if batch_index_map[stream_idx].is_none() {
                batch_index_map[stream_idx] = Some(all_batches.len());
                all_batches.push(self.current_batches[stream_idx].as_ref().unwrap());
            }
        }

        let mut growable =
            match daft_recordbatch::GrowableRecordBatch::new(&all_batches, true, result_rows.len())
            {
                Ok(g) => g,
                Err(e) => return Some(Err(e)),
            };

        // Extend growable with runs of consecutive rows from the same batch
        let mut i = 0;
        while i < result_rows.len() {
            let (stream_idx, start_row_idx) = result_rows[i];
            let batch_idx = batch_index_map[stream_idx].unwrap();
            let mut run_len = 1;
            while i + run_len < result_rows.len() {
                let (next_stream, next_row) = result_rows[i + run_len];
                if next_stream == stream_idx && next_row == start_row_idx + run_len {
                    run_len += 1;
                } else {
                    break;
                }
            }
            growable.extend(batch_idx, start_row_idx, run_len);
            i += run_len;
        }

        match growable.build() {
            Ok(rb) => Some(Ok(Arc::new(MicroPartition::new_loaded(
                self.schema.clone(),
                Arc::new(vec![rb]),
                None,
            )))),
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::{
        datatypes::{Field, Int64Array},
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::{ExternalSorter, SortParams};
    use crate::spill::SpillManager;

    fn make_sort_params(col_name: &str, schema: &Schema) -> Arc<SortParams> {
        let expr = BoundExpr::try_new(resolved_col(col_name), schema).unwrap();
        Arc::new(SortParams {
            sort_by: vec![expr],
            descending: vec![false],
            nulls_first: vec![false],
        })
    }

    fn make_mp(col_name: &str, values: Vec<i64>) -> Arc<MicroPartition> {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        let array = Int64Array::from_vec(col_name, values);
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        Arc::new(MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None))
    }

    fn collect_sorted_values(sorter: ExternalSorter) -> Vec<i64> {
        let iter = sorter.finish().unwrap();
        let mut all_values = Vec::new();
        for res in iter {
            let mp = res.unwrap();
            let rb = mp.concat_or_get().unwrap();
            if let Some(rb) = rb.as_ref() {
                let col = rb.get_column(0);
                for i in 0..col.len() {
                    if let daft_core::lit::Literal::Int64(v) = col.get_lit(i) {
                        all_values.push(v);
                    }
                }
            }
        }
        all_values
    }

    // ========== IN-MEMORY PATH (no spill) ==========

    #[test]
    fn test_sorter_in_memory_single_push() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter.push(make_mp("a", vec![3, 1, 2])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_sorter_in_memory_multiple_pushes() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter.push(make_mp("a", vec![5, 3])).unwrap();
        sorter.push(make_mp("a", vec![1, 4, 2])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_sorter_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        let result = collect_sorted_values(sorter);
        assert!(result.is_empty());
    }

    // ========== SPILL PATH ==========

    #[test]
    fn test_sorter_spill_single_partition() {
        // Set a very low threshold to force spill
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        // threshold=1 forces spill after each push
        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![5, 3, 1])).unwrap();
        sorter.push(make_mp("a", vec![4, 2, 6])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_sorter_spill_multiple_partitions() {
        // Multiple spills result in multi-way merge
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        for i in (0..10).rev() {
            sorter
                .push(make_mp("a", vec![i * 3 + 2, i * 3 + 1, i * 3]))
                .unwrap();
        }

        let result = collect_sorted_values(sorter);
        let expected: Vec<i64> = (0..30).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sorter_spill_with_duplicates() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![3, 1, 1])).unwrap();
        sorter.push(make_mp("a", vec![2, 3, 1])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 1, 1, 2, 3, 3]);
    }

    #[test]
    fn test_sorter_spill_files_cleaned_up() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![3, 1])).unwrap();
        sorter.push(make_mp("a", vec![2, 4])).unwrap();

        // Spill files should exist in the temp dir during sort
        let arrow_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "arrow"))
            .collect();
        assert!(
            !arrow_files.is_empty(),
            "Spill files should exist during sort"
        );

        // Consume the iterator (which holds SpillFile references)
        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4]);

        // After the iterator is fully consumed and dropped, spill files should be cleaned up
        let arrow_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "arrow"))
            .collect();
        assert!(
            arrow_files.is_empty(),
            "Spill files should be cleaned up after iterator consumed"
        );
    }

    // ========== MERGE ITERATOR ==========

    #[test]
    fn test_sorter_large_merge() {
        // Verify the full spill → k-way merge path:
        //   1. Data ranges across spill files overlap, so correct output is
        //      impossible without actual k-way merging via the heap.
        //   2. Each output batch respects the spill_batch_size upper bound.
        //   3. Multiple output batches are produced (merge path, not in-memory).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);
        let spill_batch_size: usize = 8192;

        // mem_threshold=1 forces every push to spill immediately.
        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, spill_batch_size);

        // Push 20 partitions whose value ranges *interleave*:
        //   partition 0: [0, 20, 40, 60, ...]   (all values where v % 20 == 0)
        //   partition 1: [1, 21, 41, 61, ...]   (all values where v % 20 == 1)
        //   ...
        //   partition 19: [19, 39, 59, 79, ...] (all values where v % 20 == 19)
        // Each partition has 1000 values. After per-partition sort (inside spill),
        // every spill file is sorted, but their ranges completely overlap.
        // Correct global ordering requires true k-way merge across all 20 streams.
        let num_partitions = 20;
        let rows_per_partition = 1000;
        let total_rows = num_partitions * rows_per_partition;
        for p in 0..num_partitions {
            let values: Vec<i64> = (0..rows_per_partition as i64)
                .map(|i| i * num_partitions as i64 + p as i64)
                .rev() // push in reverse so spill actually sorts
                .collect();
            sorter.push(make_mp("a", values)).unwrap();
        }

        let iter = sorter.finish().unwrap();
        let mut all_values = Vec::new();
        let mut batch_count = 0;
        for res in iter {
            let mp = res.unwrap();
            let rb = mp.concat_or_get().unwrap();
            if let Some(rb) = rb.as_ref() {
                assert!(
                    rb.len() <= spill_batch_size,
                    "Output batch has {} rows, exceeding spill_batch_size {}",
                    rb.len(),
                    spill_batch_size
                );
                batch_count += 1;
                let col = rb.get_column(0);
                for i in 0..col.len() {
                    if let daft_core::lit::Literal::Int64(v) = col.get_lit(i) {
                        all_values.push(v);
                    }
                }
            }
        }

        // Global sort correctness — only possible if k-way merge actually interleaved streams.
        let expected: Vec<i64> = (0..total_rows as i64).collect();
        assert_eq!(all_values.len(), total_rows);
        assert_eq!(all_values, expected);

        // Verify we went through the merge path (multiple output batches).
        assert!(
            batch_count > 1,
            "Expected multiple output batches from MergeIterator, got {batch_count}"
        );
    }

    // ========== EMPTY SORTER ==========

    #[test]
    fn test_sorter_empty_returns_empty_partition() {
        // Verify that an empty sorter returns one empty MicroPartition (not zero)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        let iter = sorter.finish().unwrap();
        let results: Vec<_> = iter.collect();
        assert_eq!(
            results.len(),
            1,
            "Empty sorter should produce exactly 1 partition"
        );
        let mp = results[0].as_ref().unwrap();
        assert_eq!(mp.len(), 0, "The partition should have 0 rows");
    }

    // ========== NEW HELPERS ==========

    fn make_custom_sort_params(
        col_name: &str,
        schema: &Schema,
        descending: bool,
        nulls_first: bool,
    ) -> Arc<SortParams> {
        let expr = BoundExpr::try_new(resolved_col(col_name), schema).unwrap();
        Arc::new(SortParams {
            sort_by: vec![expr],
            descending: vec![descending],
            nulls_first: vec![nulls_first],
        })
    }

    fn make_multi_col_sort_params(
        col_names: &[&str],
        schema: &Schema,
        desc: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> Arc<SortParams> {
        let sort_by: Vec<BoundExpr> = col_names
            .iter()
            .map(|n| BoundExpr::try_new(resolved_col(*n), schema).unwrap())
            .collect();
        Arc::new(SortParams {
            sort_by,
            descending: desc,
            nulls_first,
        })
    }

    fn make_multi_col_mp(col_names: &[&str], col_values: Vec<Vec<i64>>) -> Arc<MicroPartition> {
        let fields: Vec<_> = col_names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<_> = col_names
            .iter()
            .zip(col_values.into_iter())
            .map(|(name, values)| Int64Array::from_vec(name, values).into_series())
            .collect();
        let rb = RecordBatch::from_nonempty_columns(columns).unwrap();
        Arc::new(MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None))
    }

    fn make_nullable_mp(col_name: &str, values: Vec<Option<i64>>) -> Arc<MicroPartition> {
        let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Int64)]));
        let field = Field::new(col_name, DataType::Int64);
        let array = daft_core::datatypes::Int64Array::from_iter(field, values.into_iter());
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        Arc::new(MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None))
    }

    fn collect_sorted_nullable_values(sorter: ExternalSorter) -> Vec<Option<i64>> {
        let iter = sorter.finish().unwrap();
        let mut all_values = Vec::new();
        for res in iter {
            let mp = res.unwrap();
            let rb = mp.concat_or_get().unwrap();
            if let Some(rb) = rb.as_ref() {
                let col = rb.get_column(0);
                for i in 0..col.len() {
                    match col.get_lit(i) {
                        daft_core::lit::Literal::Null => all_values.push(None),
                        daft_core::lit::Literal::Int64(v) => all_values.push(Some(v)),
                        _ => {}
                    }
                }
            }
        }
        all_values
    }

    // ========== DESCENDING SORT ==========

    #[test]
    fn test_sorter_descending() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, true, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter.push(make_mp("a", vec![3, 1, 2])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![3, 2, 1]);
    }

    // ========== NULLS FIRST ==========

    #[test]
    fn test_sorter_nulls_first() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, false, true);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter
            .push(make_nullable_mp("a", vec![Some(3), None, Some(1)]))
            .unwrap();

        let result = collect_sorted_nullable_values(sorter);
        assert_eq!(result, vec![None, Some(1), Some(3)]);
    }

    // ========== NULL VALUES (nulls last) ==========

    #[test]
    fn test_sorter_with_null_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, false, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter
            .push(make_nullable_mp("a", vec![Some(3), None, Some(1), None]))
            .unwrap();

        let result = collect_sorted_nullable_values(sorter);
        assert_eq!(result, vec![Some(1), Some(3), None, None]);
    }

    // ========== MULTI-COLUMN SORT ==========

    #[test]
    fn test_sorter_multi_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_multi_col_sort_params(
            &["a", "b"],
            &schema,
            vec![false, false],
            vec![false, false],
        );

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter
            .push(make_multi_col_mp(
                &["a", "b"],
                vec![vec![2, 1, 2, 1], vec![20, 10, 10, 20]],
            ))
            .unwrap();

        let iter = sorter.finish().unwrap();
        let mut a_vals = Vec::new();
        let mut b_vals = Vec::new();
        for res in iter {
            let mp = res.unwrap();
            let rb = mp.concat_or_get().unwrap();
            if let Some(rb) = rb.as_ref() {
                let col_a = rb.get_column(0);
                let col_b = rb.get_column(1);
                for i in 0..col_a.len() {
                    if let daft_core::lit::Literal::Int64(v) = col_a.get_lit(i) {
                        a_vals.push(v);
                    }
                    if let daft_core::lit::Literal::Int64(v) = col_b.get_lit(i) {
                        b_vals.push(v);
                    }
                }
            }
        }
        assert_eq!(a_vals, vec![1, 1, 2, 2]);
        assert_eq!(b_vals, vec![10, 20, 10, 20]);
    }

    // ========== ALL SAME VALUES ==========

    #[test]
    fn test_sorter_all_same_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter.push(make_mp("a", vec![5, 5, 5, 5, 5])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![5, 5, 5, 5, 5]);
    }

    // ========== SPILL DESCENDING ==========

    #[test]
    fn test_sorter_spill_descending() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, true, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![1, 3, 5])).unwrap();
        sorter.push(make_mp("a", vec![2, 4, 6])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![6, 5, 4, 3, 2, 1]);
    }

    // ========== DOUBLE SPILL ==========

    #[test]
    fn test_sorter_double_spill() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![9, 7, 8])).unwrap();
        sorter.push(make_mp("a", vec![3, 1, 2])).unwrap();
        sorter.push(make_mp("a", vec![6, 4, 5])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    // ========== ALL NULLS ==========

    #[test]
    fn test_sorter_all_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, false, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, u64::MAX, schema, 8192);
        sorter
            .push(make_nullable_mp("a", vec![None, None, None]))
            .unwrap();

        let result = collect_sorted_nullable_values(sorter);
        assert_eq!(result, vec![None, None, None]);
    }

    // ========== HEAP ITEM ORDERING ==========

    #[test]
    fn test_heap_item_ordering() {
        use arrow_row::{RowConverter, SortField};

        use super::{HeapItem, keys_to_rows};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));

        let keys1 = {
            let array = Int64Array::from_vec("a", vec![1]);
            RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap()
        };
        let keys3 = {
            let array = Int64Array::from_vec("a", vec![3]);
            RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap()
        };

        let converter =
            RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)]).unwrap();

        let rows1 = Arc::new(keys_to_rows(&keys1, &converter).unwrap());
        let rows3 = Arc::new(keys_to_rows(&keys3, &converter).unwrap());

        let item1 = HeapItem {
            stream_idx: 0,
            row_idx: 0,
            rows: rows1,
        };
        let item3 = HeapItem {
            stream_idx: 1,
            row_idx: 0,
            rows: rows3,
        };

        // BinaryHeap is max-heap, HeapItem::cmp does .reverse().
        // So item with smaller key should be "greater" in HeapItem ordering.
        assert!(
            item1 > item3,
            "HeapItem with key=1 should be > HeapItem with key=3 (reversed)"
        );
    }

    // ========== SPILL BATCH BOUNDARY ==========

    #[test]
    fn test_sorter_spill_batch_boundary() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![10])).unwrap();
        sorter.push(make_mp("a", vec![5])).unwrap();
        sorter.push(make_mp("a", vec![15])).unwrap();
        sorter.push(make_mp("a", vec![1])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 5, 10, 15]);
    }

    #[test]
    fn test_sorter_many_spill_files() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        for i in (0..80).rev() {
            sorter.push(make_mp("a", vec![i])).unwrap();
        }

        let result = collect_sorted_values(sorter);
        let expected: Vec<i64> = (0..80).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sorter_few_spill_files() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        for i in (0..5).rev() {
            sorter.push(make_mp("a", vec![i * 2 + 1, i * 2])).unwrap();
        }

        let result = collect_sorted_values(sorter);
        let expected: Vec<i64> = (0..10).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_peek_drain_non_overlapping_streams() {
        // Non-overlapping key ranges: stream 0 = [1,2,3], stream 1 = [4,5,6].
        // After merge, stream 0 should be fully drained before stream 1
        // because all of stream 0's keys < stream 1's min key.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![3, 1, 2])).unwrap();
        sorter.push(make_mp("a", vec![6, 4, 5])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_peek_drain_interleaved_streams() {
        // Highly interleaved key ranges: stream 0 = [1,3,5], stream 1 = [2,4,6].
        // Peek-drain should stop after each row since next row always > peek.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![1, 3, 5])).unwrap();
        sorter.push(make_mp("a", vec![2, 4, 6])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_peek_drain_equal_keys_across_streams() {
        // Equal keys across streams: all streams have [1,1,1].
        // Peek-drain should drain all rows from first stream (all equal to peek).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![1, 1, 1])).unwrap();
        sorter.push(make_mp("a", vec![1, 1, 1])).unwrap();
        sorter.push(make_mp("a", vec![1, 1, 1])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 1, 1, 1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn test_peek_drain_descending() {
        // Descending sort with non-overlapping ranges.
        // stream 0 = [4,5,6] sorted desc → [6,5,4], stream 1 = [1,2,3] sorted desc → [3,2,1].
        // Peek-drain should drain stream 0 fully first (all keys > stream 1 keys in desc order).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, true, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![4, 5, 6])).unwrap();
        sorter.push(make_mp("a", vec![1, 2, 3])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_peek_drain_descending_interleaved() {
        // Descending sort with interleaved keys.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, true, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![5, 3, 1])).unwrap();
        sorter.push(make_mp("a", vec![6, 4, 2])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn test_peek_drain_with_nulls() {
        // Nulls should be correctly handled during peek-drain.
        // nulls_last: [1, 2, NULL] should produce [1, 2, NULL].
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, false, false);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter
            .push(make_nullable_mp("a", vec![Some(2), None]))
            .unwrap();
        sorter
            .push(make_nullable_mp("a", vec![Some(1), Some(3)]))
            .unwrap();

        let result = collect_sorted_nullable_values(sorter);
        assert_eq!(result, vec![Some(1), Some(2), Some(3), None]);
    }

    #[test]
    fn test_peek_drain_nulls_first() {
        // nulls_first: NULLs should come before all values.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_custom_sort_params("a", &schema, false, true);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter
            .push(make_nullable_mp("a", vec![Some(3), None]))
            .unwrap();
        sorter
            .push(make_nullable_mp("a", vec![None, Some(1)]))
            .unwrap();

        let result = collect_sorted_nullable_values(sorter);
        assert_eq!(result, vec![None, None, Some(1), Some(3)]);
    }

    #[test]
    fn test_peek_drain_single_stream() {
        // Single spill file: peek-drain should drain everything
        // (heap empty after pop → no comparison needed).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![5, 3, 1, 4, 2])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_peek_drain_multi_column_keys() {
        // Multi-column sort key: (a ASC, b ASC).
        // stream 0 = [(1,2), (1,3)], stream 1 = [(1,1), (2,1)].
        // Expected: (1,1), (1,2), (1,3), (2,1).
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_multi_col_sort_params(
            &["a", "b"],
            &schema,
            vec![false, false],
            vec![false, false],
        );

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter
            .push(make_multi_col_mp(&["a", "b"], vec![vec![1, 1], vec![2, 3]]))
            .unwrap();
        sorter
            .push(make_multi_col_mp(&["a", "b"], vec![vec![1, 2], vec![1, 1]]))
            .unwrap();

        let result = collect_sorted_values(sorter);
        // collect_sorted_values reads column 0 ("a").
        // Expected order: (1,1),(1,2),(1,3),(2,1) → column a = [1,1,1,2].
        assert_eq!(result, vec![1, 1, 1, 2]);
    }

    #[test]
    fn test_peek_drain_many_streams() {
        // Many streams (one value each) — tests peek-drain with K > 2.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        for i in (0..20).rev() {
            sorter.push(make_mp("a", vec![i])).unwrap();
        }

        let result = collect_sorted_values(sorter);
        let expected: Vec<i64> = (0..20).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_peek_drain_partial_overlap() {
        // Partial overlap: stream 0 = [1,2,5,6], stream 1 = [3,4,7,8].
        // Drain should stop at 2 (next=5 > peek=3), then interleave.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64)]));
        let dir = tempfile::tempdir().unwrap();
        let spill_manager = Arc::new(SpillManager::new(dir.path()).unwrap());
        let params = make_sort_params("a", &schema);

        let mut sorter = ExternalSorter::new(params, spill_manager, 1, schema, 8192);
        sorter.push(make_mp("a", vec![1, 2, 5, 6])).unwrap();
        sorter.push(make_mp("a", vec![3, 4, 7, 8])).unwrap();

        let result = collect_sorted_values(sorter);
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
