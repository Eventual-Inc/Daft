/// Summary statistics for a single output partition produced by a shuffle
/// writer (e.g. row count, serialized byte size).
pub(crate) struct ShufflePartitionMeta {
    pub(crate) num_rows: usize,
    pub(crate) size_bytes: usize,
}

impl ShufflePartitionMeta {
    pub(crate) fn new(num_rows: usize, size_bytes: usize) -> Self {
        Self {
            num_rows,
            size_bytes,
        }
    }
}
