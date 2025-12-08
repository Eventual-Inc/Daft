mod probe_set;
mod probe_table;

use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use probe_set::{ProbeSet, ProbeSetBuilder};
use probe_table::{ProbeTable, ProbeTableBuilder};

use crate::RecordBatch;

struct ArrowTableEntry(Vec<Box<dyn daft_arrow::array::Array>>);

pub fn make_probeable_builder(
    schema: SchemaRef,
    nulls_equal_aware: Option<&Vec<bool>>,
    track_indices: bool,
) -> DaftResult<Box<dyn ProbeableBuilder>> {
    if track_indices {
        Ok(Box::new(ProbeTableBuilder(ProbeTable::new(
            schema,
            nulls_equal_aware,
        )?)))
    } else {
        Ok(Box::new(ProbeSetBuilder(ProbeSet::new(
            schema,
            nulls_equal_aware,
        )?)))
    }
}

pub trait ProbeableBuilder: Send + Sync {
    fn add_table(&mut self, table: &RecordBatch) -> DaftResult<()>;
    fn build(self: Box<Self>) -> Arc<dyn Probeable>;
}

pub struct IndicesMapper<'a> {
    table_idx_shift: usize,
    lower_mask: u64,
    idx_iter: Box<dyn Iterator<Item = Option<&'a [u64]>> + 'a>,
}

impl<'a> IndicesMapper<'a> {
    pub fn new(
        idx_iter: Box<dyn Iterator<Item = Option<&'a [u64]>> + 'a>,
        table_idx_shift: usize,
        lower_mask: u64,
    ) -> Self {
        Self {
            table_idx_shift,
            lower_mask,
            idx_iter,
        }
    }

    pub fn make_iter(self) -> impl Iterator<Item = Option<impl Iterator<Item = (u32, u64)> + 'a>> {
        let table_idx_shift = self.table_idx_shift;
        let lower_mask = self.lower_mask;
        self.idx_iter.map(move |indices| match indices {
            Some(indices) => {
                let inner_iter = indices.iter().map(move |idx| {
                    let table_idx = (idx >> table_idx_shift) as u32;
                    let row_idx = idx & lower_mask;
                    (table_idx, row_idx)
                });
                Some(inner_iter)
            }
            None => None,
        })
    }
}

pub trait Probeable: Send + Sync {
    /// Probe_indices returns an iterator of optional iterators. The outer iterator iterates over the rows of the right table.
    /// The inner iterator, if present, iterates over the rows of the left table that match the right row.
    /// Otherwise, if the inner iterator is None, indicates that the right row has no matches.
    /// NOTE: This function only works if track_indices is true.
    fn probe_indices<'a>(&'a self, table: &'a RecordBatch) -> DaftResult<IndicesMapper<'a>>;

    /// Probe_exists returns an iterator of booleans. The iterator iterates over the rows of the right table.
    fn probe_exists<'a>(
        &'a self,
        table: &'a RecordBatch,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>>;
}

#[derive(Clone)]
pub struct ProbeState {
    probeable: Arc<dyn Probeable>,
    record_batches: Vec<RecordBatch>,
}

impl ProbeState {
    pub fn new(probeable: Arc<dyn Probeable>, record_batches: Vec<RecordBatch>) -> Self {
        Self {
            probeable,
            record_batches,
        }
    }

    /// Returns an iterator of booleans. The iterator iterates over the rows of the input table.
    /// True if the right row has a match in the left table, false otherwise.
    pub fn probe_exists<'a>(
        &'a self,
        record_batch: &'a RecordBatch,
    ) -> DaftResult<impl Iterator<Item = bool> + 'a> {
        self.probeable.probe_exists(record_batch)
    }

    /// Returns an iterator of optional iterators.
    /// The outer iterator iterates over the rows of the input table.
    /// The inner iterator, if present, iterates over the rows of the build table that match the input row.
    pub fn probe_indices<'a>(
        &'a self,
        record_batch: &'a RecordBatch,
    ) -> DaftResult<impl Iterator<Item = Option<impl Iterator<Item = (u32, u64)> + 'a>>> {
        self.probeable
            .probe_indices(record_batch)
            .map(|indices_mapper| indices_mapper.make_iter())
    }

    pub fn get_record_batches(&self) -> &[RecordBatch] {
        &self.record_batches
    }
}
