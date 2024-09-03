mod probe_set;
mod probe_table;

use std::sync::Arc;

use common_error::DaftResult;

use daft_core::schema::SchemaRef;
use probe_set::{ProbeSet, ProbeSetBuilder};
use probe_table::{ProbeTable, ProbeTableBuilder};

use crate::Table;

struct ArrowTableEntry(Vec<Box<dyn arrow2::array::Array>>);

pub fn make_probeable_builder(
    schema: SchemaRef,
    track_indices: bool,
) -> DaftResult<Box<dyn ProbeableBuilder>> {
    if track_indices {
        Ok(Box::new(ProbeTableBuilder(ProbeTable::new(schema)?)))
    } else {
        Ok(Box::new(ProbeSetBuilder(ProbeSet::new(schema)?)))
    }
}

pub trait ProbeableBuilder: Send + Sync {
    fn add_table(&mut self, table: &Table) -> DaftResult<()>;
    fn build(self: Box<Self>) -> Arc<dyn Probeable>;
}

type IndicesIter<'a> = Box<dyn Iterator<Item = (u32, u64)> + 'a>;
pub trait Probeable: Send + Sync {
    /// Probe_indices returns an iterator of optional iterators. The outer iterator iterates over the rows of the right table.
    /// The inner iterator, if present, iterates over the rows of the left table that match the right row.
    /// Otherwise, if the inner iterator is None, indicates that the right row has no matches.
    /// NOTE: This function only works if track_indices is true.
    fn probe_indices<'a>(
        &'a self,
        table: &'a Table,
    ) -> DaftResult<Box<dyn Iterator<Item = Option<IndicesIter>> + 'a>>;

    /// Probe_exists returns an iterator of booleans. The iterator iterates over the rows of the right table.
    fn probe_exists<'a>(
        &'a self,
        table: &'a Table,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>>;
}
