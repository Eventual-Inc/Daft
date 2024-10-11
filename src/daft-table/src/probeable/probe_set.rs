use std::{
    collections::{hash_map::RawEntryMut, HashMap},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::{
    array::ops::as_arrow::AsArrow,
    prelude::SchemaRef,
    utils::{
        dyn_compare::{build_dyn_multi_array_compare, MultiDynArrayComparator},
        identity_hash_set::IdentityBuildHasher,
    },
};

use super::{ArrowTableEntry, IndicesMapper, Probeable, ProbeableBuilder};
use crate::{ops::hash::IndexHash, Table};
pub struct ProbeSet {
    schema: SchemaRef,
    hash_table: HashMap<IndexHash, (), IdentityBuildHasher>,
    tables: Vec<ArrowTableEntry>,
    compare_fn: MultiDynArrayComparator,
    num_groups: usize,
    num_rows: usize,
}

impl ProbeSet {
    // Use the leftmost 28 bits for the table index and the rightmost 36 bits for the row number
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;

    const DEFAULT_SIZE: usize = 20;

    pub(crate) fn new(schema: SchemaRef) -> DaftResult<Self> {
        let hash_table = HashMap::<IndexHash, (), IdentityBuildHasher>::with_capacity_and_hasher(
            Self::DEFAULT_SIZE,
            Default::default(),
        );
        let compare_fn = build_dyn_multi_array_compare(&schema, false, false)?;
        Ok(Self {
            schema,
            hash_table,
            tables: vec![],
            compare_fn,
            num_groups: 0,
            num_rows: 0,
        })
    }

    fn probe<'a>(&'a self, input: &'a Table) -> DaftResult<impl Iterator<Item = bool> + 'a> {
        assert_eq!(self.schema.len(), input.schema.len());
        assert!(self
            .schema
            .fields
            .values()
            .zip(input.schema.fields.values())
            .all(|(l, r)| l.dtype == r.dtype));

        let hashes = input.hash_rows()?;

        let input_arrays = input
            .columns
            .iter()
            .map(|s| Ok(s.as_physical()?.to_arrow()))
            .collect::<DaftResult<Vec<_>>>()?;

        let iter = hashes.as_arrow().clone().into_iter();

        Ok(iter.enumerate().map(move |(idx, h)| {
            if let Some(h) = h {
                self.hash_table.raw_entry().from_hash(h, |other| {
                    h == other.hash && {
                        let other_table_idx = (other.idx >> Self::TABLE_IDX_SHIFT) as usize;
                        let other_row_idx = (other.idx & Self::LOWER_MASK) as usize;

                        let other_table = self.tables.get(other_table_idx).unwrap();

                        let other_refs = other_table.0.as_slice();

                        (self.compare_fn)(other_refs, &input_arrays, other_row_idx, idx).is_eq()
                    }
                })
            } else {
                None
            }
            .is_some()
        }))
    }

    fn add_table(&mut self, table: &Table) -> DaftResult<()> {
        // we have to cast to the join key schema
        assert_eq!(table.schema, self.schema);
        let hashes = table.hash_rows()?;
        let table_idx = self.tables.len();
        let table_offset = table_idx << Self::TABLE_IDX_SHIFT;

        assert!(table_idx < (1 << (64 - Self::TABLE_IDX_SHIFT)));
        assert!(table.len() < (1 << Self::TABLE_IDX_SHIFT));
        let current_arrays = table
            .columns
            .iter()
            .map(|s| Ok(s.as_physical()?.to_arrow()))
            .collect::<DaftResult<Vec<_>>>()?;
        self.tables.push(ArrowTableEntry(current_arrays));
        let current_array_refs = self.tables.last().unwrap().0.as_slice();
        for (i, h) in hashes.as_arrow().values_iter().enumerate() {
            let idx = table_offset | i;
            let entry = self.hash_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j_idx = other.idx;
                    let j_table_idx = (j_idx >> Self::TABLE_IDX_SHIFT) as usize;
                    let j_row_idx = (j_idx & Self::LOWER_MASK) as usize;

                    if table_idx == j_table_idx {
                        (self.compare_fn)(current_array_refs, current_array_refs, i, j_row_idx)
                            .is_eq()
                    } else {
                        let j_table = self.tables.get(j_table_idx).unwrap();

                        let array_refs = j_table.0.as_slice();

                        (self.compare_fn)(current_array_refs, array_refs, i, j_row_idx).is_eq()
                    }
                }
            });
            match entry {
                RawEntryMut::Vacant(entry) => {
                    entry.insert_hashed_nocheck(
                        *h,
                        IndexHash {
                            idx: idx as u64,
                            hash: *h,
                        },
                        (),
                    );
                    self.num_groups += 1;
                }
                RawEntryMut::Occupied(_) => {}
            }
        }
        self.num_rows += table.len();
        Ok(())
    }
}

impl Probeable for ProbeSet {
    fn probe_exists<'a>(
        &'a self,
        table: &'a Table,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>> {
        Ok(Box::new(self.probe(table)?))
    }

    fn probe_indices<'a>(&'a self, _table: &'a Table) -> DaftResult<IndicesMapper<'a>> {
        panic!("Probe indices is not supported for ProbeSet")
    }
}

pub struct ProbeSetBuilder(pub ProbeSet);

impl ProbeableBuilder for ProbeSetBuilder {
    fn add_table(&mut self, table: &Table) -> DaftResult<()> {
        self.0.add_table(table)
    }

    fn build(self: Box<Self>) -> Arc<dyn Probeable> {
        Arc::new(self.0)
    }
}
