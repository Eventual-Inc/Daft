use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::arrow::comparison::build_multi_array_is_equal_from_arrays,
    prelude::SchemaRef,
    utils::identity_hash_set::{IdentityBuildHasher, IndexHash},
};
use hashbrown::{HashMap, hash_map::RawEntryMut};

use super::{ArrowTableEntry, IndicesMapper, Probeable, ProbeableBuilder};
use crate::RecordBatch;

pub struct ProbeTable {
    schema: SchemaRef,
    hash_table: HashMap<IndexHash, Vec<u64>, IdentityBuildHasher>,
    tables: Vec<ArrowTableEntry>,
    nulls_equal: Vec<bool>,
    nans_equal: Vec<bool>,
}

impl ProbeTable {
    // Use the leftmost 28 bits for the table index and the rightmost 36 bits for the row number
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;

    const DEFAULT_SIZE: usize = 20;

    pub(crate) fn new(schema: SchemaRef, null_equal_aware: Option<&Vec<bool>>) -> DaftResult<Self> {
        let hash_table =
            HashMap::<IndexHash, Vec<u64>, IdentityBuildHasher>::with_capacity_and_hasher(
                Self::DEFAULT_SIZE,
                Default::default(),
            );
        if let Some(null_equal_aware) = null_equal_aware
            && null_equal_aware.len() != schema.len()
        {
            return Err(DaftError::InternalError(format!(
                "null_equal_aware should have the same length as the schema. Expected: {}, Found: {}",
                schema.len(),
                null_equal_aware.len()
            )));
        }
        let nulls_equal = null_equal_aware
            .cloned()
            .unwrap_or_else(|| vec![false; schema.len()]);
        let nans_equal = vec![false; schema.len()];
        Ok(Self {
            schema,
            hash_table,
            tables: vec![],
            nulls_equal,
            nans_equal,
        })
    }

    fn probe<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> DaftResult<impl Iterator<Item = Option<&'a [u64]>> + 'a> {
        assert_eq!(self.schema.len(), input.schema.len());
        assert!(
            self.schema
                .into_iter()
                .zip(input.schema.fields())
                .all(|(l, r)| l.dtype == r.dtype)
        );

        let hashes = input.hash_rows()?;

        let input_arrays = input
            .columns
            .iter()
            .map(|s| s.as_physical()?.to_arrow())
            .collect::<DaftResult<Vec<_>>>()?;

        // Pre-create comparators for each stored table vs input.
        // Each comparator captures typed arrays via Arc, avoiding per-row downcasts.
        let comparators: Vec<_> = self
            .tables
            .iter()
            .map(|table| {
                build_multi_array_is_equal_from_arrays(
                    &table.0,
                    &input_arrays,
                    &self.nulls_equal,
                    &self.nans_equal,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // Collect needed: hashes is a local variable, so .iter() borrows it and
        // the returned iterator would reference a dropped value.
        #[allow(clippy::needless_collect)]
        let hash_vec: Vec<Option<u64>> = hashes.iter().collect();

        Ok(Box::new(hash_vec.into_iter().enumerate().map(
            move |(idx, h)| match h {
                Some(h) => {
                    if let Some((_, indices)) = self.hash_table.raw_entry().from_hash(h, |other| {
                        h == other.hash && {
                            let other_table_idx = (other.idx >> Self::TABLE_IDX_SHIFT) as usize;
                            let other_row_idx = (other.idx & Self::LOWER_MASK) as usize;
                            comparators[other_table_idx](other_row_idx, idx)
                        }
                    }) {
                        Some(indices.as_slice())
                    } else {
                        None
                    }
                }
                None => None,
            },
        )))
    }

    fn add_table(&mut self, table: &RecordBatch) -> DaftResult<()> {
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
            .map(|s| s.as_physical()?.to_arrow())
            .collect::<DaftResult<Vec<_>>>()?;

        // Pre-create comparators: current table vs each existing table + self.
        // Comparators capture array data via Arc, so they remain valid after the move below.
        let mut comparators: Vec<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> = self
            .tables
            .iter()
            .map(|other_table| {
                build_multi_array_is_equal_from_arrays(
                    &current_arrays,
                    &other_table.0,
                    &self.nulls_equal,
                    &self.nans_equal,
                )
            })
            .collect::<DaftResult<Vec<_>>>()?;
        comparators.push(build_multi_array_is_equal_from_arrays(
            &current_arrays,
            &current_arrays,
            &self.nulls_equal,
            &self.nans_equal,
        )?);

        self.tables.push(ArrowTableEntry(current_arrays));

        for (i, h) in hashes.values().iter().enumerate() {
            let idx = table_offset | i;
            let entry = self.hash_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j_table_idx = (other.idx >> Self::TABLE_IDX_SHIFT) as usize;
                    let j_row_idx = (other.idx & Self::LOWER_MASK) as usize;
                    comparators[j_table_idx](i, j_row_idx)
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
                        vec![idx as u64],
                    );
                }
                RawEntryMut::Occupied(mut entry) => {
                    entry.get_mut().push(idx as u64);
                }
            }
        }
        Ok(())
    }
}

impl Probeable for ProbeTable {
    fn probe_indices<'a>(&'a self, table: &'a RecordBatch) -> DaftResult<IndicesMapper<'a>> {
        let iter = self.probe(table)?;
        Ok(IndicesMapper::new(
            Box::new(iter),
            Self::TABLE_IDX_SHIFT,
            Self::LOWER_MASK,
        ))
    }

    fn probe_exists<'a>(
        &'a self,
        table: &'a RecordBatch,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>> {
        let iter = self.probe(table)?;
        Ok(Box::new(iter.map(|indices| indices.is_some())))
    }
}

pub struct ProbeTableBuilder(pub ProbeTable);

impl ProbeableBuilder for ProbeTableBuilder {
    fn add_table(&mut self, table: &RecordBatch) -> DaftResult<()> {
        self.0.add_table(table)
    }

    fn build(self: Box<Self>) -> Arc<dyn Probeable> {
        Arc::new(self.0)
    }
}
