use std::collections::{hash_map::RawEntryMut, HashMap};

use arrow2::array::Array;
use common_error::DaftResult;
use daft_core::{array::ops::{arrow2::comparison2::build_dyn_multi_array_is_equal, as_arrow::AsArrow}, schema::SchemaRef};

use crate::{ops::hash::{IdentityBuildHasher, IndexHash}, Table};




struct ProbeTableBuilder {
    schema: SchemaRef,
    hash_table: HashMap::<IndexHash, Vec<u64>, IdentityBuildHasher>,
    tables: Vec<Table>,
    compare_fn: Box<dyn Fn(&[&dyn Array], &[&dyn Array], usize, usize) -> bool + Send + Sync>
}

impl ProbeTableBuilder {
    const DEFAULT_SIZE: usize = 20;
    // Use the leftmost 28 bits for the table index and the rightmost 36 bits for the row number
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;


    fn new(schema: SchemaRef) -> DaftResult<Self> {
        let hash_table =  HashMap::<IndexHash, Vec<u64>, IdentityBuildHasher>::with_capacity_and_hasher(
            Self::DEFAULT_SIZE,
            Default::default(),
        );
        let compare_fn = build_dyn_multi_array_is_equal(&schema, false, false)?;
        Ok(Self { schema, hash_table, tables: vec![], compare_fn  })
    }

    fn add_table(&mut self, table: Table) -> DaftResult<()> {
        assert_eq!(table.schema, self.schema);
        let hashes = table.hash_rows()?;
        let table_idx = self.tables.len();
        let table_offset = table_idx << Self::TABLE_IDX_SHIFT;

        assert!(table_idx < (1 << (64-Self::TABLE_IDX_SHIFT)));
        assert!(table.len() < (1 << Self::TABLE_IDX_SHIFT));

        let current_arrays = table.columns.iter().map(|s| s.to_arrow()).collect::<Vec<_>>();
        let current_array_refs = current_arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        self.tables.push(table);

        for (i, h) in hashes.as_arrow().values_iter().enumerate() {
            let idx = table_offset | i;
            let entry = self.hash_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j_idx = other.idx;
                    let j_table_idx = (j_idx >> Self::TABLE_IDX_SHIFT) as usize;
                    let j_row_idx = (j_idx & Self::LOWER_MASK) as usize;

                    if table_idx == j_table_idx {
                        (self.compare_fn)(&current_array_refs, &current_array_refs, i, j_row_idx as usize)
                    } else {
                        let j_table = self.tables.get(j_table_idx as usize).unwrap();
                    
                        // WE SHOULDN'T BE CALLING TO_ARROW EVERY ITERATION: DO NOT MERGE
                        let arrays = j_table.columns.iter().map(|s| s.to_arrow()).collect::<Vec<_>>();
                        let array_refs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                
                        (self.compare_fn)(&current_array_refs, &array_refs, i, j_row_idx as usize)
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