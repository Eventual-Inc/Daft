use std::collections::{hash_map::RawEntryMut, HashMap};

use arrow2::array::Array;
use common_error::DaftResult;
use daft_core::{
    array::{
        ops::{arrow2::comparison2::build_dyn_multi_array_is_equal, as_arrow::AsArrow},
        DataArray,
    },
    datatypes::UInt64Array,
    schema::SchemaRef,
};

use crate::{
    ops::hash::{IdentityBuildHasher, IndexHash},
    Table,
};

struct ArrowTableEntry(Vec<Box<dyn arrow2::array::Array>>);

pub struct ProbeTable {
    schema: SchemaRef,
    hash_table: HashMap<IndexHash, Vec<u64>, IdentityBuildHasher>,
    tables: Vec<ArrowTableEntry>,
    compare_fn:
        Box<dyn Fn(&[Box<dyn Array>], &[Box<dyn Array>], usize, usize) -> bool + Send + Sync>,
}

impl ProbeTable {
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;

    pub fn probe<'a>(
        &'a self,
        right: &'a Table,
    ) -> DaftResult<impl Iterator<Item = (u32, u64, u64)> + 'a> {
        assert_eq!(right.schema, self.schema);
        let r_hashes = right.hash_rows()?;

        let right_arrays = right
            .columns
            .iter()
            .map(|s| s.to_arrow())
            .collect::<Vec<_>>();

        let iter = r_hashes.as_arrow().clone().into_iter();

        Ok(iter
            .enumerate()
            .filter_map(|(i, h)| h.and_then(|h| Some((i, h))))
            .map(move |(r_idx, h)| {
                let indices = if let Some((_, indices)) =
                    self.hash_table.raw_entry().from_hash(h, |other| {
                        h == other.hash && {
                            let l_idx = other.idx;
                            let l_table_idx = (l_idx >> Self::TABLE_IDX_SHIFT) as usize;
                            let l_row_idx = (l_idx & Self::LOWER_MASK) as usize;

                            let l_table = self.tables.get(l_table_idx as usize).unwrap();

                            let left_refs = l_table.0.as_slice();

                            (self.compare_fn)(left_refs, &right_arrays, l_row_idx, r_idx)
                        }
                    }) {
                    indices.as_slice()
                } else {
                    [].as_slice()
                };
                indices.iter().map(move |l_idx| {
                    let l_table_idx = (l_idx >> Self::TABLE_IDX_SHIFT) as usize;
                    let l_row_idx = (l_idx & Self::LOWER_MASK) as usize;
                    (l_table_idx as u32, l_row_idx as u64, r_idx as u64)
                })
            })
            .flatten())
    }
}

pub struct ProbeTableBuilder {
    pt: ProbeTable,
}

impl ProbeTableBuilder {
    const DEFAULT_SIZE: usize = 20;
    // Use the leftmost 28 bits for the table index and the rightmost 36 bits for the row number
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;

    pub fn new(schema: SchemaRef) -> DaftResult<Self> {
        let hash_table =
            HashMap::<IndexHash, Vec<u64>, IdentityBuildHasher>::with_capacity_and_hasher(
                Self::DEFAULT_SIZE,
                Default::default(),
            );
        let compare_fn = build_dyn_multi_array_is_equal(&schema, false, false)?;
        Ok(Self {
            pt: ProbeTable {
                schema,
                hash_table,
                tables: vec![],
                compare_fn,
            },
        })
    }

    pub fn add_table(&mut self, table: &Table) -> DaftResult<()> {
        // we have to cast to the join key schema
        assert_eq!(table.schema, self.pt.schema);
        let hashes = table.hash_rows()?;
        let table_idx = self.pt.tables.len();
        let table_offset = table_idx << Self::TABLE_IDX_SHIFT;

        assert!(table_idx < (1 << (64 - Self::TABLE_IDX_SHIFT)));
        assert!(table.len() < (1 << Self::TABLE_IDX_SHIFT));
        let current_arrays = table
            .columns
            .iter()
            .map(|s| s.to_arrow())
            .collect::<Vec<_>>();
        self.pt.tables.push(ArrowTableEntry(current_arrays));
        let current_array_refs = self.pt.tables.last().unwrap().0.as_slice();
        // TODO: move probe table logic into that struct impl
        for (i, h) in hashes.as_arrow().values_iter().enumerate() {
            let idx = table_offset | i;
            let entry = self.pt.hash_table.raw_entry_mut().from_hash(*h, |other| {
                (*h == other.hash) && {
                    let j_idx = other.idx;
                    let j_table_idx = (j_idx >> Self::TABLE_IDX_SHIFT) as usize;
                    let j_row_idx = (j_idx & Self::LOWER_MASK) as usize;

                    if table_idx == j_table_idx {
                        (self.pt.compare_fn)(
                            &current_array_refs,
                            &current_array_refs,
                            i,
                            j_row_idx as usize,
                        )
                    } else {
                        let j_table = self.pt.tables.get(j_table_idx as usize).unwrap();

                        let array_refs = j_table.0.as_slice();

                        (self.pt.compare_fn)(
                            &current_array_refs,
                            &array_refs,
                            i,
                            j_row_idx as usize,
                        )
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

    pub fn build(self) -> ProbeTable {
        self.pt
    }
}
