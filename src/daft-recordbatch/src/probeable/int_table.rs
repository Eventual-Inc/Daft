use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_core::{array::DataArray, prelude::DaftIntegerType};

use super::{IndicesMapper, ProbeContent, Probeable, ProbeableBuilder};
use crate::RecordBatch;

// TODO: Extend to types other than ints
// In order of the following:
// * Float types: f32, f64
// * Decimal, bool, Date, Time, Datetime

pub struct IntProbeTable<T: DaftIntegerType, V: ProbeContent>
where
    T::Native: Ord + std::hash::Hash,
{
    hash_table: HashMap<T::Native, V>,
    num_tables: usize,
}

impl<T: DaftIntegerType, V: ProbeContent> IntProbeTable<T, V>
where
    T::Native: Ord + std::hash::Hash,
{
    // Use the leftmost 28 bits for the table index and the rightmost 36 bits for the row number
    const TABLE_IDX_SHIFT: usize = 36;
    const LOWER_MASK: u64 = (1 << Self::TABLE_IDX_SHIFT) - 1;

    const DEFAULT_SIZE: usize = 32;

    pub(crate) fn new() -> DaftResult<Self> {
        let hash_table = HashMap::with_capacity(Self::DEFAULT_SIZE);
        Ok(Self {
            hash_table,
            num_tables: 0,
        })
    }

    fn probe<'a>(
        &'a self,
        input: &'a DataArray<T>,
    ) -> DaftResult<impl Iterator<Item = Option<V::ProbeOutput<'a>>> + 'a> {
        Ok(input.into_iter().map(|val| {
            let val = val?;
            self.hash_table.get(val).map(|indices| indices.probe_out())
        }))
    }

    fn insert_build(&mut self, input: &DataArray<T>) -> DaftResult<()> {
        for (i, h) in input.into_iter().enumerate() {
            let Some(h) = h else {
                continue;
            };

            let idx = (self.num_tables << Self::TABLE_IDX_SHIFT) | i;
            self.hash_table.entry(*h).or_default().add_row(idx as u64);
        }

        self.num_tables += 1;
        Ok(())
    }
}

impl<T: DaftIntegerType, V: ProbeContent> Probeable for IntProbeTable<T, V>
where
    T::Native: Ord + std::hash::Hash,
{
    fn probe_indices<'a>(&'a self, table: &'a RecordBatch) -> DaftResult<IndicesMapper<'a>> {
        let iter = self.probe(table.get_column(0).downcast::<DataArray<T>>()?)?;
        let converted_iter = iter.map(|opt| V::to_indices(opt));
        Ok(IndicesMapper::new(
            Box::new(converted_iter),
            Self::TABLE_IDX_SHIFT,
            Self::LOWER_MASK,
        ))
    }

    fn probe_exists<'a>(
        &'a self,
        table: &'a RecordBatch,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>> {
        let iter = self.probe(table.get_column(0).downcast::<DataArray<T>>()?)?;
        Ok(Box::new(iter.map(|output| V::to_exists(output))))
    }
}

pub struct ProbeTableBuilder<T: DaftIntegerType, V: ProbeContent>(pub IntProbeTable<T, V>)
where
    T::Native: Ord + std::hash::Hash;

impl<T: DaftIntegerType, V: ProbeContent> ProbeTableBuilder<T, V>
where
    T::Native: Ord + std::hash::Hash,
{
    pub fn new() -> DaftResult<Self> {
        Ok(Self(IntProbeTable::new()?))
    }
}

impl<T: DaftIntegerType, V: ProbeContent> ProbeableBuilder for ProbeTableBuilder<T, V>
where
    T::Native: Ord + std::hash::Hash,
{
    fn add_table(&mut self, table: &RecordBatch) -> DaftResult<()> {
        self.0
            .insert_build(table.get_column(0).downcast::<DataArray<T>>()?)
    }

    fn build(self: Box<Self>) -> Arc<dyn Probeable> {
        Arc::new(self.0)
    }
}
