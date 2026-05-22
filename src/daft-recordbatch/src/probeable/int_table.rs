use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{array::DataArray, prelude::DaftIntegerType};
use hashbrown::HashMap;

use super::{IndicesMapper, ProbeContent, Probeable, ProbeableBuilder};
use crate::{
    RecordBatch,
    probeable::probes::{LOWER_MASK, TABLE_IDX_SHIFT},
};

// Applies to logical types that are backed by ints as well
// TODO: Extend to types other than ints
// In order of the following:
// * Float types: f32, f64
// * Decimal, bool

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
    const DEFAULT_SIZE: usize = 64 * 1024;

    pub(crate) fn new() -> DaftResult<Self> {
        let hash_table = HashMap::with_capacity(Self::DEFAULT_SIZE);
        Ok(Self {
            hash_table,
            num_tables: 0,
        })
    }

    fn probe(
        &self,
        input: DataArray<T>,
    ) -> DaftResult<impl Iterator<Item = Option<V::ProbeOutput<'_>>>> {
        Ok(input.into_iter().map(|val| {
            let val = val?;
            self.hash_table.get(&val).map(|indices| indices.probe_out())
        }))
    }

    fn add_table(&mut self, input: &DataArray<T>) -> DaftResult<()> {
        debug_assert!(self.num_tables < (1 << (64 - TABLE_IDX_SHIFT)));
        debug_assert!(input.len() < (1 << TABLE_IDX_SHIFT));

        for (i, h) in input.into_iter().enumerate() {
            let Some(h) = h else {
                continue;
            };

            let idx = (self.num_tables << TABLE_IDX_SHIFT) | i;
            self.hash_table.entry(h).or_default().add_row(idx as u64);
        }

        self.num_tables += 1;
        Ok(())
    }
}

impl<T: DaftIntegerType, V: ProbeContent> Probeable for IntProbeTable<T, V>
where
    T::Native: Ord + std::hash::Hash,
{
    fn probe_indices(&'_ self, table: RecordBatch) -> DaftResult<IndicesMapper<'_>> {
        let iter = self.probe(
            table
                .get_column(0)
                .as_physical()?
                .downcast::<DataArray<T>>()?
                .clone(),
        )?;
        let converted_iter = iter.map(|opt| V::to_indices(opt));
        Ok(IndicesMapper::new(
            Box::new(converted_iter),
            TABLE_IDX_SHIFT,
            LOWER_MASK,
        ))
    }

    fn probe_exists<'a>(
        &'a self,
        table: RecordBatch,
    ) -> DaftResult<Box<dyn Iterator<Item = bool> + 'a>> {
        let iter = self.probe(
            table
                .get_column(0)
                .as_physical()?
                .downcast::<DataArray<T>>()?
                .clone(),
        )?;
        Ok(Box::new(iter.map(|output| output.is_some())))
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
        self.0.add_table(
            table
                .get_column(0)
                .as_physical()?
                .downcast::<DataArray<T>>()?,
        )
    }

    fn build(self: Box<Self>) -> Arc<dyn Probeable> {
        Arc::new(self.0)
    }
}
