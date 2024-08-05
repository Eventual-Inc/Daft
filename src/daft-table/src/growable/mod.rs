use common_error::DaftResult;
use daft_core::{
    array::growable::{make_growable, Growable},
    Series,
};

use crate::Table;

pub struct GrowableTable<'a> {
    growables: Vec<Box<dyn Growable + 'a>>,
}

impl<'a> GrowableTable<'a> {
    pub fn new(tables: &[&'a Table], use_validity: bool, capacity: usize) -> Self {
        let num_tables = tables.len();
        assert!(num_tables > 0);
        let first = tables.first().unwrap();
        let num_columns = first.num_columns();
        let schema = &first.schema;
        let mut series_list = (0..num_columns)
            .map(|_| Vec::<&Series>::with_capacity(num_columns))
            .collect::<Vec<_>>();
        for tab in tables {
            for (col, v) in tab.columns.iter().zip(series_list.iter_mut()) {
                v.push(col);
            }
        }
        let growables = series_list
            .into_iter()
            .zip(schema.fields.values())
            .map(|(vector, f)| make_growable(&f.name, &f.dtype, vector, use_validity, capacity))
            .collect::<Vec<_>>();
        Self { growables }
    }

    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    pub fn extend(&mut self, index: usize, start: usize, len: usize) {
        if !self.growables.is_empty() {
            self.growables
                .iter_mut()
                .for_each(|g| g.extend(index, start, len))
        }
    }

    /// Extends this [`Growable`] with null elements
    pub fn add_nulls(&mut self, additional: usize) {
        if !self.growables.is_empty() {
            self.growables
                .iter_mut()
                .for_each(|g| g.add_nulls(additional))
        }
    }

    /// Builds an array from the [`Growable`]
    pub fn build(&mut self) -> DaftResult<Table> {
        if self.growables.is_empty() {
            Table::empty(None)
        } else {
            let columns = self
                .growables
                .iter_mut()
                .map(|g| g.build())
                .collect::<DaftResult<Vec<_>>>()?;
            Table::from_nonempty_columns(columns)
        }
    }
}
