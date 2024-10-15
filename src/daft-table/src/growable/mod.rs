use common_error::{DaftError, DaftResult};
use daft_core::{
    array::growable::{make_growable, Growable},
    series::Series,
};

use crate::Table;

pub struct GrowableTable<'a> {
    growables: Vec<Box<dyn Growable + 'a>>,
}

impl<'a> GrowableTable<'a> {
    pub fn new(tables: &[&'a Table], use_validity: bool, capacity: usize) -> DaftResult<Self> {
        let num_tables = tables.len();
        if tables.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 Table for GrowableTable".to_string(),
            ));
        }

        let first_table = tables.first().unwrap();
        let num_columns = first_table.num_columns();
        let first_schema = first_table.schema.as_ref();

        let mut series_list = (0..num_columns)
            .map(|_| Vec::<&Series>::with_capacity(num_tables))
            .collect::<Vec<_>>();

        for tab in tables {
            if tab.schema.as_ref() != first_schema {
                return Err(DaftError::SchemaMismatch(format!(
                    "GrowableTable requires all schemas to match, {} vs {}",
                    first_schema, tab.schema
                )));
            }
            for (col, v) in tab.columns.iter().zip(series_list.iter_mut()) {
                v.push(col);
            }
        }
        let growables = series_list
            .into_iter()
            .zip(first_schema.fields.values())
            .map(|(vector, f)| make_growable(&f.name, &f.dtype, vector, use_validity, capacity))
            .collect::<Vec<_>>();
        Ok(Self { growables })
    }

    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    pub fn extend(&mut self, index: usize, start: usize, len: usize) {
        if !self.growables.is_empty() {
            self.growables
                .iter_mut()
                .for_each(|g| g.extend(index, start, len));
        }
    }

    /// Extends this [`Growable`] with null elements
    pub fn add_nulls(&mut self, additional: usize) {
        if !self.growables.is_empty() {
            self.growables
                .iter_mut()
                .for_each(|g| g.add_nulls(additional));
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
