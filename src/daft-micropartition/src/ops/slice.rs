use common_error::{DaftError, DaftResult};
use daft_io::IOStatsContext;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new(format!("MicroPartition::slice {start}-{end}"));

        let mut rows_needed = end.min(self.len()) - start.min(self.len());

        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice MicroPartition with negative length, start: {start} vs end: {end}"
            )));
        } else if rows_needed == 0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let tables = self.tables_or_read(io_stats)?;
        let mut slices_tables = vec![];
        let mut offset_so_far = start;

        for tab in tables.iter() {
            if rows_needed == 0 {
                break;
            }

            let tab_rows = tab.len();

            if offset_so_far >= tab_rows {
                // we can skip the entire table
                offset_so_far -= tab_rows;
            } else if offset_so_far == 0 && rows_needed >= tab_rows {
                // we can take the entire table
                slices_tables.push(tab.clone());
                rows_needed -= tab_rows;
            } else {
                // we need to slice the table
                let tab_end = (offset_so_far + rows_needed).min(tab_rows);
                let sliced = tab.slice(offset_so_far, tab_end)?;
                offset_so_far = 0;
                rows_needed -= sliced.len();
                slices_tables.push(sliced);
            }
        }

        Ok(MicroPartition::new_loaded(
            self.schema.clone(),
            slices_tables.into(),
            self.statistics.clone(),
        ))
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        self.slice(0, num)
    }

    pub fn split_at(&self, idx: usize) -> DaftResult<(Self, Self)> {
        Ok((self.head(idx)?, self.slice(idx, self.len())?))
    }
}
