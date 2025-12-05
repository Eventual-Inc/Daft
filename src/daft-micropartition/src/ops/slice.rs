use common_error::{DaftError, DaftResult};

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let mut rows_needed = end.min(self.len()) - start.min(self.len());

        if start > end {
            return Err(DaftError::ValueError(format!(
                "Trying to slice MicroPartition with negative length, start: {start} vs end: {end}"
            )));
        } else if rows_needed == 0 {
            return Ok(Self::empty(Some(self.schema.clone())));
        }

        let mut slices_tables = vec![];
        let mut offset_so_far = start;

        for tab in self.record_batches() {
            if rows_needed == 0 {
                break;
            }

            let tab_rows = tab.len();
            if offset_so_far > 0 && offset_so_far >= tab_rows {
                offset_so_far -= tab_rows;
                continue;
            }

            if offset_so_far == 0 && rows_needed >= tab_rows {
                slices_tables.push(tab.clone());
                rows_needed -= tab_rows;
            } else {
                let new_end = (rows_needed + offset_so_far).min(tab_rows);
                let sliced = tab.slice(offset_so_far, new_end)?;
                offset_so_far = 0;
                rows_needed -= sliced.len();
                slices_tables.push(sliced);
            }
        }

        Ok(Self::new_loaded(
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
