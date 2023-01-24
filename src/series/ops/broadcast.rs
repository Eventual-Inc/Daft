use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn broadcast(&self, num: usize) -> DaftResult<Series> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Series named: {}",
                self.name()
            )));
        }

        return Ok(self.clone());
    }
}
