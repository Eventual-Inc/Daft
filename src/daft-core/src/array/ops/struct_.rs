use common_error::{DaftError, DaftResult};

use crate::{array::StructArray, Series};

impl StructArray {
    pub fn get(&self, name: &str) -> DaftResult<Series> {
        for child in &self.children {
            if child.name() == name {
                return match self.validity() {
                    Some(valid) => {
                        let all_valid = match child.validity() {
                            Some(child_valid) => child_valid & valid,
                            None => valid.clone(),
                        };

                        child.with_validity(Some(all_valid))
                    }
                    None => Ok(child.clone()),
                };
            }
        }

        Err(DaftError::FieldNotFound(format!(
            "Field {} not found in schema: {:?}",
            name,
            self.children
                .iter()
                .map(|c| c.name())
                .collect::<Vec<&str>>()
        )))
    }
}
