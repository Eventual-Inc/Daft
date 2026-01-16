use common_error::{DaftError, DaftResult};
use daft_arrow::buffer::NullBuffer;

use crate::{array::StructArray, series::Series};

impl StructArray {
    pub fn get(&self, name: &str) -> DaftResult<Series> {
        for child in &self.children {
            if child.name() == name {
                return match self.nulls() {
                    Some(valid) => {
                        let all_valid = NullBuffer::union(child.nulls(), Some(valid));
                        child.with_nulls(all_valid)
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

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_arrow::buffer::NullBuffer;

    use crate::prelude::*;

    #[test]
    fn test_struct_get_invalid() -> DaftResult<()> {
        let child_nulls = NullBuffer::from(&[true, true, false, false]);
        let parent_nulls = NullBuffer::from(&[true, false, false, true]);
        let combined_nulls = NullBuffer::from(&[true, false, false, false]);

        let child = Int64Array::from(("bar", (0..4).collect::<Vec<i64>>()))
            .with_nulls(Some(child_nulls.clone()))?;

        let mut parent = StructArray::new(
            Field::new(
                "foo",
                DataType::Struct(vec![Field::new("bar", DataType::Int64)]),
            ),
            vec![child.into_series()],
            None,
        );

        let old_child = parent.get("bar")?.i64()?.clone();
        let old_child_nulls = old_child
            .nulls()
            .expect("Expected old child to have validity map")
            .clone();

        assert_eq!(old_child_nulls, child_nulls);
        assert_eq!(old_child.get(0), Some(0));
        assert_eq!(old_child.get(1), Some(1));
        assert_eq!(old_child.get(2), None);
        assert_eq!(old_child.get(3), None);

        parent = parent.with_nulls(Some(parent_nulls))?;

        let new_child = parent.get("bar")?.i64()?.clone();
        let new_child_nulls = new_child
            .nulls()
            .expect("Expected new child to have validity map")
            .clone();

        assert_eq!(new_child_nulls, combined_nulls);
        assert_eq!(new_child.get(0), Some(0));
        assert_eq!(new_child.get(1), None);
        assert_eq!(new_child.get(2), None);
        assert_eq!(new_child.get(3), None);

        Ok(())
    }
}
