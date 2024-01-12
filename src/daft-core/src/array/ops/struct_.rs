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

#[cfg(test)]
mod tests {
    use arrow2::bitmap::Bitmap;
    use common_error::DaftResult;

    use crate::{
        array::StructArray,
        datatypes::{Field, Int64Array},
        DataType, IntoSeries,
    };

    #[test]
    fn test_struct_get_invalid() -> DaftResult<()> {
        let child_validity = Bitmap::from(&[true, true, false, false]);
        let parent_validity = Bitmap::from(&[true, false, false, true]);
        let combined_validity = Bitmap::from(&[true, false, false, false]);

        let child = Int64Array::from(("bar", (0..4).collect::<Vec<i64>>()))
            .with_validity(Some(child_validity.clone()))?;

        let mut parent = StructArray::new(
            Field::new(
                "foo",
                DataType::Struct(vec![Field::new("bar", DataType::Int64)]),
            ),
            vec![child.clone().into_series()],
            None,
        );

        let old_child = parent.get("bar")?.i64()?.clone();
        let old_child_validity = old_child
            .validity()
            .expect("Expected old child to have validity map")
            .clone();

        assert_eq!(old_child_validity, child_validity);
        assert_eq!(old_child.get(0), Some(0));
        assert_eq!(old_child.get(1), Some(1));
        assert_eq!(old_child.get(2), None);
        assert_eq!(old_child.get(3), None);

        parent = parent.with_validity(Some(parent_validity.clone()))?;

        let new_child = parent.get("bar")?.i64()?.clone();
        let new_child_validity = new_child
            .validity()
            .expect("Expected new child to have validity map")
            .clone();

        assert_eq!(new_child_validity, combined_validity);
        assert_eq!(new_child.get(0), Some(0));
        assert_eq!(new_child.get(1), None);
        assert_eq!(new_child.get(2), None);
        assert_eq!(new_child.get(3), None);

        Ok(())
    }
}
