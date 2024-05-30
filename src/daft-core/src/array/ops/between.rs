use super::{DaftBetween, DaftCompare, DaftLogical};
use crate::{
    array::DataArray,
    datatypes::{BooleanArray, DaftNumericType},
};
use common_error::{DaftError, DaftResult};

impl<T> DaftBetween<&DataArray<T>, &DataArray<T>> for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<BooleanArray>;

    fn between(&self, lower: &DataArray<T>, upper: &DataArray<T>) -> Self::Output {
        let are_two_equal_and_single_one = |v_size, l_size, u_size: usize| {
            [v_size, l_size, u_size]
                .iter()
                .filter(|&&size| size != 1)
                .collect::<std::collections::HashSet<_>>()
                .len()
                == 1
        };
        match  (self.len(), lower.len(), upper.len()) {
            (v_size, l_size, u_size) if (v_size == l_size && v_size == u_size) || (l_size == 1 && u_size == 1) || (are_two_equal_and_single_one(v_size, l_size, u_size)) => {
                let gte_res = self.gte(lower)?;
                let lte_res = self.lte(upper)?;
                gte_res.and(&lte_res)
            },
            (v_size, l_size, u_size) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {v_size} vs {}: {l_size} vs {}: {u_size}",
                self.name(),
                lower.name(),
                upper.name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{array::ops::DaftBetween, datatypes::Int64Array};
    use common_error::DaftResult;

    #[test]
    fn test_between_two_arrays_of_same_size() -> DaftResult<()> {
        let value = Int64Array::arange("value", 1, 4, 1)?;
        let lower = Int64Array::arange("lower", 0, 6, 2)?;
        let upper = Int64Array::arange("upper", -2, 8, 4)?;
        let result: Vec<_> = value.between(&lower, &upper)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn test_between_array_with_multiple_items_and_array_with_single_item() -> DaftResult<()> {
        let value = Int64Array::arange("value", 1, 4, 1)?;
        let lower = Int64Array::arange("lower", 1, 4, 1)?;
        let upper = Int64Array::arange("upper", 1, 2, 1)?;
        let result: Vec<_> = value.between(&lower, &upper)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn test_between_two_arrays_with_single_item() -> DaftResult<()> {
        let value = Int64Array::arange("value", 1, 4, 1)?;
        let lower = Int64Array::arange("lower", 1, 2, 1)?;
        let upper = Int64Array::arange("upper", 1, 2, 1)?;
        let result: Vec<_> = value.between(&lower, &upper)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);
        Ok(())
    }
}
