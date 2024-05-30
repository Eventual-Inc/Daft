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
        match  (self.len(), lower.len(), upper.len()) {
            (v_size, l_size, u_size) if (v_size == l_size && v_size == u_size) || (l_size == 1 && u_size == 1) => {
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
}
