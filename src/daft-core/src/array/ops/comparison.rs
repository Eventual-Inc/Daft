#![allow(deprecated, reason = "arrow2->arrow migration")]
use std::{
    ops::{BitAnd, BitOr, BitXor, Not},
    sync::Arc,
};

use arrow::{
    array::{ArrowPrimitiveType, Datum, PrimitiveArray},
    buffer::{BooleanBuffer, NullBuffer},
    compute::kernels::cmp,
};
use common_error::{DaftError, DaftResult};
use daft_arrow::ArrowError;
use num_traits::{NumCast, ToPrimitive};

use super::{DaftCompare, DaftLogical, as_arrow::AsArrow};
use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftArrowBackedType, DaftPrimitiveType, DataType, Field,
        FixedSizeBinaryArray, NumericNative, Utf8Array,
    },
};

impl<T> PartialEq for DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.to_arrow().eq(&other.to_arrow())
    }
}

impl<T> DataArray<T> {
    fn compare_op(
        &self,
        rhs: &Self,
        op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
    ) -> DaftResult<BooleanArray> {
        let arrow_arr = match (self.len(), rhs.len()) {
            (1, 1) => op(
                &arrow::array::Scalar::new(self.to_arrow()),
                &arrow::array::Scalar::new(rhs.to_arrow()),
            )?,
            (1, _) => op(&arrow::array::Scalar::new(self.to_arrow()), &rhs.to_arrow())?,
            (_, 1) => op(&self.to_arrow(), &arrow::array::Scalar::new(rhs.to_arrow()))?,
            (l, r) if l == r => op(&self.to_arrow(), &rhs.to_arrow())?,
            (l, r) => {
                return Err(DaftError::ValueError(format!(
                    "trying to compare different length arrays: {}: {l} vs {}: {r}",
                    self.name(),
                    rhs.name()
                )));
            }
        };

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl<T> DaftCompare<&Self> for DataArray<T> {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::eq)
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        if self.data_type().is_null() {
            let len = match (self.len(), rhs.len()) {
                (1, len) | (len, 1) => len,
                (l, r) if l == r => l,
                (l, r) => {
                    return Err(DaftError::ValueError(format!(
                        "trying to compare different length arrays: {}: {l} vs {}: {r}",
                        self.name(),
                        rhs.name()
                    )));
                }
            };

            return Ok(BooleanArray::from_values(
                self.name(),
                std::iter::repeat_n(true, len),
            ));
        }

        let eq_arr = self
            .with_validity(None)?
            .compare_op(&rhs.with_validity(None)?, cmp::eq)?;

        match (self.validity(), rhs.validity()) {
            (Some(l_valid), Some(r_valid)) => {
                let l_valid = BooleanArray::from_null_buffer("", l_valid)?;
                let r_valid = BooleanArray::from_null_buffer("", r_valid)?;

                // (E & L & R) | (!L & !R)
                (eq_arr.and(&l_valid)?.and(&r_valid)?).or(&l_valid.not()?.and(&r_valid.not()?)?)
            }
            (Some(valid), None) | (None, Some(valid)) => {
                let valid = BooleanArray::from_null_buffer("", valid)?;

                eq_arr.and(&valid)
            }
            (None, None) => Ok(eq_arr),
        }
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::neq)
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::lt)
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::lt_eq)
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::gt)
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        self.compare_op(rhs, cmp::gt_eq)
    }
}

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: NumCast,
{
    fn compare_to_scalar(
        &self,
        rhs: impl ToPrimitive,
        op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
    ) -> DaftResult<BooleanArray> {
        let rhs: <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let rhs = PrimitiveArray::<<T::Native as NumericNative>::ARROWTYPE>::new_scalar(rhs);

        let arrow_arr = op(&self.to_arrow(), &rhs)?;

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl<T, Scalar> DaftCompare<Scalar> for DataArray<T>
where
    T: DaftPrimitiveType,
    Scalar: ToPrimitive,
    <<T::Native as NumericNative>::ARROWTYPE as ArrowPrimitiveType>::Native: NumCast,
{
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::eq)
    }

    fn not_equal(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::neq)
    }

    fn lt(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::lt)
    }

    fn lte(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::lt_eq)
    }

    fn gt(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::gt)
    }

    fn gte(&self, rhs: Scalar) -> Self::Output {
        self.compare_to_scalar(rhs, cmp::gt_eq)
    }

    fn eq_null_safe(&self, rhs: Scalar) -> Self::Output {
        // we can simply use the equality kernel then set all nulls to false.
        // this is because the type constraints ensure that the scalar is never null
        let eq_arr = self.compare_to_scalar(rhs, cmp::eq)?;

        Ok(if let Some(validity) = eq_arr.validity() {
            let valid_arr = BooleanArray::from_null_buffer("", validity)?;
            eq_arr.with_validity(None)?.and(&valid_arr)?
        } else {
            eq_arr
        })
    }
}

impl Not for &BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn not(self) -> Self::Output {
        let arrow_arr = arrow::compute::not(&self.as_arrow()?)?;

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow_arr),
        )
    }
}

impl DaftLogical<&Self> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: &Self) -> Self::Output {
        // When performing a logical AND with a NULL value:
        // - If the non-null value is false, the result is false (not null)
        // - If the non-null value is true, the result is null
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.and(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.and(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitand(r_values);

                let validity = match (self.validity(), rhs.validity()) {
                    (None, None) => None,
                    (Some(l_valid), None) => {
                        let l_valid = l_valid.inner();

                        Some(
                            l_valid
                                .bitor(&l_values.not().bitand(l_valid))
                                .bitor(&r_values.not()),
                        )
                    }
                    (None, Some(r_valid)) => {
                        let r_valid = r_valid.inner();

                        Some(
                            r_valid
                                .bitor(&l_values.not())
                                .bitor(&r_values.not().bitand(r_valid)),
                        )
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let l_valid = l_valid.inner();
                        let r_valid = r_valid.inner();

                        Some(
                            (l_valid.bitand(r_valid))
                                .bitor(&l_values.not().bitand(l_valid))
                                .bitor(&r_values.not().bitand(r_valid)),
                        )
                    }
                };

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(
                        values,
                        validity.map(Into::into),
                    )),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        // When performing a logical OR with a NULL value:
        // - If the non-null value is false, the result is null
        // - If the non-null value is true, the result is true (not null)
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.or(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.or(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitor(r_values);

                let validity = match (self.validity(), rhs.validity()) {
                    (None, None) => None,
                    (Some(l_valid), None) => {
                        let l_valid = l_valid.inner();

                        Some(l_valid.bitor(&l_values.bitand(l_valid)).bitor(r_values))
                    }
                    (None, Some(r_valid)) => {
                        let r_valid = r_valid.inner();

                        Some(r_valid.bitor(l_values).bitor(&r_values.bitand(r_valid)))
                    }
                    (Some(l_valid), Some(r_valid)) => {
                        let l_valid = l_valid.inner();
                        let r_valid = r_valid.inner();

                        Some(
                            (l_valid.bitand(r_valid))
                                .bitor(&l_values.bitand(l_valid))
                                .bitor(&r_values.bitand(r_valid)),
                        )
                    }
                };

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(
                        values,
                        validity.map(Into::into),
                    )),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        match (self.len(), rhs.len()) {
            (_, 1) => Ok(self.xor(rhs.get(0))?.rename(self.name())),
            (1, _) => Ok(rhs.xor(self.get(0))?.rename(self.name())),
            (l, r) if l == r => {
                let lhs_arrow = self.as_arrow()?;
                let rhs_arrow = rhs.as_arrow()?;
                let l_values = lhs_arrow.values();
                let r_values = rhs_arrow.values();

                let values = l_values.bitxor(r_values);
                let validity = NullBuffer::union(self.validity(), rhs.validity());

                Self::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow::array::BooleanArray::new(values, validity)),
                )
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftLogical<Option<bool>> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => {
                // keep false values, all true values become nulls
                let false_values = self.as_arrow()?.values().not();
                let validity = if let Some(original_validity) = self.validity() {
                    false_values.bitand(original_validity.inner())
                } else {
                    false_values
                };

                self.with_validity(Some(validity.into()))
            }
            Some(rhs) => self.and(rhs),
        }
    }

    fn or(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => {
                // keep true values, all false values become nulls
                let true_arr = self.as_arrow()?;
                let true_values = true_arr.values();
                let validity = if let Some(original_validity) = self.validity() {
                    true_values.bitand(original_validity.inner())
                } else {
                    true_values.clone()
                };

                self.with_validity(Some(validity.into()))
            }
            Some(rhs) => self.or(rhs),
        }
    }

    fn xor(&self, rhs: Option<bool>) -> Self::Output {
        match rhs {
            None => self.with_validity(Some(NullBuffer::new_null(self.len()))),
            Some(rhs) => self.xor(rhs),
        }
    }
}

impl DaftLogical<bool> for BooleanArray {
    type Output = DaftResult<Self>;
    fn and(&self, rhs: bool) -> Self::Output {
        if rhs {
            Ok(self.clone())
        } else {
            Self::from_arrow(
                Field::new(self.name(), DataType::Boolean),
                Arc::new(arrow::array::BooleanArray::new(
                    BooleanBuffer::new_unset(self.len()),
                    None,
                )),
            )
        }
    }

    fn or(&self, rhs: bool) -> Self::Output {
        if rhs {
            Self::from_arrow(
                Field::new(self.name(), DataType::Boolean),
                Arc::new(arrow::array::BooleanArray::new(
                    BooleanBuffer::new_set(self.len()),
                    None,
                )),
            )
        } else {
            Ok(self.clone())
        }
    }

    fn xor(&self, rhs: bool) -> Self::Output {
        if rhs { self.not() } else { Ok(self.clone()) }
    }
}

/// Macro to implement scalar comparison for byte-like array types (Utf8Array, BinaryArray, FixedSizeBinaryArray)
macro_rules! impl_scalar_compare {
    ($array_type:ty, $scalar_type:ty, $arrow_scalar:ty) => {
        impl $array_type {
            fn compare_to_scalar(
                &self,
                rhs: $scalar_type,
                op: impl Fn(&dyn Datum, &dyn Datum) -> Result<arrow::array::BooleanArray, ArrowError>,
            ) -> DaftResult<BooleanArray> {
                let rhs = <$arrow_scalar>::new_scalar(rhs);
                let arrow_arr = op(&self.to_arrow(), &rhs)?;

                BooleanArray::from_arrow(
                    Field::new(self.name(), DataType::Boolean),
                    Arc::new(arrow_arr),
                )
            }
        }

        impl DaftCompare<$scalar_type> for $array_type {
            type Output = DaftResult<BooleanArray>;

            fn equal(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::eq)
            }

            fn not_equal(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::neq)
            }

            fn lt(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::lt)
            }

            fn lte(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::lt_eq)
            }

            fn gt(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::gt)
            }

            fn gte(&self, rhs: $scalar_type) -> Self::Output {
                self.compare_to_scalar(rhs, cmp::gt_eq)
            }

            fn eq_null_safe(&self, rhs: $scalar_type) -> Self::Output {
                let eq_arr = self.compare_to_scalar(rhs, cmp::eq)?;

                Ok(if let Some(validity) = eq_arr.validity() {
                    let valid_arr = BooleanArray::from_null_buffer("", validity)?;
                    eq_arr.with_validity(None)?.and(&valid_arr)?
                } else {
                    eq_arr
                })
            }
        }
    };
}

impl_scalar_compare!(Utf8Array, &str, arrow::array::LargeStringArray);
impl_scalar_compare!(BinaryArray, &[u8], arrow::array::LargeBinaryArray);
impl_scalar_compare!(
    FixedSizeBinaryArray,
    &[u8],
    arrow::array::FixedSizeBinaryArray
);

#[cfg(test)]
mod tests {
    use common_error::{DaftError, DaftResult};

    use crate::{
        array::ops::{DaftCompare, DaftLogical, full::FullNull},
        datatypes::{
            BinaryArray, BooleanArray, DataType, FixedSizeBinaryArray, Int64Array, NullArray,
            Utf8Array,
        },
    };

    #[test]
    fn equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.lt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.lte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.gt(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.gte(2)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(true)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(false)]);

        let array = array.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_array() -> DaftResult<()> {
        let lhs = Int64Array::arange("a", 1, 4, 1)?;
        let rhs = Int64Array::arange("a", 0, 6, 2)?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity_slice(&[false, true, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_handles_null_alignment() -> DaftResult<()> {
        let lhs = Int64Array::from(("lhs", vec![1, 2, 3, 4]));
        let lhs = lhs.with_validity_slice(&[true, false, true, false])?;
        let rhs = Int64Array::from(("rhs", vec![1, 20, 30, 4]));
        let rhs = rhs.with_validity_slice(&[true, true, false, false])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = Int64Array::from(("lhs", vec![1, 2, 3]));
        let lhs = lhs.with_validity_slice(&[true, false, true])?;
        let rhs = Int64Array::from(("rhs", vec![0]));
        let rhs = rhs.with_validity_slice(&[false])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_broadcast_null_lhs() -> DaftResult<()> {
        let lhs = Int64Array::from(("lhs", vec![0]));
        let lhs = lhs.with_validity_slice(&[false])?;
        let rhs = Int64Array::from(("rhs", vec![1, 2, 3]));
        let rhs = rhs.with_validity_slice(&[true, false, true])?;

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn eq_null_safe_int64_length_mismatch_errors() {
        let lhs = Int64Array::from(("lhs", vec![1, 2, 3]));
        let rhs = Int64Array::from(("rhs", vec![1, 2]));

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn eq_null_safe_boolean_handles_null_alignment() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(true), None, Some(false), None][..]));
        let rhs = BooleanArray::from(("rhs", &[Some(true), Some(false), None, None][..]));

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_boolean_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(true), Some(false), None][..]));
        let rhs = BooleanArray::from(("rhs", &[None][..]));

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(false), Some(true)]);
        Ok(())
    }

    #[test]
    fn boolean_and_handles_nulls() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(true), Some(false), None][..]));
        let rhs = BooleanArray::from(("rhs", &[Some(true), None, Some(true)][..]));

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), None]);
        Ok(())
    }

    #[test]
    fn boolean_or_handles_nulls() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(true), Some(false), None][..]));
        let rhs = BooleanArray::from(("rhs", &[Some(false), None, Some(true)][..]));

        let result: Vec<_> = lhs.or(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn boolean_and_with_null_scalar() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(false), Some(true)][..]));
        let rhs = BooleanArray::from(("rhs", &[None][..]));

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), None]);
        Ok(())
    }

    #[test]
    fn boolean_or_with_null_scalar() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[Some(true), Some(false)][..]));
        let rhs = BooleanArray::from(("rhs", &[None][..]));

        let result: Vec<_> = lhs.or(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(true), None]);
        Ok(())
    }

    #[test]
    fn boolean_and_null_lhs_broadcasts() -> DaftResult<()> {
        let lhs = BooleanArray::from(("lhs", &[None][..]));
        let rhs = BooleanArray::from(("rhs", &[Some(false), Some(true)][..]));

        let result: Vec<_> = lhs.and(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), None]);
        Ok(())
    }

    #[test]
    fn null_array_equal_returns_nulls() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 2);

        let eq: Vec<_> = lhs.equal(&rhs)?.into_iter().collect();
        assert_eq!(eq, vec![None, None]);
        Ok(())
    }

    #[test]
    fn null_array_eq_null_safe() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 2);

        let eq_null_safe: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(eq_null_safe, vec![Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn null_array_equal_broadcasts() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 3);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 1);

        let result: Vec<_> = lhs.equal(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![None, None, None]);
        Ok(())
    }

    #[test]
    fn null_array_eq_null_safe_broadcasts() -> DaftResult<()> {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 3);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 1);

        let eq_null_safe: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(eq_null_safe, vec![Some(true), Some(true), Some(true)]);
        Ok(())
    }

    #[test]
    fn null_array_equal_length_mismatch_errors() {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 3);

        let err = lhs.equal(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn null_array_eq_null_safe_length_mismatch_errors() {
        let lhs = <NullArray as FullNull>::full_null("lhs", &DataType::Null, 2);
        let rhs = <NullArray as FullNull>::full_null("rhs", &DataType::Null, 3);

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }

    #[test]
    fn utf8_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = Utf8Array::from_iter("lhs", vec![Some("a"), None, Some("c"), None].into_iter());
        let rhs = Utf8Array::from_iter("rhs", vec![Some("a"), Some("b"), None, None].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn utf8_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = Utf8Array::from_iter("vals", vec![Some("a"), None, Some("b")].into_iter());

        let result: Vec<_> = array.eq_null_safe("a")?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = BinaryArray::from_iter(
            "lhs",
            vec![Some(&b"aa"[..]), None, Some(&b"cc"[..]), None].into_iter(),
        );
        let rhs = BinaryArray::from_iter(
            "rhs",
            vec![Some(&b"aa"[..]), Some(&b"bb"[..]), None, None].into_iter(),
        );

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = BinaryArray::from_iter(
            "vals",
            vec![Some(&b"aa"[..]), None, Some(&b"bb"[..])].into_iter(),
        );

        let result: Vec<_> = array.eq_null_safe(&b"aa"[..])?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn binary_eq_null_safe_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = BinaryArray::from_iter(
            "lhs",
            vec![Some(&b"aa"[..]), None, Some(&b"cc"[..])].into_iter(),
        );
        let rhs = BinaryArray::from_iter("rhs", vec![None::<&[u8]>].into_iter());

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_handles_nulls() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), None, Some([3u8, 3u8]), None].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8]), None, None].into_iter(),
            2,
        );

        let result: Vec<_> = lhs.eq_null_safe(&rhs).unwrap().into_iter().collect();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_scalar_masks_nulls() -> DaftResult<()> {
        let array = FixedSizeBinaryArray::from_iter(
            "vals",
            vec![Some([1u8, 1u8]), None, Some([2u8, 2u8])].into_iter(),
            2,
        );

        let result: Vec<_> = array.eq_null_safe(&[1u8, 1u8][..])?.into_iter().collect();
        assert_eq!(result, vec![Some(true), Some(false), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_broadcast_null_rhs() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), None, Some([3u8, 3u8])].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter("rhs", vec![None::<&[u8]>].into_iter(), 2);

        let result: Vec<_> = lhs.eq_null_safe(&rhs)?.into_iter().collect();
        assert_eq!(result, vec![Some(false), Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_equal_broadcast_renames_to_lhs() -> DaftResult<()> {
        let lhs = FixedSizeBinaryArray::from_iter("lhs", vec![Some([1u8, 1u8])].into_iter(), 2);
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8])].into_iter(),
            2,
        );

        let result = lhs.equal(&rhs)?;
        assert_eq!(result.name(), "lhs");
        let collected: Vec<_> = result.into_iter().collect();
        assert_eq!(collected, vec![Some(true), Some(false)]);
        Ok(())
    }

    #[test]
    fn fixed_size_binary_eq_null_safe_length_mismatch_errors() {
        let lhs = FixedSizeBinaryArray::from_iter(
            "lhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8])].into_iter(),
            2,
        );
        let rhs = FixedSizeBinaryArray::from_iter(
            "rhs",
            vec![Some([1u8, 1u8]), Some([2u8, 2u8]), Some([3u8, 3u8])].into_iter(),
            2,
        );

        let err = lhs.eq_null_safe(&rhs).unwrap_err();
        match err {
            DaftError::ValueError(msg) => assert!(msg.contains("different length arrays")),
            other => panic!("expected ValueError, got {other:?}"),
        }
    }
}
