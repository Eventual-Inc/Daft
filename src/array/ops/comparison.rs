use num_traits::{NumCast, ToPrimitive};

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BooleanArray, DaftArrowBackedType, DaftNumericType, DataType, NullArray, Utf8Array,
    },
    error::{DaftError, DaftResult},
    utils::arrow::arrow_bitmap_and_helper,
};

use std::ops::Not;

use super::{DaftCompare, DaftLogical};

use super::downcast::Downcastable;
use arrow2::{compute::comparison, scalar::PrimitiveScalar};

impl<T> PartialEq for DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        arrow2::array::equal(self.data(), other.data())
    }
}

impl<T> DaftCompare<&DataArray<T>> for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.not_equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.not_equal(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.lt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.gt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.lte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.gte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.gt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.lt(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &DataArray<T>) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    Ok(self.gte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    Ok(rhs.lte(value))
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    fn compare_to_scalar(
        &self,
        rhs: T::Native,
        func: impl Fn(
            &dyn arrow2::array::Array,
            &dyn arrow2::scalar::Scalar,
        ) -> arrow2::array::BooleanArray,
    ) -> BooleanArray {
        let arrow_array = self.downcast();

        let scalar = PrimitiveScalar::new(arrow_array.data_type().clone(), Some(rhs));

        let validity = self.downcast().validity().cloned();

        let arrow_result = func(self.downcast(), &scalar).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }
}

impl<T, Scalar> DaftCompare<Scalar> for DataArray<T>
where
    T: DaftNumericType,
    Scalar: ToPrimitive,
{
    type Output = BooleanArray;

    fn equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::eq_scalar)
    }

    fn not_equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::neq_scalar)
    }

    fn lt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::lt_scalar)
    }

    fn lte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::lt_eq_scalar)
    }

    fn gt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::gt_scalar)
    }

    fn gte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        self.compare_to_scalar(rhs, comparison::gt_eq_scalar)
    }
}

impl DaftCompare<&BooleanArray> for BooleanArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<bool> for BooleanArray {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn not_equal(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::neq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lt(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::lt_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lte(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::lt_eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gt(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::gt_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gte(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::boolean::gt_eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }
}

impl Not for &BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn not(self) -> Self::Output {
        let new_bitmap = self.downcast().values().not();
        let arrow_array = arrow2::array::BooleanArray::new(
            arrow2::datatypes::DataType::Boolean,
            new_bitmap,
            self.downcast().validity().cloned(),
        );
        Ok(BooleanArray::from((self.name(), arrow_array)))
    }
}

impl DaftLogical<&BooleanArray> for BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn and(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());

                let result_bitmap =
                    arrow2::bitmap::and(self.downcast().values(), rhs.downcast().values());
                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.and(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.and(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn or(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());

                let result_bitmap =
                    arrow2::bitmap::or(self.downcast().values(), rhs.downcast().values());
                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.or(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.or(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn xor(&self, rhs: &BooleanArray) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());

                let result_bitmap =
                    arrow2::bitmap::xor(self.downcast().values(), rhs.downcast().values());
                Ok(BooleanArray::from((
                    self.name(),
                    arrow2::array::BooleanArray::new(
                        arrow2::datatypes::DataType::Boolean,
                        result_bitmap,
                        validity,
                    ),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.xor(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.xor(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

macro_rules! null_array_comparison_method {
    ($func_name:ident) => {
        fn $func_name(&self, rhs: &NullArray) -> Self::Output {
            match (self.len(), rhs.len()) {
                (x, y) if x == y => Ok(BooleanArray::full_null(self.name(), &DataType::Boolean, x)),
                (l_size, 1) => Ok(BooleanArray::full_null(
                    self.name(),
                    &DataType::Boolean,
                    l_size,
                )),
                (1, r_size) => Ok(BooleanArray::full_null(
                    self.name(),
                    &DataType::Boolean,
                    r_size,
                )),
                (l, r) => Err(DaftError::ValueError(format!(
                    "trying to compare different length arrays: {}: {l} vs {}: {r}",
                    self.name(),
                    rhs.name()
                ))),
            }
        }
    };
}

impl DaftCompare<&NullArray> for NullArray {
    type Output = DaftResult<BooleanArray>;
    null_array_comparison_method!(equal);
    null_array_comparison_method!(not_equal);
    null_array_comparison_method!(lt);
    null_array_comparison_method!(lte);
    null_array_comparison_method!(gt);
    null_array_comparison_method!(gte);
}

impl DaftLogical<bool> for BooleanArray {
    type Output = DaftResult<BooleanArray>;
    fn and(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity();
        if rhs {
            Ok(self.clone())
        } else {
            use arrow2::{array, bitmap::Bitmap, datatypes::DataType};
            let arrow_array = array::BooleanArray::new(
                DataType::Boolean,
                Bitmap::new_zeroed(self.len()),
                validity.cloned(),
            );
            return Ok(BooleanArray::from((self.name(), arrow_array)));
        }
    }

    fn or(&self, rhs: bool) -> Self::Output {
        let validity = self.downcast().validity();
        if rhs {
            use arrow2::{array, bitmap::Bitmap, datatypes::DataType};
            let arrow_array = array::BooleanArray::new(
                DataType::Boolean,
                Bitmap::new_zeroed(self.len()).not(),
                validity.cloned(),
            );
            return Ok(BooleanArray::from((self.name(), arrow_array)));
        } else {
            Ok(self.clone())
        }
    }

    fn xor(&self, rhs: bool) -> Self::Output {
        if rhs {
            self.not()
        } else {
            Ok(self.clone())
        }
    }
}

impl DaftCompare<&Utf8Array> for Utf8Array {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn not_equal(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::neq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.not_equal(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lt(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn lte(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::lt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gt(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lt(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }

    fn gte(&self, rhs: &Utf8Array) -> Self::Output {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let validity =
                    arrow_bitmap_and_helper(self.downcast().validity(), rhs.downcast().validity());
                Ok(BooleanArray::from((
                    self.name(),
                    comparison::gt_eq(self.downcast(), rhs.downcast()).with_validity(validity),
                )))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.gte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    rhs.lte(value)
                } else {
                    Ok(BooleanArray::full_null(
                        self.name(),
                        &DataType::Boolean,
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to compare different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}

impl DaftCompare<&str> for Utf8Array {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn not_equal(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::neq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lt(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::lt_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn lte(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::lt_eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gt(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::gt_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }

    fn gte(&self, rhs: &str) -> Self::Output {
        let validity = self.downcast().validity().cloned();
        let arrow_result =
            comparison::utf8::gt_eq_scalar(self.downcast(), rhs).with_validity(validity);

        Ok(BooleanArray::from((self.name(), arrow_result)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{array::ops::DaftCompare, datatypes::Int64Array, error::DaftResult};

    #[test]
    fn equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(false)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn not_equal_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.not_equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(true)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.not_equal(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn lt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lt(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(false), Some(false)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.lt(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn lte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.lte(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(false)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.lte(2).into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);
        Ok(())
    }

    #[test]
    fn gt_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gt(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(false), Some(true)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.gt(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn gte_int64_array_with_scalar() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.gte(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), Some(true), Some(true)]);

        let array = array.with_validity(&[true, false, true])?;
        let result: Vec<_> = array.gte(2).into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);
        Ok(())
    }

    #[test]
    fn equal_int64_array_with_same_array() -> DaftResult<()> {
        let array = Int64Array::arange("a", 1, 4, 1)?;
        assert_eq!(array.len(), 3);
        let result: Vec<_> = array.equal(&array)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), Some(true), Some(true)]);

        let array = array.with_validity(&[true, false, true])?;
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

        let array = array.with_validity(&[true, false, true])?;
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

        let lhs = lhs.with_validity(&[true, false, true])?;
        let result: Vec<_> = lhs.lt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity(&[false, true, true])?;
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

        let lhs = lhs.with_validity(&[true, false, true])?;
        let result: Vec<_> = lhs.lte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(false), None, Some(true)]);

        let rhs = rhs.with_validity(&[false, true, true])?;
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

        let lhs = lhs.with_validity(&[true, false, true])?;
        let result: Vec<_> = lhs.gt(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity(&[false, true, true])?;
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

        let lhs = lhs.with_validity(&[true, false, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [Some(true), None, Some(false)]);

        let rhs = rhs.with_validity(&[false, true, true])?;
        let result: Vec<_> = lhs.gte(&rhs)?.into_iter().collect();
        assert_eq!(result[..], [None, None, Some(false)]);
        Ok(())
    }
}
