use num_traits::{NumCast, ToPrimitive};

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{BooleanArray, DaftNumericType},
};

use super::DaftCompare;
use arrow2::compute::comparison::{self, Simd8, Simd8PartialEq, Simd8PartialOrd};

fn comparison_helper<T, Kernel, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    kernel: Kernel,
    operation: F,
) -> DataArray<T>
where
    T: DaftNumericType,
    Kernel: Fn(&PrimitiveArray<T::Native>, &PrimitiveArray<T::Native>) -> PrimitiveArray<T::Native>,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let ca = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => {
            DataArray::from((lhs.name(), Box::new(kernel(lhs.downcast(), rhs.downcast()))))
        }
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => DataArray::full_null(lhs.name(), lhs.len()),
                Some(rhs) => lhs.apply(|lhs| operation(lhs, rhs)),
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => DataArray::full_null(rhs.name(), rhs.len()),
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        _ => panic!("Cannot apply operation on arrays of different lengths"),
    };
    ca
}

impl<T> DaftCompare<&DataArray<T>> for DataArray<T>
where
    T: DaftNumericType,
{
    type Output = BooleanArray;

    fn equal(&self, rhs: &DataArray<T>) -> Self::Output {}
}

impl<T, Scalar> DaftCompare<Scalar> for DataArray<T>
where
    T: DaftNumericType,
    Scalar: ToPrimitive,
    <<T as DaftNumericType>::Native as Simd8>::Simd: Simd8PartialEq,
    <<T as DaftNumericType>::Native as Simd8>::Simd: Simd8PartialOrd,
{
    type Output = BooleanArray;

    fn equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let arrow_result = comparison::primitive::eq_scalar_and_validity(self.downcast(), rhs);
        DataArray::from((self.name(), arrow_result))
    }

    fn not_equal(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let arrow_result = comparison::primitive::neq_scalar_and_validity(self.downcast(), rhs);
        DataArray::from((self.name(), arrow_result))
    }

    fn lt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let validity = match self.downcast().validity() {
            Some(bitmap) => Some(bitmap.clone()),
            None => None,
        };
        let arrow_result =
            comparison::primitive::lt_scalar(self.downcast(), rhs).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }

    fn lte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let validity = match self.downcast().validity() {
            Some(bitmap) => Some(bitmap.clone()),
            None => None,
        };
        let arrow_result =
            comparison::primitive::lt_eq_scalar(self.downcast(), rhs).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }

    fn gt(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let validity = match self.downcast().validity() {
            Some(bitmap) => Some(bitmap.clone()),
            None => None,
        };
        let arrow_result =
            comparison::primitive::gt_scalar(self.downcast(), rhs).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }

    fn gte(&self, rhs: Scalar) -> Self::Output {
        let rhs: T::Native =
            NumCast::from(rhs).expect("could not cast to underlying DataArray type");
        let validity = match self.downcast().validity() {
            Some(bitmap) => Some(bitmap.clone()),
            None => None,
        };
        let arrow_result =
            comparison::primitive::gt_eq_scalar(self.downcast(), rhs).with_validity(validity);
        DataArray::from((self.name(), arrow_result))
    }
}
