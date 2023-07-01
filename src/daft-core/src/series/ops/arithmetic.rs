use std::ops::{Add, Div, Mul, Rem, Sub};

use crate::series::Series;

use common_error::DaftResult;

impl Add for &Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        self.inner.add(rhs)
    }
}

impl Add for Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Sub for &Series {
    type Output = DaftResult<Series>;
    fn sub(self, rhs: Self) -> Self::Output {
        self.inner.sub(rhs)
    }
}

impl Sub for Series {
    type Output = DaftResult<Series>;
    fn sub(self, rhs: Self) -> Self::Output {
        (&self).sub(&rhs)
    }
}

impl Mul for &Series {
    type Output = DaftResult<Series>;
    fn mul(self, rhs: Self) -> Self::Output {
        self.inner.mul(rhs)
    }
}

impl Mul for Series {
    type Output = DaftResult<Series>;
    fn mul(self, rhs: Self) -> Self::Output {
        (&self).mul(&rhs)
    }
}

impl Div for &Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        self.inner.div(rhs)
    }
}

impl Div for Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        (&self).div(&rhs)
    }
}

impl Rem for &Series {
    type Output = DaftResult<Series>;
    fn rem(self, rhs: Self) -> Self::Output {
        self.inner.rem(rhs)
    }
}

impl Rem for Series {
    type Output = DaftResult<Series>;
    fn rem(self, rhs: Self) -> Self::Output {
        (&self).rem(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use crate::datatypes::{DataType, Float64Array, Int64Array, Utf8Array};
    use crate::series::IntoSeries;
    use common_error::DaftResult;

    #[test]
    fn add_int_and_int() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Int64Array::from(("b", vec![1, 2, 3]));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Int64);
        Ok(())
    }

    #[test]
    fn add_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn sub_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() - b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }

    #[test]
    fn mul_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() * b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn div_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() / b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn rem_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() % b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn add_int_and_int_full_null() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Int64Array::full_null("b", &DataType::Int64, 3);
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Int64);
        Ok(())
    }
    #[test]
    fn add_int_and_utf8() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let str_array = vec!["a", "b", "c"];
        let b = Utf8Array::from(("b", str_array.as_slice()));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Utf8);
        Ok(())
    }
}
