mod abs;
mod apply;
mod arange;
mod arithmetic;
mod arrow2;
mod broadcast;
mod cast;
mod compare_agg;
mod comparison;
mod count;
mod downcast;
mod filter;
mod full;
mod hash;
mod len;
mod mean;
mod pairwise;
mod search_sorted;
mod sort;
mod sum;
mod take;

pub trait DaftCompare<Rhs> {
    type Output;

    /// equality.
    fn equal(&self, rhs: Rhs) -> Self::Output;

    /// inequality.
    fn not_equal(&self, rhs: Rhs) -> Self::Output;

    /// Greater than
    fn gt(&self, rhs: Rhs) -> Self::Output;

    /// Greater than or equal
    fn gte(&self, rhs: Rhs) -> Self::Output;

    /// Less than
    fn lt(&self, rhs: Rhs) -> Self::Output;

    /// Less than or equal
    fn lte(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftLogical<Rhs> {
    type Output;

    /// and.
    fn and(&self, rhs: Rhs) -> Self::Output;

    /// or.
    fn or(&self, rhs: Rhs) -> Self::Output;

    /// xor.
    fn xor(&self, rhs: Rhs) -> Self::Output;
}

pub trait DaftCountAggable {
    type Output;
    fn count(&self) -> Self::Output;
}

pub trait DaftSumAggable {
    type Output;
    fn sum(&self) -> Self::Output;
}

pub trait DaftMeanAggable {
    type Output;
    fn mean(&self) -> Self::Output;
}

pub trait DaftCompareAggable {
    type Output;
    fn min(&self) -> Self::Output;
    fn max(&self) -> Self::Output;
}
