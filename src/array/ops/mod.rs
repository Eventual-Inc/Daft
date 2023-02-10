mod apply;
mod arange;
mod arithmetic;
mod broadcast;
mod cast;
mod comparison;
mod downcast;
mod full;
mod len;
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
