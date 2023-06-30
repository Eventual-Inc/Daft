pub mod arrow;
pub mod supertype;

#[macro_export]
macro_rules! impl_binary_trait_by_reference {
    ($ty:ty, $trait:ident, $fname:ident) => {
        impl $trait for $ty {
            type Output = DaftResult<$ty>;
            fn $fname(self, other: Self) -> Self::Output {
                (&self).$fname(&other)
            }
        }
    };
}
