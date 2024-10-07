#[cfg(feature = "python")]
pub(super) mod python_fn;
pub mod cast {
    macro_rules! cast_downcast_op {
        ($lhs:expr, $rhs:expr, $ty_expr:expr, $ty_type:ty, $op:ident) => {{
            let lhs = $lhs.cast($ty_expr)?;
            let rhs = $rhs.cast($ty_expr)?;
            let lhs = lhs.downcast::<$ty_type>()?;
            let rhs = rhs.downcast::<$ty_type>()?;
            lhs.$op(rhs)
        }};
    }
    pub(crate) use cast_downcast_op;
}
