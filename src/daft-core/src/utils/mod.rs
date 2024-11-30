use common_error::{DaftError, DaftResult};

pub mod arrow;
pub mod display;
pub mod dyn_compare;
pub mod identity_hash_set;
pub mod stats;
pub mod supertype;

/// Ensure that the nulls_first parameter is compatible with the descending parameter.
/// TODO: remove this function once nulls_first is implemented.
pub(crate) fn ensure_nulls_first(descending: bool, nulls_first: bool) -> DaftResult<()> {
    if nulls_first != descending {
        return Err(DaftError::NotImplemented(
            "nulls_first is not implemented".to_string(),
        ));
    }
    Ok(())
}

/// Ensure that the nulls_first parameter is compatible with the descending parameter.
/// TODO: remove this function once nulls_first is implemented.
pub(crate) fn ensure_nulls_first_arr(descending: &[bool], nulls_first: &[bool]) -> DaftResult<()> {
    if nulls_first.iter().zip(descending).any(|(a, b)| a != b) {
        return Err(DaftError::NotImplemented(
            "nulls_first is not implemented".to_string(),
        ));
    }

    Ok(())
}
