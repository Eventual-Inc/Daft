use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};

// An float newtype wrapper that implements basic hashability.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FloatWrapper<T>(pub T);

macro_rules! impl_hash_for_float_wrapper {
    ($T:ident, $UintEquivalent:ident) => {
        impl Hash for FloatWrapper<$T> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                // This is a super basic hash function that could lead to e.g. different hashes for different
                // NaN representations. Look to crates like https://docs.rs/ordered-float/latest/ordered_float/index.html
                // for a more advanced Hash implementation, if we end up needing it.
                state.write(&$UintEquivalent::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        }
    };
}
impl_hash_for_float_wrapper!(f32, u32);
impl_hash_for_float_wrapper!(f64, u64);

impl<T: PartialEq> PartialEq for FloatWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for FloatWrapper<f32> {}
impl Eq for FloatWrapper<f64> {}

impl<T: PartialOrd> PartialOrd for FloatWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}
macro_rules! impl_ord_for_float_wrapper {
    ($T:ident) => {
        impl Ord for FloatWrapper<$T> {
            fn cmp(&self, other: &Self) -> Ordering {
                // This implementation of cmp considers NaNs to be equal to each other, and less than any other value.
                match (self.0.is_nan(), other.0.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    (false, false) => self.0.partial_cmp(&other.0).unwrap(),
                }
            }
        }
    };
}
impl_ord_for_float_wrapper!(f32);
impl_ord_for_float_wrapper!(f64);
