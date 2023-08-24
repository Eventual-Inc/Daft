use std::hash::{Hash, Hasher};

// An f64 newtype wrapper that implements basic hashability.
pub struct FloatWrapper(pub f64);

impl Hash for FloatWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // This is a super basic hash function that could lead to e.g. different hashes for different
        // NaN representations. Look to crates like https://docs.rs/ordered-float/latest/ordered_float/index.html
        // for a more advanced Hash implementation, if we end up needing it.
        state.write(&u64::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
    }
}
