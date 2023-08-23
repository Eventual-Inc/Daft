use std::hash::{Hash, Hasher};

pub struct FloatWrapper(pub f64);

impl Hash for FloatWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&u64::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
    }
}
