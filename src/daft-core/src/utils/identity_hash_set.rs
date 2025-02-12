use std::hash::{BuildHasherDefault, Hash, Hasher};

pub type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;

/// The `IdentityHasher` is a hasher that does not hash at all.
///
/// This will *not* perform another round of hashing.
/// This is useful for when you are already working with hashed
/// values and want to use the abstraction of a `HashSet` or
/// `HashMap` without incurring a performance hit.
#[derive(Default)]
pub struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdentityHasher should be used by u64")
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

#[derive(Eq, PartialEq)]
pub struct IndexHash {
    pub idx: u64,
    pub hash: u64,
}

impl Hash for IndexHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}
