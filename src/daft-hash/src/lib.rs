#![feature(split_array)]

use std::hash::{BuildHasher, Hasher};

use sha1::Digest;

pub struct MurBuildHasher {
    seed: u32,
}

impl Default for MurBuildHasher {
    fn default() -> Self {
        Self::new(42)
    }
}

impl MurBuildHasher {
    pub fn new(seed: u32) -> Self {
        Self { seed }
    }
}

impl BuildHasher for MurBuildHasher {
    type Hasher = mur3::Hasher32;

    fn build_hasher(&self) -> Self::Hasher {
        mur3::Hasher32::with_seed(self.seed)
    }
}

#[derive(Default)]
pub struct Sha1Hasher {
    state: sha1::Sha1,
}

impl Hasher for Sha1Hasher {
    fn finish(&self) -> u64 {
        let result = self.state.clone().finalize();
        let (&result, _) = result.0.split_array_ref::<8>();
        u64::from_le_bytes(result)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.state.update(bytes);
    }
}
