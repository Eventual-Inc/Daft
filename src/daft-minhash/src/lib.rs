#![feature(test)]
#![feature(portable_simd)]
#![feature(iter_next_chunk)]
#![feature(iter_array_chunks)]
#![feature(split_array)]

mod minhash;

use std::hash::BuildHasher;

pub use minhash::{load_simd, minhash};

// todo: move to another crate
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
