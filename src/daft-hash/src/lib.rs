#![feature(split_array)]

use std::{
    hash::{BuildHasher, Hasher},
    str::FromStr,
};

use common_error::DaftError;
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashFunctionKind {
    MurmurHash3,
    XxHash,
    Sha1,
}

impl FromStr for HashFunctionKind {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "murmurhash3" => Ok(Self::MurmurHash3),
            "xxhash" => Ok(Self::XxHash),
            "sha1" => Ok(Self::Sha1),
            _ => Err(DaftError::ValueError(format!(
                "Invalid hash function: {}",
                s
            ))),
        }
    }
}
