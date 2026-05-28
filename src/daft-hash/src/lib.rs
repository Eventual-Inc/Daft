use std::{
    hash::{BuildHasher, Hasher},
    str::FromStr,
};

use common_error::DaftError;
use serde::{Deserialize, Serialize};
use sha1::Digest;

/// Computes MD5 hash and returns a hex string
pub fn compute_md5(data: &[u8]) -> String {
    use md5::Md5;
    let result = Md5::digest(data);
    hex_encode(&result)
}

/// Computes SHA-1 hash and returns a hex string
pub fn compute_sha1(data: &[u8]) -> String {
    let result = sha1::Sha1::digest(data);
    hex_encode(&result)
}

/// Computes SHA-2 hash and returns a hex string
/// bit_length supports 224, 256, 384, 512; defaults to 256
pub fn compute_sha2(data: &[u8], bit_length: u32) -> String {
    use sha2::{Sha224, Sha256, Sha384, Sha512};
    match bit_length {
        224 => hex_encode(&Sha224::digest(data)),
        256 | 0 => hex_encode(&Sha256::digest(data)),
        384 => hex_encode(&Sha384::digest(data)),
        512 => hex_encode(&Sha512::digest(data)),
        _ => hex_encode(&Sha256::digest(data)), // Default to SHA-256
    }
}

/// Computes CRC32 and returns an i64 value (Spark-compatible)
pub fn compute_crc32(data: &[u8]) -> i64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as i64
}

/// Computes xxHash64 and returns an i64 value (Spark-compatible)
pub fn compute_xxhash64(data: &[u8], seed: u64) -> i64 {
    xxhash_rust::xxh64::xxh64(data, seed) as i64
}

/// Computes xxHash64 for multi-column combination (Spark-compatible)
pub fn compute_xxhash64_seeded(data: &[u8], seed: i64) -> i64 {
    xxhash_rust::xxh64::xxh64(data, seed as u64) as i64
}

/// Converts a byte array to a hex string
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{:02x}", b);
            s
        })
}

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
        let bytes: [u8; 8] = result[..8]
            .try_into()
            .expect("sha1 digest should be 20 bytes");
        u64::from_le_bytes(bytes)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.state.update(bytes);
    }
}

pub struct XxHash32Hasher {
    inner: xxhash_rust::xxh32::Xxh32,
}

impl Hasher for XxHash32Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }

    fn finish(&self) -> u64 {
        self.inner.digest() as u64
    }
}

pub struct XxHash32BuildHasher {
    seed: u32,
}

impl XxHash32BuildHasher {
    pub fn new(seed: u32) -> Self {
        Self { seed }
    }
}

impl BuildHasher for XxHash32BuildHasher {
    type Hasher = XxHash32Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        XxHash32Hasher {
            inner: xxhash_rust::xxh32::Xxh32::new(self.seed),
        }
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashFunctionKind {
    #[default]
    MurmurHash3,
    XxHash64,
    XxHash32,
    XxHash3_64,
    Sha1,
}

impl FromStr for HashFunctionKind {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "murmurhash3" => Ok(Self::MurmurHash3),
            "xxhash64" => Ok(Self::XxHash64),
            "xxhash32" => Ok(Self::XxHash32),
            "xxhash3_64" | "xxhash" => Ok(Self::XxHash3_64),
            "sha1" => Ok(Self::Sha1),
            _ => Err(DaftError::ValueError(format!(
                "Invalid hash function: {}",
                s
            ))),
        }
    }
}
