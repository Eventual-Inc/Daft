#![feature(test)]
#![feature(portable_simd)]

mod minhash;
pub use minhash::{load_simd, minhash};
