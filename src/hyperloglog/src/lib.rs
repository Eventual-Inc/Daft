// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! # HyperLogLog
//!
//! `hyperloglog` is a module that contains a modified version
//! of [redis's implementation](https://github.com/redis/redis/blob/4930d19e70c391750479951022e207e19111eb55/src/hyperloglog.c)
//! with some modification based on strong assumption of usage
//! within datafusion, so that function can
//! be efficiently implemented.
//!
//! Specifically, like Redis's version, this HLL structure uses
//! 2**14 = 16384 registers, which means the standard error is
//! 1.04/(16384**0.5) = 0.8125%. Unlike Redis, the register takes
//! up full [`u8`] size instead of a raw int* and thus saves some
//! tricky bit shifting techniques used in the original version.
//! This results in a memory usage increase from 12Kib to 16Kib.
//! Also only the dense version is adopted, so there's no automatic
//! conversion, largely to simplify the code.
//!
//! This module also borrows some code structure from [pdatastructs.rs](https://github.com/crepererum/pdatastructs.rs/blob/3997ed50f6b6871c9e53c4c5e0f48f431405fc63/src/hyperloglog.rs).
//!
//! Borrowed from [`datafusion`](https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/hyperloglog.rs).
//! Turned into a standalone crate for easier use.
//! - Daft

use std::borrow::Cow;

/// The greater is P, the smaller the error.
const HLL_P: usize = 14_usize;
/// The number of bits of the hash value used determining the number of leading zeros
const HLL_Q: usize = 64_usize - HLL_P;
pub const NUM_REGISTERS: usize = 1_usize << HLL_P;
/// Mask to obtain index into the registers
const HLL_P_MASK: u64 = (NUM_REGISTERS as u64) - 1;

#[derive(Clone, Debug)]
pub struct HyperLogLog<'a> {
    pub registers: Cow<'a, [u8; NUM_REGISTERS]>,
}

impl Default for HyperLogLog<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> HyperLogLog<'a> {
    #[must_use]
    pub fn new_with_byte_slice(slice: &'a [u8]) -> Self {
        assert_eq!(
            slice.len(),
            NUM_REGISTERS,
            "unexpected slice length, expect {}, got {}",
            NUM_REGISTERS,
            slice.len()
        );
        let registers = slice.try_into().unwrap();
        Self {
            registers: Cow::Borrowed(registers),
        }
    }
}

impl HyperLogLog<'_> {
    /// Creates a new, empty HyperLogLog.
    #[must_use]
    pub fn new() -> Self {
        let registers = [0; NUM_REGISTERS];
        Self::new_with_registers(registers)
    }

    /// Creates a HyperLogLog from already populated registers
    /// note that this method should not be invoked in untrusted environment
    /// because the internal structure of registers are not examined.
    #[must_use]
    pub fn new_with_registers(registers: [u8; NUM_REGISTERS]) -> Self {
        Self {
            registers: Cow::Owned(registers),
        }
    }

    pub fn add_already_hashed(&mut self, already_hashed: u64) {
        let registers = self.registers.to_mut();
        let index = (already_hashed & HLL_P_MASK) as usize;
        let p = ((already_hashed >> HLL_P) | (1_u64 << HLL_Q)).trailing_zeros() + 1;
        registers[index] = registers[index].max(p as u8);
    }

    /// Get the register histogram (each value in register index into
    /// the histogram; u32 is enough because we only have 2**14=16384 registers
    #[inline]
    fn get_histogram(&self) -> [u32; HLL_Q + 2] {
        let mut histogram = [0; HLL_Q + 2];
        // hopefully this can be unrolled
        for &r in self.registers.as_ref() {
            histogram[r as usize] += 1;
        }
        histogram
    }

    /// Merge the other [`HyperLogLog`] into this one
    pub fn merge(&mut self, other: &HyperLogLog) {
        assert!(
            self.registers.len() == other.registers.len(),
            "unexpected got unequal register size, expect {}, got {}",
            self.registers.len(),
            other.registers.len()
        );
        let length = self.registers.len();
        let registers = self.registers.to_mut();
        let other_registers = other.registers.as_ref();
        for i in 0..length {
            registers[i] = registers[i].max(other_registers[i]);
        }
    }

    /// Guess the number of unique elements seen by the HyperLogLog.
    #[must_use]
    pub fn count(&self) -> usize {
        let histogram = self.get_histogram();
        let m = NUM_REGISTERS as f64;
        let mut z = m * hll_tau((m - histogram[HLL_Q + 1] as f64) / m);
        for i in histogram[1..=HLL_Q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * hll_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
}

/// Helper function sigma as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        f64::INFINITY
    } else {
        let mut y = 1.0;
        let mut z = x;
        let mut x = x;
        loop {
            x *= x;
            let z_prime = z;
            z += x * y;
            y += y;
            if z_prime == z {
                break;
            }
        }
        z
    }
}

/// Helper function tau as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        0.0
    } else {
        let mut y = 1.0;
        let mut z = 1.0 - x;
        let mut x = x;
        loop {
            x = x.sqrt();
            let z_prime = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
    }
}

impl AsRef<[u8]> for HyperLogLog<'_> {
    fn as_ref(&self) -> &[u8] {
        self.registers.as_ref()
    }
}

impl Extend<u64> for HyperLogLog<'_> {
    fn extend<T: IntoIterator<Item = u64>>(&mut self, iter: T) {
        for elem in iter {
            self.add_already_hashed(elem);
        }
    }
}
