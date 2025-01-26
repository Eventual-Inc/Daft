use crate::error::Error;

use super::super::uleb128;
use super::{super::ceil8, HybridEncoded};

/// An [`Iterator`] of [`HybridEncoded`].
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    values: &'a [u8],
    num_bits: usize,
}

impl<'a> Decoder<'a> {
    /// Returns a new [`Decoder`]
    pub fn new(values: &'a [u8], num_bits: usize) -> Self {
        Self { values, num_bits }
    }

    /// Returns the number of bits being used by this decoder.
    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = Result<HybridEncoded<'a>, Error>;

    #[inline] // -18% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.num_bits == 0 {
            return None;
        }

        if self.values.is_empty() {
            return None;
        }

        let (indicator, consumed) = match uleb128::decode(self.values) {
            Ok((indicator, consumed)) => (indicator, consumed),
            Err(e) => return Some(Err(e)),
        };
        self.values = &self.values[consumed..];
        if self.values.is_empty() {
            return None;
        };

        if indicator & 1 == 1 {
            // is bitpacking
            let bytes = (indicator as usize >> 1) * self.num_bits;
            let bytes = std::cmp::min(bytes, self.values.len());
            let (result, remaining) = self.values.split_at(bytes);
            self.values = remaining;
            Some(Ok(HybridEncoded::Bitpacked(result)))
        } else {
            // is rle
            let run_length = indicator as usize >> 1;
            // repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
            let rle_bytes = ceil8(self.num_bits);
            let (result, remaining) = self.values.split_at(rle_bytes);
            self.values = remaining;
            Some(Ok(HybridEncoded::Rle(result, run_length)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::super::bitpacked;

    #[test]
    fn basics_1() {
        let bit_width = 1usize;
        let length = 5;
        let values = [
            2, 0, 0, 0, // length
            0b00000011, 0b00001011,
        ];

        let mut decoder = Decoder::new(&values[4..6], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            assert_eq!(values, &[0b00001011]);
            let result = bitpacked::Decoder::<u32>::try_new(values, bit_width, length)
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, &[1, 1, 0, 1, 0]);
        } else {
            panic!()
        };
    }

    #[test]
    fn basics_2() {
        // This test was validated by the result of what pyarrow3 outputs when
        // the bitmap is used.
        let bit_width = 1;
        let values = [
            3, 0, 0, 0, // length
            0b00000101, 0b11101011, 0b00000010,
        ];
        let expected = &[1, 1, 0, 1, 0, 1, 1, 1, 0, 1];

        let mut decoder = Decoder::new(&values[4..4 + 3], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            assert_eq!(values, &[0b11101011, 0b00000010]);
            let result = bitpacked::Decoder::<u32>::try_new(values, bit_width, 10)
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expected);
        } else {
            panic!()
        };
    }

    #[test]
    fn basics_3() {
        let bit_width = 1;
        let length = 8;
        let values = [
            2, 0, 0, 0,          // length
            0b00010000, // data
            0b00000001,
        ];

        let mut decoder = Decoder::new(&values[4..4 + 2], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Rle(values, items) = run.unwrap() {
            assert_eq!(values, &[0b00000001]);
            assert_eq!(items, length);
        } else {
            panic!()
        };
    }

    #[test]
    fn test_bool_bitpacked() {
        let bit_width = 1usize;
        let length = 4;
        let values = [
            2, 0, 0, 0, // Length indicator as u32.
            0b00000011, // Bitpacked indicator with 1 value (1 << 1 | 1).
            0b00001101, // Values (true, false, true, true).
        ];
        let expected = &[1, 0, 1, 1];

        let mut decoder = Decoder::new(&values[4..], bit_width);

        if let Ok(HybridEncoded::Bitpacked(values)) = decoder.next().unwrap() {
            assert_eq!(values, &[0b00001101]);

            let result = bitpacked::Decoder::<u8>::try_new(values, bit_width, length)
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expected);
        } else {
            panic!("Expected bitpacked encoding");
        }
    }

    #[test]
    fn test_bool_rle() {
        let bit_width = 1usize;
        let length = 4;
        let values = [
            2, 0, 0, 0, // Length indicator as u32.
            0b00001000, // RLE indicator (4 << 1 | 0).
            true as u8  // Value to repeat.
        ];

        let mut decoder = Decoder::new(&values[4..], bit_width);

        if let Ok(HybridEncoded::Rle(value, run_length)) = decoder.next().unwrap() {
            assert_eq!(value, &[1u8]);  // true encoded as 1.
            assert_eq!(run_length, length); // Repeated 4 times.
        } else {
            panic!("Expected RLE encoding");
        }
    }

    #[test]
    fn test_bool_mixed_rle() {
        let bit_width = 1usize;
        let values = [
            4, 0, 0, 0, // Length indicator as u32.
            0b00000011, // Bitpacked indicator with 1 value (1 << 1 | 1).
            0b00001101, // Values (true, false, true, true).
            0b00001000, // RLE indicator (4 << 1 | 0)
            false as u8 // RLE value
        ];

        let mut decoder = Decoder::new(&values[4..], bit_width);

        // Decode bitpacked values.
        if let Ok(HybridEncoded::Bitpacked(values)) = decoder.next().unwrap() {
            assert_eq!(values, &[0b00001101]);
        } else {
            panic!("Expected bitpacked encoding");
        }

        // Decode RLE values.
        if let Ok(HybridEncoded::Rle(value, run_length)) = decoder.next().unwrap() {
            assert_eq!(value, &[0u8]); // false encoded as 0.
            assert_eq!(run_length, 4);
        } else {
            panic!("Expected RLE encoding");
        }
    }

    #[test]
    fn test_bool_nothing_encoded() {
        let bit_width = 1usize;
        let values = [0, 0, 0, 0]; // Length indicator only.

        let mut decoder = Decoder::new(&values[4..], bit_width);
        assert!(decoder.next().is_none());
    }

    #[test]
    fn test_bool_invalid_encoding() {
        let bit_width = 1usize;
        let values = [
            2, 0, 0, 0, // Length indicator as u32.
            0b00000101, // Bitpacked indicator with 1 value (2 << 1 | 1).
            true as u8 // Incomplete encoding (should have another u8).
        ];

        let mut decoder = Decoder::new(&values[4..], bit_width);

        if let Ok(HybridEncoded::Bitpacked(values)) = decoder.next().unwrap() {
            assert_eq!(values, &[1u8]);
        } else {
            panic!("Expected bitpacked encoding");
        }

        // Next call should return None since we've exhausted the buffer.
        assert!(decoder.next().is_none());
    }
}
