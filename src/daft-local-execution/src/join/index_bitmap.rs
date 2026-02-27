use arrow_buffer::{BooleanBuffer, MutableBuffer, NullBuffer, bit_util};
use daft_core::prelude::BooleanArray;
use daft_recordbatch::RecordBatch;

pub(crate) struct IndexBitmapBuilder {
    mutable_buffers: Vec<(MutableBuffer, usize)>,
}

impl IndexBitmapBuilder {
    pub fn new(tables: &[RecordBatch]) -> Self {
        Self {
            mutable_buffers: tables
                .iter()
                .map(|table| {
                    let len = table.len();
                    let num_bytes = bit_util::ceil(len, 8);
                    let buffer = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                    (buffer, len)
                })
                .collect(),
        }
    }

    #[inline]
    pub fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        let (buf, _) = &mut self.mutable_buffers[table_idx];
        bit_util::unset_bit(buf.as_slice_mut(), row_idx);
    }

    pub fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmaps: self
                .mutable_buffers
                .into_iter()
                .map(|(buffer, len)| BooleanBuffer::new(buffer.into(), 0, len))
                .collect(),
        }
    }
}

pub(crate) struct IndexBitmap {
    bitmaps: Vec<BooleanBuffer>,
}

impl IndexBitmap {
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            bitmaps: self
                .bitmaps
                .iter()
                .zip(other.bitmaps.iter())
                .map(|(a, b)| a & b)
                .collect(),
        }
    }

    pub fn negate(&self) -> Self {
        Self {
            bitmaps: self.bitmaps.iter().map(|bitmap| !bitmap).collect(),
        }
    }

    pub fn convert_to_boolean_arrays(self) -> impl Iterator<Item = BooleanArray> {
        self.bitmaps.into_iter().map(|bitmap| {
            let null_buffer = NullBuffer::new(bitmap);
            BooleanArray::from_null_buffer("bitmap", &null_buffer).unwrap()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create an IndexBitmapBuilder without needing real RecordBatches.
    /// Creates a builder with bitmaps of the given lengths, all bits initially set (unused).
    fn make_builder(lengths: &[usize]) -> IndexBitmapBuilder {
        IndexBitmapBuilder {
            mutable_buffers: lengths
                .iter()
                .map(|&len| {
                    let num_bytes = bit_util::ceil(len, 8);
                    let buffer = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                    (buffer, len)
                })
                .collect(),
        }
    }

    /// Extract boolean values from an IndexBitmap as Vec<Vec<bool>>.
    fn bitmap_to_vecs(bitmap: &IndexBitmap) -> Vec<Vec<bool>> {
        bitmap
            .bitmaps
            .iter()
            .map(|b| b.iter().collect::<Vec<bool>>())
            .collect()
    }

    #[test]
    fn test_new_builder_all_unused() {
        let builder = make_builder(&[4, 8]);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        assert_eq!(vecs[0], vec![true, true, true, true]);
        assert_eq!(vecs[1], vec![true; 8]);
    }

    #[test]
    fn test_mark_used_single() {
        let mut builder = make_builder(&[5]);
        builder.mark_used(0, 2);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        assert_eq!(vecs[0], vec![true, true, false, true, true]);
    }

    #[test]
    fn test_mark_used_multiple() {
        let mut builder = make_builder(&[8]);
        builder.mark_used(0, 0);
        builder.mark_used(0, 3);
        builder.mark_used(0, 7);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        assert_eq!(
            vecs[0],
            vec![false, true, true, false, true, true, true, false]
        );
    }

    #[test]
    fn test_mark_used_across_tables() {
        let mut builder = make_builder(&[4, 4]);
        builder.mark_used(0, 1);
        builder.mark_used(1, 3);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        assert_eq!(vecs[0], vec![true, false, true, true]);
        assert_eq!(vecs[1], vec![true, true, true, false]);
    }

    #[test]
    fn test_merge() {
        let mut builder_a = make_builder(&[4]);
        builder_a.mark_used(0, 1); // [T, F, T, T]
        let bitmap_a = builder_a.build();

        let mut builder_b = make_builder(&[4]);
        builder_b.mark_used(0, 2); // [T, T, F, T]
        let bitmap_b = builder_b.build();

        let merged = bitmap_a.merge(&bitmap_b);
        let vecs = bitmap_to_vecs(&merged);

        // AND: [T, F, F, T]
        assert_eq!(vecs[0], vec![true, false, false, true]);
    }

    #[test]
    fn test_negate() {
        let mut builder = make_builder(&[4]);
        builder.mark_used(0, 0);
        builder.mark_used(0, 2);
        let bitmap = builder.build(); // [F, T, F, T]
        let negated = bitmap.negate(); // [T, F, T, F]
        let vecs = bitmap_to_vecs(&negated);

        assert_eq!(vecs[0], vec![true, false, true, false]);
    }

    #[test]
    fn test_convert_to_boolean_arrays() {
        let mut builder = make_builder(&[4]);
        builder.mark_used(0, 1);
        builder.mark_used(0, 3);
        let bitmap = builder.build(); // [T, F, T, F]

        let arrays: Vec<BooleanArray> = bitmap.convert_to_boolean_arrays().collect();
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0].len(), 4);

        let values: Vec<bool> = (0..4).map(|i| arrays[0].get(i).unwrap()).collect();
        assert_eq!(values, vec![true, false, true, false]);
    }

    #[test]
    fn test_empty_bitmap() {
        let builder = make_builder(&[0]);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        assert_eq!(vecs[0], Vec::<bool>::new());
    }

    #[test]
    fn test_non_byte_aligned_length() {
        // 10 bits — not aligned to byte boundary
        let mut builder = make_builder(&[10]);
        builder.mark_used(0, 9);
        let bitmap = builder.build();
        let vecs = bitmap_to_vecs(&bitmap);

        let mut expected = vec![true; 10];
        expected[9] = false;
        assert_eq!(vecs[0], expected);
    }

    #[test]
    fn test_merge_then_negate() {
        let mut builder_a = make_builder(&[4]);
        builder_a.mark_used(0, 0); // [F, T, T, T]
        let bitmap_a = builder_a.build();

        let mut builder_b = make_builder(&[4]);
        builder_b.mark_used(0, 1); // [T, F, T, T]
        let bitmap_b = builder_b.build();

        let merged = bitmap_a.merge(&bitmap_b); // AND: [F, F, T, T]
        let negated = merged.negate(); // NOT: [T, T, F, F]
        let vecs = bitmap_to_vecs(&negated);

        assert_eq!(vecs[0], vec![true, true, false, false]);
    }
}
