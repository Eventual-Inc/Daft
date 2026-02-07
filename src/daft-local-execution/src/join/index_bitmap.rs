use daft_core::prelude::{
    BooleanArray,
    bitmap::{Bitmap, MutableBitmap, and},
};
use daft_recordbatch::RecordBatch;

pub(crate) struct IndexBitmapBuilder {
    mutable_bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapBuilder {
    pub fn new(tables: &[RecordBatch]) -> Self {
        Self {
            mutable_bitmaps: tables
                .iter()
                .map(|table| MutableBitmap::from_len_set(table.len()))
                .collect(),
        }
    }

    #[inline]
    pub fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        self.mutable_bitmaps[table_idx].set(row_idx, false);
    }

    pub fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmaps: self
                .mutable_bitmaps
                .into_iter()
                .map(|bitmap| bitmap.into())
                .collect(),
        }
    }
}

pub(crate) struct IndexBitmap {
    bitmaps: Vec<Bitmap>,
}

impl IndexBitmap {
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            bitmaps: self
                .bitmaps
                .iter()
                .zip(other.bitmaps.iter())
                .map(|(a, b)| and(a, b))
                .collect(),
        }
    }

    pub fn negate(&self) -> Self {
        Self {
            bitmaps: self.bitmaps.iter().map(|bitmap| !bitmap).collect(),
        }
    }

    pub fn convert_to_boolean_arrays(self) -> impl Iterator<Item = BooleanArray> {
        self.bitmaps
            .into_iter()
            .map(|bitmap| BooleanArray::from_null_buffer("bitmap", &(bitmap.into())).unwrap())
    }
}
