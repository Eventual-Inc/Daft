pub struct ArrowBitmapGrowable<'a> {
    bitmap_refs: Vec<Option<&'a arrow2::bitmap::Bitmap>>,
    mutable_bitmap: arrow2::bitmap::MutableBitmap,
}

impl<'a> ArrowBitmapGrowable<'a> {
    pub fn new(bitmap_refs: Vec<Option<&'a arrow2::bitmap::Bitmap>>, capacity: usize) -> Self {
        Self {
            bitmap_refs,
            mutable_bitmap: arrow2::bitmap::MutableBitmap::with_capacity(capacity),
        }
    }

    pub fn extend(&mut self, index: usize, start: usize, len: usize) {
        let bm = self.bitmap_refs.get(index).unwrap();
        match bm {
            None => self.mutable_bitmap.extend_constant(len, true),
            Some(bm) => {
                let (bm_data, bm_start, _bm_len) = bm.as_slice();
                self.mutable_bitmap
                    .extend_from_slice(bm_data, bm_start + start, len)
            }
        }
    }

    pub fn add_nulls(&mut self, additional: usize) {
        self.mutable_bitmap.extend_constant(additional, false)
    }

    pub fn build(self) -> arrow2::bitmap::Bitmap {
        self.mutable_bitmap.clone().into()
    }
}

impl<'a> Default for ArrowBitmapGrowable<'a> {
    fn default() -> Self {
        ArrowBitmapGrowable::new(vec![], 0)
    }
}
