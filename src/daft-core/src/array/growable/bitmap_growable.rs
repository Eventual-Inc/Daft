pub struct ArrowBitmapGrowable<'a> {
    bitmap_refs: Vec<Option<&'a daft_arrow::buffer::NullBuffer>>,
    mutable_bitmap: daft_arrow::buffer::NullBufferBuilder,
}

impl<'a> ArrowBitmapGrowable<'a> {
    pub fn new(
        bitmap_refs: Vec<Option<&'a daft_arrow::buffer::NullBuffer>>,
        capacity: usize,
    ) -> Self {
        Self {
            bitmap_refs,
            mutable_bitmap: daft_arrow::buffer::NullBufferBuilder::new(capacity),
        }
    }

    pub fn extend(&mut self, index: usize, start: usize, len: usize) {
        let bm = self.bitmap_refs.get(index).unwrap();
        match bm {
            None => self.mutable_bitmap.append_n_non_nulls(len),
            Some(bm) => {
                bm.slice(start, len).iter().for_each(|b| {
                    self.mutable_bitmap.append(b);
                });
            }
        }
    }

    pub fn add_nulls(&mut self, additional: usize) {
        self.mutable_bitmap.append_n_nulls(additional);
    }

    pub fn build(mut self) -> Option<daft_arrow::buffer::NullBuffer> {
        self.mutable_bitmap.finish()
    }
}

impl Default for ArrowBitmapGrowable<'_> {
    fn default() -> Self {
        ArrowBitmapGrowable::new(vec![], 0)
    }
}
