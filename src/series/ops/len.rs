use crate::{series::Series, with_match_comparable_daft_types};

impl Series {
    pub fn len(&self) -> usize {
        self.data_array.len()
    }

    pub fn size_bytes(&self) -> usize {
        with_match_comparable_daft_types!(self.data_type(), |$T| {
            let downcasted = self.downcast::<$T>().unwrap();
            downcasted.size_bytes()
        })
    }
}
