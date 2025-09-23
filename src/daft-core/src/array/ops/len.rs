use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{DaftArrowBackedType, FileArray},
};

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn size_bytes(&self) -> usize {
        arrow2::compute::aggregate::estimated_bytes_size(self.data())
    }
}

/// From arrow2 private method (arrow2::compute::aggregate::validity_size)
fn validity_size(validity: Option<&arrow2::bitmap::Bitmap>) -> usize {
    validity.as_ref().map(|b| b.as_slice().0.len()).unwrap_or(0)
}

fn offset_size(offsets: &arrow2::offset::OffsetsBuffer<i64>) -> usize {
    offsets.len_proxy() * std::mem::size_of::<i64>()
}

impl FixedSizeListArray {
    pub fn size_bytes(&self) -> usize {
        self.flat_child.size_bytes() + validity_size(self.validity())
    }
}

impl ListArray {
    pub fn size_bytes(&self) -> usize {
        self.flat_child.size_bytes() + validity_size(self.validity()) + offset_size(self.offsets())
    }
}

impl StructArray {
    pub fn size_bytes(&self) -> usize {
        let children_size_bytes: usize = self.children.iter().map(|s| s.size_bytes()).sum();
        children_size_bytes + validity_size(self.validity())
    }
}

impl FileArray {
    pub fn size_bytes(&self) -> usize {
        self.physical.size_bytes()
    }
}
