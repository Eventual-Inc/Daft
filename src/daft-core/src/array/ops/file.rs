use common_file::FileReference;

use crate::{
    datatypes::FileArray,
    prelude::{BinaryArray, UInt8Array},
};

#[allow(clippy::len_without_is_empty)]
pub trait AsFileObj {
    fn name(&self) -> &str;
    fn len(&self) -> usize;
    fn as_file(&self, idx: usize) -> Option<FileReference>;
}

impl AsFileObj for FileArray {
    fn name(&self) -> &str {
        FileArray::name(self)
    }

    fn len(&self) -> usize {
        FileArray::len(self)
    }

    fn as_file(&self, idx: usize) -> Option<FileReference> {
        match self.get_lit(idx) {
            crate::lit::Literal::Null => None,
            crate::lit::Literal::File(f) => Some(f),
            _ => unreachable!("unexpected literal type for FileArray"),
        }
    }
}

impl FileArray {
    pub fn discriminant_array(&self) -> UInt8Array {
        self.physical
            .get("discriminant")
            .unwrap()
            .u8()
            .unwrap()
            .clone()
    }

    pub fn data_array(&self) -> BinaryArray {
        self.physical.get("data").unwrap().binary().unwrap().clone()
    }
}
