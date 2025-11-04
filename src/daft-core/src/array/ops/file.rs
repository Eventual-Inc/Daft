use crate::{
    datatypes::FileArray,
    file::DaftMediaType,
    prelude::{BinaryArray, UInt8Array},
};

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
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
