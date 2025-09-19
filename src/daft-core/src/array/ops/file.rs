use crate::{
    datatypes::FileArray,
    prelude::{BinaryArray, UInt8Array},
};

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
