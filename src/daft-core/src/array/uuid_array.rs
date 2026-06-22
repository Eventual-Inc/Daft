use uuid::{Bytes, Uuid};

use crate::{
    array::{DataArray, iterator::GenericArrayIter},
    prelude::{FixedSizeBinaryType, LogicalArray, UuidType},
};

pub type UuidArray = LogicalArray<UuidType>;

pub struct UuidArrayIter<'a> {
    physical_iter: GenericArrayIter<'a, DataArray<FixedSizeBinaryType>, &'a [u8]>,
}

impl<'a> Iterator for UuidArrayIter<'a> {
    type Item = Option<&'a Uuid>;

    fn next(&mut self) -> Option<Self::Item> {
        self.physical_iter.next().map(|opt_bytes| {
            opt_bytes.map(|bytes| {
                let bytes: &Bytes = unsafe { &*(bytes.as_ptr().cast::<Bytes>()) };
                Uuid::from_bytes_ref(bytes)
            })
        })
    }
}

impl<'a> IntoIterator for &'a UuidArray {
    type Item = Option<&'a Uuid>;
    type IntoIter = UuidArrayIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        UuidArrayIter {
            physical_iter: self.physical.into_iter(),
        }
    }
}
