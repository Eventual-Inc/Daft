use std::sync::Arc;

use daft_schema::{dtype::DataType, field::Field, media_type::MediaType};

use crate::{
    array::prelude::*,
    datatypes::logical::LogicalArrayImpl,
    file::{BlobType, DaftMediaType, FileReference},
};

/// BlobArray is a logical array that represents a collection of files.
///
/// BlobArray's underlying representation implements a tagged union pattern through a struct
/// containing all possible fields (discriminant, data, url, io_config), though they're
/// mutually exclusive in usage:
///
/// - Reference files: Use url and io_config fields (data is null)
/// - Data files: Use only the data field (url and io_config are null)
///
/// The discriminant field serves as the "tag" that determines which fields are active/valid.
/// This manual union implementation is necessary because our type system lacks native
/// union types, requiring a consistent struct schema regardless of which variant is active.
///
/// The io_config field contains bincode-serialized IOConfig objects, as this was the most
/// straightforward approach to store these configuration objects in our array structure.
pub type BlobArray<T> = LogicalArray<BlobType<T>>;

impl<U> BlobArray<U> where U: DaftMediaType {}

impl<T> BlobArray<T>
where
    T: DaftMediaType,
{
    /// Converts to a different file format
    pub fn change_type<U: DaftMediaType>(self) -> BlobArray<U> {
        let LogicalArrayImpl {
            field,
            mut physical,
            ..
        } = self;
        physical.field = Arc::new(Field::new(
            "literal",
            DataType::File(U::get_type(), true).to_physical(),
        ));

        BlobArray::new(
            Field::new(&field.name, DataType::File(U::get_type(), true)),
            physical,
        )
    }

    pub fn media_type(&self) -> MediaType {
        T::get_type()
    }

    pub fn from_values(name: &str, values: &BinaryArray) -> Self {
        BlobArray::new(Field::new(name, Self::dtype()), values.clone())
    }
    pub fn iter(&self) -> BlobArrayIter<'_, T> {
        BlobArrayIter {
            array: self,
            idx: 0,
        }
    }

    fn dtype() -> DataType {
        DataType::File(T::get_type(), true)
    }
}

pub struct BlobArrayIter<'a, T>
where
    T: DaftMediaType,
{
    array: &'a BlobArray<T>,
    idx: usize,
}

impl<T> Iterator for BlobArrayIter<'_, T>
where
    T: DaftMediaType,
{
    type Item = Option<FileReference>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.array.len() {
            None
        } else {
            let file_ref = self.array.get(self.idx);
            self.idx += 1;
            Some(file_ref)
        }
    }
}

impl<'a, T> IntoIterator for &'a BlobArray<T>
where
    T: DaftMediaType,
{
    type Item = Option<FileReference>;
    type IntoIter = BlobArrayIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        BlobArrayIter {
            array: self,
            idx: 0,
        }
    }
}
