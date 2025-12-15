use std::sync::Arc;

use common_error::DaftResult;
use common_io_config::IOConfig;
use daft_arrow::array::{MutableArray, MutableBinaryArray, MutableUtf8Array};
use daft_schema::{dtype::DataType, field::Field, media_type::MediaType};

use crate::{
    array::prelude::*,
    datatypes::logical::LogicalArrayImpl,
    file::{DaftMediaType, FileReference, FileType},
    series::{IntoSeries, Series},
};

/// FileArray is a logical array that represents a collection of files.
///
/// FileArray's underlying representation implements a tagged union pattern through a struct
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
pub type FileArray<T> = LogicalArray<FileType<T>>;

impl<U> FileArray<U>
where
    U: DaftMediaType,
{
    /// Converts to a different file format
    pub fn change_type<T: DaftMediaType>(self) -> FileArray<T> {
        let LogicalArrayImpl {
            field,
            mut physical,
            ..
        } = self;
        physical.field = Arc::new(Field::new(
            "literal",
            DataType::File(T::get_type()).to_physical(),
        ));

        FileArray::new(
            Field::new(&field.name, DataType::File(T::get_type())),
            physical,
        )
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn media_type(&self) -> MediaType {
        T::get_type()
    }
    pub fn new_from_file_references<I: Iterator<Item = DaftResult<Option<FileReference>>>>(
        name: &str,
        iter: I,
    ) -> DaftResult<Self> {
        let mut io_conf_arr = MutableBinaryArray::<i64>::new();
        let mut urls_arr = MutableUtf8Array::<i64>::new();

        for value in iter {
            let value = value?;
            match value {
                Some(value) => {
                    urls_arr.push(Some(value.url));
                    let io_config = value.io_config.map(|c| {
                        bincode::serde::encode_to_vec(&c, bincode::config::legacy())
                            .expect("Failed to serialize IOConfig")
                    });
                    io_conf_arr.push(io_config);
                }
                None => {
                    urls_arr.push_null();
                    io_conf_arr.push_null();
                }
            }
        }
        let sa_field = Field::new("literal", DataType::File(T::get_type()).to_physical());
        let urls = Series::from_arrow(
            Arc::new(Field::new("url", DataType::Utf8)),
            urls_arr.as_box(),
        )?;
        let io_config = Series::from_arrow(
            Arc::new(Field::new("io_config", DataType::Binary)),
            io_conf_arr.as_box(),
        )?;
        let validity = urls.validity().cloned();
        let sa = StructArray::new(sa_field, vec![urls, io_config], validity);

        Ok(FileArray::new(
            Field::new(name, DataType::File(T::get_type())),
            sa,
        ))
    }
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        let sa_field = Field::new("literal", DataType::File(T::get_type()).to_physical());
        let io_conf: Option<Vec<u8>> = io_config.map(|c| {
            bincode::serde::encode_to_vec(&c, bincode::config::legacy())
                .expect("Failed to serialize IOConfig")
        });
        let io_conf = BinaryArray::from_iter("io_config", std::iter::repeat_n(io_conf, urls.len()));

        let io_conf = io_conf
            .with_validity(urls.validity().cloned())
            .expect("Failed to set validity");

        let sa = StructArray::new(
            sa_field,
            vec![
                urls.clone().into_series().rename("url"),
                io_conf.into_series(),
            ],
            urls.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File(T::get_type())), sa)
    }

    pub fn iter(&self) -> FileArrayIter<'_, T> {
        FileArrayIter {
            array: self,
            idx: 0,
        }
    }
}

pub struct FileArrayIter<'a, T>
where
    T: DaftMediaType,
{
    array: &'a FileArray<T>,
    idx: usize,
}

impl<T> Iterator for FileArrayIter<'_, T>
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

impl<'a, T> IntoIterator for &'a FileArray<T>
where
    T: DaftMediaType,
{
    type Item = Option<FileReference>;
    type IntoIter = FileArrayIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        FileArrayIter {
            array: self,
            idx: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_io_config::IOConfig;

    use crate::{
        datatypes::FileArray,
        file::{FileReference, MediaTypeUnknown},
        lit::Literal,
        prelude::FromArrow,
        series::Series,
    };

    #[test]
    fn test_arrow_roundtrip_url_variant() {
        let io_conf = Some(IOConfig::default());
        let url = "file://example.com";

        let urls: Series = Literal::Utf8(url.to_string()).into();
        let urls = urls.utf8().unwrap();

        let arr =
            FileArray::<MediaTypeUnknown>::new_from_reference_array("urls", urls, io_conf.clone());
        let arrow_data = arr.to_arrow2();

        let new_arr = FileArray::<MediaTypeUnknown>::from_arrow(arr.field.clone(), arrow_data)
            .expect("Failed to create FileArray from arrow data");

        let FileReference {
            media_type: _,
            url,
            io_config,
        } = new_arr.get(0).expect("Failed to get data");

        assert_eq!(url, "file://example.com");
        assert_eq!(io_config, io_conf.map(Arc::new));
    }
}
