use std::sync::Arc;

use common_io_config::IOConfig;
use daft_schema::{dtype::DataType, field::Field, media_type::MediaType};

use crate::{
    array::prelude::*,
    datatypes::logical::LogicalArrayImpl,
    file::{DaftMediaType, FileReference, FileType},
    series::IntoSeries,
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
        physical.field = Arc::new(Field::new("literal", Self::dtype().to_physical()));

        FileArray::new(Field::new(&field.name, Self::dtype()), physical)
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub const URLS_KEY: &'static str = "url";
    pub const IO_CONFIG_KEY: &'static str = "io_config";
    fn dtype() -> DataType {
        DataType::File(T::get_type())
    }

    pub fn media_type(&self) -> MediaType {
        T::get_type()
    }

    pub fn from_values(name: &str, urls: &Utf8Array, io_config: Option<IOConfig>) -> Self {
        let sa_field = Field::new("literal", Self::dtype().to_physical());

        let io_conf: Option<Vec<u8>> =
            io_config.map(|c| bincode::serialize(&c).expect("Failed to serialize IOConfig"));
        let io_conf = BinaryArray::from_iter(
            Self::IO_CONFIG_KEY,
            std::iter::repeat_n(io_conf, urls.len()),
        );

        let io_conf = io_conf
            .with_validity(urls.validity().cloned())
            .expect("Failed to set validity");

        let sa = StructArray::new(
            sa_field,
            vec![
                urls.clone().into_series().rename(Self::URLS_KEY),
                io_conf.into_series(),
            ],
            urls.validity().cloned(),
        );
        FileArray::new(Field::new(name, Self::dtype()), sa)
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
        file::{DataOrReference, FileReference, MediaTypeUnknown},
        lit::Literal,
        prelude::FromArrow,
        series::Series,
    };

    #[test]
    fn test_arrow_roundtrip_data_variant() {
        todo!()
        // let data = vec![1, 2, 3];
        // let bin_arr = BinaryArray::from_iter("data", std::iter::once(Some(data.clone())));

        // let arr = FileArray::<MediaTypeUnknown>::new_from_data_array("data", &bin_arr);
        // let arrow_data = arr.to_arrow();

        // let new_arr = FileArray::<MediaTypeUnknown>::from_arrow(arr.field.clone(), arrow_data)
        //     .expect("Failed to create FileArray from arrow data");
        // let new_arr = new_arr.data_array();

        // let new_data = new_arr.get(0).expect("Failed to get data");
        // assert_eq!(new_data, &data);
    }

    #[test]
    fn test_arrow_roundtrip_url_variant() {
        let io_conf = Some(IOConfig::default());
        let url = "file://example.com";

        let urls: Series = Literal::Utf8(url.to_string()).into();
        let urls = urls.utf8().unwrap();

        let arr = FileArray::<MediaTypeUnknown>::from_values("urls", urls, io_conf.clone());
        let arrow_data = arr.to_arrow();

        let new_arr = FileArray::<MediaTypeUnknown>::from_arrow(arr.field.clone(), arrow_data)
            .expect("Failed to create FileArray from arrow data");

        let FileReference {
            media_type: _,
            inner: DataOrReference::Reference(url, io_config),
        } = new_arr.get(0).expect("Failed to get data")
        else {
            unreachable!("Expected FileReference::Reference")
        };

        assert_eq!(url, "file://example.com");
        assert_eq!(io_config, io_conf.map(Arc::new));
    }
}
