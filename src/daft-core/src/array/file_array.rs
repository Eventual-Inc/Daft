use std::sync::Arc;

use arrow2::array::{MutableArray, MutableBinaryArray, MutablePrimitiveArray, MutableUtf8Array};
use common_error::DaftResult;
use common_io_config::IOConfig;
use daft_schema::{dtype::DataType, field::Field};

use crate::{
    array::prelude::*,
    file::{DaftFileFormat, DataOrReference, FileReference, FileReferenceType, FileType},
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

impl<T> FileArray<T>
where
    T: DaftFileFormat,
{
    pub fn new_from_file_references<I: Iterator<Item = DaftResult<Option<FileReference>>>>(
        name: &str,
        iter: I,
    ) -> DaftResult<Self> {
        let mut discriminant_arr = MutablePrimitiveArray::<u8>::new();
        let mut io_conf_arr = MutableBinaryArray::<i64>::new();
        let mut data_arr = MutableBinaryArray::<i64>::new();
        let mut urls_arr = MutableUtf8Array::<i64>::new();

        for value in iter {
            let value = value?;
            match value {
                Some(value) => {
                    discriminant_arr.push(Some(value.get_type() as u8));
                    match value.inner {
                        DataOrReference::Reference(url, ioconfig) => {
                            urls_arr.push(Some(url));
                            let io_config = ioconfig.map(|c| {
                                bincode::serialize(&c).expect("Failed to serialize IOConfig")
                            });
                            io_conf_arr.push(io_config);
                            data_arr.push_null();
                        }
                        DataOrReference::Data(items) => {
                            urls_arr.push_null();
                            io_conf_arr.push_null();
                            data_arr.push(Some(items.as_ref()));
                        }
                    }
                }
                None => {
                    discriminant_arr.push_null();
                    urls_arr.push_null();
                    io_conf_arr.push_null();
                    data_arr.push_null();
                }
            }
        }
        let sa_field = Field::new("literal", DataType::File(T::get_type()).to_physical());
        let discriminant = Series::from_arrow(
            Arc::new(Field::new("discriminant", DataType::UInt8)),
            discriminant_arr.as_box(),
        )?;
        let data = Series::from_arrow(
            Arc::new(Field::new("data", DataType::Binary)),
            data_arr.as_box(),
        )?;
        let urls = Series::from_arrow(
            Arc::new(Field::new("url", DataType::Utf8)),
            urls_arr.as_box(),
        )?;
        let io_config = Series::from_arrow(
            Arc::new(Field::new("io_config", DataType::Binary)),
            io_conf_arr.as_box(),
        )?;
        let validity = discriminant.validity().cloned();
        let sa = StructArray::new(
            sa_field,
            vec![discriminant, data, urls, io_config],
            validity,
        );

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
        let discriminant = UInt8Array::from_values(
            "discriminant",
            std::iter::repeat_n(FileReferenceType::Reference as u8, urls.len()),
        )
        .into_series();

        let sa_field = Field::new("literal", DataType::File(T::get_type()).to_physical());
        let io_conf: Option<Vec<u8>> =
            io_config.map(|c| bincode::serialize(&c).expect("Failed to serialize IOConfig"));
        let io_conf = BinaryArray::from_iter("io_config", std::iter::repeat_n(io_conf, urls.len()));

        let data = BinaryArray::full_null("data", &DataType::Binary, urls.len()).into_series();
        let io_conf = io_conf
            .with_validity(urls.validity().cloned())
            .expect("Failed to set validity");

        let sa = StructArray::new(
            sa_field,
            vec![
                discriminant,
                data,
                urls.clone().into_series().rename("url"),
                io_conf.into_series(),
            ],
            urls.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File(T::get_type())), sa)
    }

    pub fn new_from_data_array(name: &str, values: &BinaryArray) -> Self {
        let discriminant = UInt8Array::from_values(
            "discriminant",
            std::iter::repeat_n(FileReferenceType::Data as u8, values.len()),
        )
        .into_series();

        let fld = Field::new("literal", DataType::File(T::get_type()).to_physical());
        let urls = Utf8Array::full_null("url", &DataType::Utf8, values.len()).into_series();
        let io_configs =
            BinaryArray::full_null("io_config", &DataType::Binary, values.len()).into_series();
        let sa = StructArray::new(
            fld,
            vec![
                discriminant,
                values.clone().into_series().rename("data"),
                urls,
                io_configs,
            ],
            values.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File(T::get_type())), sa)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_io_config::IOConfig;

    use crate::{
        datatypes::FileArray,
        file::{DataOrReference, FileFormatUnknown, FileReference},
        lit::Literal,
        prelude::{BinaryArray, FromArrow},
        series::Series,
    };

    #[test]
    fn test_arrow_roundtrip_data_variant() {
        let data = vec![1, 2, 3];
        let bin_arr = BinaryArray::from_iter("data", std::iter::once(Some(data.clone())));

        let arr = FileArray::<FileFormatUnknown>::new_from_data_array("data", &bin_arr);
        let arrow_data = arr.to_arrow();

        let new_arr = FileArray::<FileFormatUnknown>::from_arrow(arr.field.clone(), arrow_data)
            .expect("Failed to create FileArray from arrow data");
        let new_arr = new_arr.data_array();

        let new_data = new_arr.get(0).expect("Failed to get data");
        assert_eq!(new_data, &data);
    }

    #[test]
    fn test_arrow_roundtrip_url_variant() {
        let io_conf = Some(IOConfig::default());
        let url = "file://example.com";

        let urls: Series = Literal::Utf8(url.to_string()).into();
        let urls = urls.utf8().unwrap();

        let arr =
            FileArray::<FileFormatUnknown>::new_from_reference_array("urls", urls, io_conf.clone());
        let arrow_data = arr.to_arrow();

        let new_arr = FileArray::<FileFormatUnknown>::from_arrow(arr.field.clone(), arrow_data)
            .expect("Failed to create FileArray from arrow data");

        let FileReference {
            file_format: _,
            inner: DataOrReference::Reference(url, io_config),
        } = new_arr.get(0).expect("Failed to get data")
        else {
            unreachable!("Expected FileReference::Reference")
        };

        assert_eq!(url, "file://example.com");
        assert_eq!(io_config, io_conf.map(Arc::new));
    }
}
