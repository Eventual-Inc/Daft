use common_file::DaftFileType;
use common_io_config::IOConfig;
use daft_schema::{dtype::DataType, field::Field};

use crate::{array::prelude::*, datatypes::FileType, series::IntoSeries};

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
pub type FileArray = LogicalArray<FileType>;

impl FileArray {
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        let discriminant = UInt8Array::from_values(
            "discriminant",
            std::iter::repeat_n(DaftFileType::Reference as u8, urls.len()),
        )
        .into_series();

        let sa_field = Field::new("literal", DataType::File.to_physical());
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
        FileArray::new(Field::new(name, DataType::File), sa)
    }

    pub fn new_from_data_array(name: &str, values: &BinaryArray) -> Self {
        let discriminant = UInt8Array::from_values(
            "discriminant",
            std::iter::repeat_n(DaftFileType::Data as u8, values.len()),
        )
        .into_series();

        let fld = Field::new("literal", DataType::File.to_physical());
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
        FileArray::new(Field::new(name, DataType::File), sa)
    }
}
