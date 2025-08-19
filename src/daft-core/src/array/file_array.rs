use common_file::DaftFileType;
use common_io_config::IOConfig;
use daft_schema::{dtype::DataType, field::Field};

use crate::{array::prelude::*, datatypes::FileType, series::IntoSeries};

pub type FileArray = LogicalArray<FileType>;

impl FileArray {
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        use crate::series::IntoSeries;

        let discriminant_field = Field::new("discriminant", DataType::UInt8);
        let discriminant_values = vec![DaftFileType::Reference as u8; urls.len()];
        let discriminant = UInt8Array::from_values_iter(
            discriminant_field.clone(),
            discriminant_values.into_iter(),
        )
        .into_series();

        let sa_field = Field::new(
            "literal",
            DataType::Struct(vec![
                discriminant_field.clone(),
                Field::new("data", DataType::Binary),
                Field::new("url", DataType::Utf8),
                Field::new("io_config", DataType::Binary),
            ]),
        );
        let io_config = bincode::serialize(&io_config).expect("Failed to serialize data");
        let io_configs =
            BinaryArray::from_values("io_config", std::iter::repeat(io_config).take(urls.len()));
        let data = BinaryArray::full_null("data", &DataType::Binary, urls.len()).into_series();
        let io_configs = io_configs
            .with_validity(urls.validity().cloned())
            .expect("Failed to set validity");

        let sa = StructArray::new(
            sa_field,
            vec![
                discriminant,
                data,
                urls.clone().into_series().rename("url"),
                io_configs.into_series(),
            ],
            urls.validity().cloned(),
        );
        FileArray::new(Field::new(name, DataType::File), sa)
    }

    pub fn new_from_data_array(name: &str, values: &BinaryArray) -> Self {
        let discriminant_field = Field::new("discriminant", DataType::UInt8);
        let values_field = Field::new("data", DataType::Binary);

        let discriminant_values = vec![DaftFileType::Data as u8; values.len()];

        let discriminant = UInt8Array::from_values_iter(
            discriminant_field.clone(),
            discriminant_values.into_iter(),
        )
        .into_series();
        let fld = Field::new(
            "literal",
            DataType::Struct(vec![
                discriminant_field,
                values_field,
                Field::new("url", DataType::Utf8),
                Field::new("io_config", DataType::Binary),
            ]),
        );
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
