#[cfg(feature = "python")]
use common_file::DaftFileType;
use common_io_config::IOConfig;
#[cfg(feature = "python")]
use daft_schema::{dtype::DataType, field::Field};

#[cfg(feature = "python")]
use crate::series::IntoSeries;
use crate::{array::prelude::*, datatypes::FileType};
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
    #[cfg(feature = "python")]
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        use crate::{prelude::PythonArray, series::IntoSeries};

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
                discriminant_field,
                Field::new("data", DataType::Binary),
                Field::new("url", DataType::Utf8),
                Field::new("io_config", DataType::Python),
            ]),
        );

        let io_conf = io_config.map(common_io_config::python::IOConfig::from);
        let io_conf = pyo3::Python::with_gil(|py| {
            use std::sync::Arc;

            use pyo3::IntoPyObjectExt;

            Arc::new(
                io_conf
                    .into_py_any(py)
                    .expect("Failed to convert ioconfig to PyObject"),
            )
        });
        let io_configs = PythonArray::from((
            "io_config",
            std::iter::repeat(io_conf)
                .take(urls.len())
                .collect::<Vec<_>>(),
        ));

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

    #[cfg(not(feature = "python"))]
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        unimplemented!()
    }

    #[cfg(feature = "python")]
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
            crate::prelude::PythonArray::full_null("io_config", &DataType::Python, values.len())
                .into_series();
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

    #[cfg(not(feature = "python"))]
    pub fn new_from_data_array(name: &str, values: &BinaryArray) -> Self {
        unimplemented!()
    }
}
