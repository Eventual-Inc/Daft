use std::sync::Arc;

use arrow2::Either;
use common_io_config::IOConfig;
use daft_schema::{dtype::DataType, field::Field};

use crate::{array::prelude::*, datatypes::FileType, lit::Literal};

pub type FileArray = LogicalArray<FileType>;

impl FileArray {
    pub fn new_from_data_array(name: &str, data: &BinaryArray) -> Self {
        todo!()
    }

    // pub fn new(name: &str, data: Vec<Option<DaftFile>>) -> Self {
    //     let field = Arc::new(Field::new(name, DataType::File));

    //     let (raw_validity, data) = data
    //         .iter()
    //         .map(|v| {
    //             let data = bincode::serialize(&v).expect("Failed to serialize data");
    //             (v.is_some(), data)
    //         })
    //         .unzip::<_, _, Vec<_>, Vec<_>>();

    //     let validity = if raw_validity.contains(&false) {
    //         Some(arrow2::bitmap::Bitmap::from(raw_validity.as_slice()))
    //     } else {
    //         None
    //     };

    //     Self { field, data }
    // }

    #[cfg(feature = "python")]
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        use common_file::DaftFile;

        use crate::series::IntoSeries;

        let sa_field = Field::new(
            "data",
            DataType::Struct(vec![
                Field::new("urls", DataType::Utf8),
                Field::new("io_config", DataType::Binary),
            ]),
        );
        let io_configs = bincode::serialize(&io_config).expect("Failed to serialize data");

        let io_configs =
            BinaryArray::from_values("io_config", std::iter::repeat(io_configs).take(urls.len()));

        let sa = StructArray::new(
            sa_field,
            vec![urls.clone().into_series(), io_configs.into_series()],
            urls.validity().cloned(),
        );
        
        



        todo!()
    }

    #[cfg(not(feature = "python"))]
    pub fn new_from_reference_array(
        name: &str,
        urls: &Utf8Array,
        io_config: Option<IOConfig>,
    ) -> Self {
        unimplemented!()
    }

    pub fn data_array(&self) -> BinaryArray {
        todo!()
        // self.physical.get("data").unwrap().binary().unwrap().clone()
    }
}
