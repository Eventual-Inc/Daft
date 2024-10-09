use daft_core::{datatypes::ExtensionArray, prelude::DataType, series::Series};

pub fn html_value(s: &Series, idx: usize) -> String {
    match s.data_type() {
        DataType::Image(_) => {
            let arr = s.image().unwrap();
            daft_image::ops::image_html_value(arr, idx)
        }
        DataType::Null => {
            let arr = s.null().unwrap();
            arr.html_value(idx)
        }
        DataType::Boolean => {
            let arr = s.bool().unwrap();
            arr.html_value(idx)
        }
        DataType::Int8 => {
            let arr = s.i8().unwrap();
            arr.html_value(idx)
        }
        DataType::Int16 => {
            let arr = s.i16().unwrap();
            arr.html_value(idx)
        }
        DataType::Int32 => {
            let arr = s.i32().unwrap();
            arr.html_value(idx)
        }
        DataType::Int64 => {
            let arr = s.i64().unwrap();
            arr.html_value(idx)
        }
        DataType::Int128 => {
            let arr = s.i128().unwrap();
            arr.html_value(idx)
        }
        DataType::UInt8 => {
            let arr = s.u8().unwrap();
            arr.html_value(idx)
        }
        DataType::UInt16 => {
            let arr = s.u16().unwrap();
            arr.html_value(idx)
        }
        DataType::UInt32 => {
            let arr = s.u32().unwrap();
            arr.html_value(idx)
        }
        DataType::UInt64 => {
            let arr = s.u64().unwrap();
            arr.html_value(idx)
        }
        DataType::Float32 => {
            let arr = s.f32().unwrap();
            arr.html_value(idx)
        }
        DataType::Float64 => {
            let arr = s.f64().unwrap();
            arr.html_value(idx)
        }
        DataType::Decimal128(_, _) => {
            let arr = s.decimal128().unwrap();
            arr.html_value(idx)
        }
        DataType::Timestamp(_, _) => {
            let arr = s.timestamp().unwrap();
            arr.html_value(idx)
        }
        DataType::Date => {
            let arr = s.date().unwrap();
            arr.html_value(idx)
        }
        DataType::Time(_) => {
            let arr = s.time().unwrap();
            arr.html_value(idx)
        }
        DataType::Duration(_) => {
            let arr = s.duration().unwrap();
            arr.html_value(idx)
        }
        DataType::Binary => {
            let arr = s.binary().unwrap();
            arr.html_value(idx)
        }
        DataType::FixedSizeBinary(_) => {
            let arr = s.fixed_size_binary().unwrap();
            arr.html_value(idx)
        }
        DataType::Utf8 => {
            let arr = s.utf8().unwrap();
            arr.html_value(idx)
        }
        DataType::FixedSizeList(_, _) => {
            let arr = s.fixed_size_list().unwrap();
            arr.html_value(idx)
        }
        DataType::List(_) => {
            let arr = s.list().unwrap();
            arr.html_value(idx)
        }
        DataType::Struct(_) => {
            let arr = s.struct_().unwrap();
            arr.html_value(idx)
        }
        DataType::Map { .. } => {
            let arr = s.map().unwrap();
            arr.html_value(idx)
        }
        DataType::Extension(_, _, _) => {
            let arr = s.downcast::<ExtensionArray>().unwrap();
            arr.html_value(idx)
        }
        DataType::Embedding(_, _) => {
            let arr = s.embedding().unwrap();
            arr.html_value(idx)
        }
        DataType::FixedShapeImage(_, _, _) => {
            let arr = s.fixed_size_image().unwrap();
            daft_image::ops::fixed_image_html_value(arr, idx)
        }
        DataType::Tensor(_) => {
            let arr = s.tensor().unwrap();
            arr.html_value(idx)
        }
        DataType::FixedShapeTensor(_, _) => {
            let arr = s.fixed_shape_tensor().unwrap();
            arr.html_value(idx)
        }
        DataType::SparseTensor(_) => {
            let arr = s.sparse_tensor().unwrap();
            arr.html_value(idx)
        }
        DataType::FixedShapeSparseTensor(_, _) => {
            let arr = s.fixed_shape_sparse_tensor().unwrap();
            arr.html_value(idx)
        }
        #[cfg(feature = "python")]
        DataType::Python => {
            let arr = s.python().unwrap();
            arr.html_value(idx)
        }
        DataType::Unknown => {
            panic!("Unknown data type")
        }
    }
}
