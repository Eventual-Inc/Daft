use daft_core::{
    datatypes::ExtensionArray, prelude::DataType, series::Series, with_match_file_types,
};

pub fn html_value(s: &Series, idx: usize, truncate: bool) -> String {
    match s.data_type() {
        DataType::Image(_) => {
            let arr = s.image().unwrap();
            daft_image::ops::image_html_value(arr, idx, truncate)
        }
        DataType::Null => {
            let arr = s.null().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Boolean => {
            let arr = s.bool().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Int8 => {
            let arr = s.i8().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Int16 => {
            let arr = s.i16().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Int32 => {
            let arr = s.i32().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Int64 => {
            let arr = s.i64().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::UInt8 => {
            let arr = s.u8().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::UInt16 => {
            let arr = s.u16().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::UInt32 => {
            let arr = s.u32().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::UInt64 => {
            let arr = s.u64().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Float32 => {
            let arr = s.f32().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Float64 => {
            let arr = s.f64().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Decimal128(_, _) => {
            let arr = s.decimal128().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Timestamp(_, _) => {
            let arr = s.timestamp().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Date => {
            let arr = s.date().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Time(_) => {
            let arr = s.time().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Duration(_) => {
            let arr = s.duration().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Interval => {
            let arr = s.interval().unwrap();
            arr.html_value(idx, truncate)
        }

        DataType::Binary => {
            let arr = s.binary().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::FixedSizeBinary(_) => {
            let arr = s.fixed_size_binary().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Utf8 => {
            let arr = s.utf8().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::FixedSizeList(_, _) => {
            let arr = s.fixed_size_list().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::List(_) => {
            let arr = s.list().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Struct(_) => {
            let arr = s.struct_().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Union(_, _, _) => {
            let arr = s.union().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Map { .. } => {
            let arr = s.map().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Extension(_, _, _) => {
            let arr = s.downcast::<ExtensionArray>().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Embedding(_, _) => {
            let arr = s.embedding().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::FixedShapeImage(_, _, _) => {
            let arr = s.fixed_size_image().unwrap();
            daft_image::ops::fixed_image_html_value(arr, idx, truncate)
        }
        DataType::Tensor(_) => {
            let arr = s.tensor().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::FixedShapeTensor(_, _) => {
            let arr = s.fixed_shape_tensor().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::SparseTensor(_, _) => {
            let arr = s.sparse_tensor().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::FixedShapeSparseTensor(_, _, _) => {
            let arr = s.fixed_shape_sparse_tensor().unwrap();
            arr.html_value(idx, truncate)
        }
        #[cfg(feature = "python")]
        DataType::Python => {
            let arr = s.python().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::File(_) => {
            with_match_file_types!(s.data_type(), |$P| {
                let arr = s.file::<$P>().unwrap();
                arr.html_value(idx, truncate)
            })
        }
        DataType::Unknown => {
            panic!("Unknown data type")
        }
        DataType::WKT(_) => {
            let arr = s.wkt().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::WKB(_) => {
            let arr = s.wkb().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Point(_) => {
            let arr = s.point().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::LineString(_) => {
            let arr = s.line_string().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Polygon(_) => {
            let arr = s.polygon().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::MultiPoint(_) => {
            let arr = s.multi_point().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::MultiLineString(_) => {
            let arr = s.multi_line_string().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::MultiPolygon(_) => {
            let arr = s.multi_polygon().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::GeometryCollection(_) => {
            let arr = s.geometry_collection().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Geometry(_) => {
            let arr = s.geometry().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Geography => {
            let arr = s.geography().unwrap();
            arr.html_value(idx, truncate)
        }
        DataType::Rect(_) => {
            let arr = s.rect().unwrap();
            arr.html_value(idx, truncate)
        }
    }
}
