use common_image::Image;

use crate::{
    array::ops::image::AsImageObj, datatypes::FileArray, file::DaftMediaType, lit::Literal,
    prelude::*,
};

fn map_or_null<T, U, F>(o: Option<T>, f: F) -> Literal
where
    F: FnOnce(U) -> Literal,
    U: From<T>,
{
    match o {
        Some(v) => f(v.into()),
        None => Literal::Null,
    }
}

impl NullArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        Literal::Null
    }
}

impl StructArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        if self.is_valid(idx) {
            Literal::Struct(
                self.children
                    .iter()
                    // Below line is commented out because it significantly complicates Series <-> Literal conversion to do this.
                    // Instead, we'll filter these empty fields out at the Literal to Python object boundary.
                    // .filter(|child| !child.name().is_empty() && !child.data_type().is_null())
                    .map(|child| (child.name().to_string(), child.get_lit(idx)))
                    .collect(),
            )
        } else {
            Literal::Null
        }
    }
}

impl TensorArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        if self.physical.is_valid(idx)
            && let (Some(data), Some(shape)) =
                (self.data_array().get(idx), self.shape_array().get(idx))
        {
            let shape = shape.u64().unwrap().as_arrow2().values().to_vec();
            Literal::Tensor { data, shape }
        } else {
            Literal::Null
        }
    }
}

impl SparseTensorArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        let indices_offset = match self.data_type() {
            DataType::SparseTensor(_, indices_offset) => *indices_offset,
            dtype => unreachable!("Unexpected data type for SparseTensorArray: {dtype}"),
        };

        if self.physical.is_valid(idx)
            && let (Some(values), Some(indices), Some(shape)) = (
                self.values_array().get(idx),
                self.indices_array().get(idx),
                self.shape_array().get(idx),
            )
        {
            let shape = shape.u64().unwrap().as_arrow2().values().to_vec();

            Literal::SparseTensor {
                values,
                indices,
                shape,
                indices_offset,
            }
        } else {
            Literal::Null
        }
    }
}

impl FixedShapeSparseTensorArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        let (shape, indices_offset) = match self.data_type() {
            DataType::FixedShapeSparseTensor(_, shape, indices_offset) => {
                (shape.clone(), *indices_offset)
            }
            dtype => unreachable!("Unexpected data type for FixedShapeSparseTensorArray: {dtype}",),
        };

        if self.physical.is_valid(idx)
            && let (Some(values), Some(indices)) =
                (self.values_array().get(idx), self.indices_array().get(idx))
        {
            Literal::SparseTensor {
                values,
                indices,
                shape,
                indices_offset,
            }
        } else {
            Literal::Null
        }
    }
}

impl MapArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        map_or_null(self.get(idx), |entry: Series| {
            let entry = entry.struct_().unwrap();
            let keys = entry.get("key").unwrap();
            let values = entry.get("value").unwrap();

            Literal::Map { keys, values }
        })
    }
}

impl ExtensionArray {
    pub fn get_lit(&self, idx: usize) -> Literal {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );

        if self.is_valid(idx) {
            Literal::Extension(self.slice(idx, idx + 1).unwrap().into_series())
        } else {
            Literal::Null
        }
    }
}

macro_rules! impl_array_get_lit {
    ($type:ty, $variant:ident) => {
        impl $type {
            pub fn get_lit(&self, idx: usize) -> Literal {
                // don't need to do assertions here because it also happens in `self.get`
                map_or_null(self.get(idx), Literal::$variant)
            }
        }
    };

    ($type:ty, $dtype:pat => $mapper:expr) => {
        impl $type {
            pub fn get_lit(&self, idx: usize) -> Literal {
                assert!(
                    idx < self.len(),
                    "Out of bounds: {} vs len: {}",
                    idx,
                    self.len()
                );

                match self.data_type() {
                    $dtype => map_or_null(self.get(idx), $mapper),
                    other => {
                        unreachable!("Unexpected data type for {}: {}", stringify!($type), other)
                    }
                }
            }
        }
    };
}

macro_rules! impl_image_array_get_lit {
    ($type:ty) => {
        impl $type {
            pub fn get_lit(&self, idx: usize) -> Literal {
                assert!(
                    idx < self.len(),
                    "Out of bounds: {} vs len: {}",
                    idx,
                    self.len()
                );

                map_or_null(self.as_image_obj(idx), |obj| Literal::Image(Image(obj)))
            }
        }
    };
}

impl_array_get_lit!(BooleanArray, Boolean);
impl_array_get_lit!(BinaryArray, Binary);
impl_array_get_lit!(FixedSizeBinaryArray, Binary);
impl_array_get_lit!(Int8Array, Int8);
impl_array_get_lit!(Int16Array, Int16);
impl_array_get_lit!(Int32Array, Int32);
impl_array_get_lit!(Int64Array, Int64);
impl_array_get_lit!(UInt8Array, UInt8);
impl_array_get_lit!(UInt16Array, UInt16);
impl_array_get_lit!(UInt32Array, UInt32);
impl_array_get_lit!(UInt64Array, UInt64);
impl_array_get_lit!(Float32Array, Float32);
impl_array_get_lit!(Float64Array, Float64);
impl_array_get_lit!(Utf8Array, Utf8);
impl_array_get_lit!(IntervalArray, Interval);
impl_array_get_lit!(DateArray, Date);
impl_array_get_lit!(ListArray, List);
impl_array_get_lit!(FixedSizeListArray, List);
impl_array_get_lit!(EmbeddingArray, Embedding);

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn get_lit(&self, idx: usize) -> Literal {
        map_or_null(self.get(idx), Literal::File)
    }
}

#[cfg(feature = "python")]
impl_array_get_lit!(PythonArray, Python);

impl_array_get_lit!(Decimal128Array, DataType::Decimal128(precision, scale) => |v| Literal::Decimal(v, *precision as _, *scale as _));
impl_array_get_lit!(TimestampArray, DataType::Timestamp(tu, tz) => |v| Literal::Timestamp(v, *tu, tz.clone()));
impl_array_get_lit!(TimeArray, DataType::Time(tu) => |v| Literal::Time(v, *tu));
impl_array_get_lit!(DurationArray, DataType::Duration(tu) => |v| Literal::Duration(v, *tu));
impl_array_get_lit!(FixedShapeTensorArray, DataType::FixedShapeTensor(_, shape) => |data| Literal::Tensor { data, shape: shape.clone() });

impl_image_array_get_lit!(ImageArray);
impl_image_array_get_lit!(FixedShapeImageArray);
