use crate::array::{FixedSizeListArray, ListArray, StructArray};
use crate::datatypes::logical::FixedShapeImageArray;
use crate::datatypes::*;
use crate::series::array_impl::ArrayWrapper;
use crate::series::Series;
use common_error::DaftResult;

impl Series {
    pub fn downcast<Arr: DaftArrayType>(&self) -> DaftResult<&Arr> {
        match self.inner.as_any().downcast_ref() {
            Some(ArrayWrapper(arr)) => Ok(arr),
            None => {
                panic!(
                    "Attempting to downcast {:?} to {:?}",
                    self.data_type(),
                    std::any::type_name::<Arr>(),
                )
            }
        }
    }

    pub fn null(&self) -> DaftResult<&NullArray> {
        self.downcast()
    }

    pub fn bool(&self) -> DaftResult<&BooleanArray> {
        self.downcast()
    }

    pub fn i8(&self) -> DaftResult<&Int8Array> {
        self.downcast()
    }

    pub fn i16(&self) -> DaftResult<&Int16Array> {
        self.downcast()
    }

    pub fn i32(&self) -> DaftResult<&Int32Array> {
        self.downcast()
    }

    pub fn i64(&self) -> DaftResult<&Int64Array> {
        self.downcast()
    }

    pub fn u8(&self) -> DaftResult<&UInt8Array> {
        self.downcast()
    }

    pub fn u16(&self) -> DaftResult<&UInt16Array> {
        self.downcast()
    }

    pub fn u32(&self) -> DaftResult<&UInt32Array> {
        self.downcast()
    }

    pub fn u64(&self) -> DaftResult<&UInt64Array> {
        self.downcast()
    }

    // pub fn f16(&self) -> DaftResult<&Float16Array> {
    //     self.downcast()
    // }

    pub fn f32(&self) -> DaftResult<&Float32Array> {
        self.downcast()
    }

    pub fn f64(&self) -> DaftResult<&Float64Array> {
        self.downcast()
    }

    pub fn binary(&self) -> DaftResult<&BinaryArray> {
        self.downcast()
    }

    pub fn utf8(&self) -> DaftResult<&Utf8Array> {
        self.downcast()
    }

    pub fn fixed_size_list(&self) -> DaftResult<&FixedSizeListArray> {
        self.downcast()
    }

    pub fn list(&self) -> DaftResult<&ListArray> {
        self.downcast()
    }

    pub fn struct_(&self) -> DaftResult<&StructArray> {
        self.downcast()
    }

    pub fn fixed_size_image(&self) -> DaftResult<&FixedShapeImageArray> {
        self.downcast()
    }

    #[cfg(feature = "python")]
    pub fn python(&self) -> DaftResult<&PythonArray> {
        self.downcast()
    }
}
