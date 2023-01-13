use crate::datatypes::*;

use crate::{
    error::{DaftError, DaftResult},
    series::Series,
};

impl Series {
    pub fn downcast<T>(&self) -> DaftResult<&DataArray<T>>
    where
        T: DaftDataType + 'static,
    {
        if self.data_type().eq(&T::get_dtype()) {
            Ok(self.array().as_any().downcast_ref().unwrap())
        } else {
            return Err(DaftError::SchemaMismatch(format!(
                "{:?} not {:?}",
                self.data_type(),
                T::get_dtype()
            )));
        }
    }

    pub fn null(&self) -> DaftResult<&NullArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Null => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not null", t))),
        }
    }

    pub fn bool(&self) -> DaftResult<&BooleanArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Boolean => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not boolean", t))),
        }
    }

    pub fn i8(&self) -> DaftResult<&Int8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not int8", t))),
        }
    }

    pub fn i16(&self) -> DaftResult<&Int16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not int16", t))),
        }
    }

    pub fn i32(&self) -> DaftResult<&Int32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not int32", t))),
        }
    }

    pub fn i64(&self) -> DaftResult<&Int64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not int64", t))),
        }
    }

    pub fn u8(&self) -> DaftResult<&UInt8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not uint8", t))),
        }
    }

    pub fn u16(&self) -> DaftResult<&UInt16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not uint16", t))),
        }
    }

    pub fn u32(&self) -> DaftResult<&UInt32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not uint32", t))),
        }
    }

    pub fn u64(&self) -> DaftResult<&UInt64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not uint64", t))),
        }
    }

    pub fn f16(&self) -> DaftResult<&Float16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not float16", t))),
        }
    }

    pub fn f32(&self) -> DaftResult<&Float32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not float32", t))),
        }
    }

    pub fn f64(&self) -> DaftResult<&Float64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not float64", t))),
        }
    }

    pub fn timestamp(&self) -> DaftResult<&TimestampArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Timestamp(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not timestamp", t))),
        }
    }

    pub fn date(&self) -> DaftResult<&DateArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Date => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not date", t))),
        }
    }

    pub fn time(&self) -> DaftResult<&TimeArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Time(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not time", t))),
        }
    }

    pub fn binary(&self) -> DaftResult<&BinaryArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Binary => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not binary", t))),
        }
    }

    pub fn utf8(&self) -> DaftResult<&Utf8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Utf8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not utf8", t))),
        }
    }

    pub fn fixed_size_list(&self) -> DaftResult<&FixedSizeListArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            FixedSizeList(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!(
                "{:?} not fixed sized list",
                t
            ))),
        }
    }

    pub fn list(&self) -> DaftResult<&ListArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            List(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not list", t))),
        }
    }

    pub fn struct_(&self) -> DaftResult<&StructArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Struct(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{:?} not struct", t))),
        }
    }
}
