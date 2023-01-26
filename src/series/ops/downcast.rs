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
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not null"))),
        }
    }

    pub fn bool(&self) -> DaftResult<&BooleanArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Boolean => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not boolean"))),
        }
    }

    pub fn i8(&self) -> DaftResult<&Int8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not int8"))),
        }
    }

    pub fn i16(&self) -> DaftResult<&Int16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not int16"))),
        }
    }

    pub fn i32(&self) -> DaftResult<&Int32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not int32"))),
        }
    }

    pub fn i64(&self) -> DaftResult<&Int64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Int64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not int64"))),
        }
    }

    pub fn u8(&self) -> DaftResult<&UInt8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not uint8"))),
        }
    }

    pub fn u16(&self) -> DaftResult<&UInt16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not uint16"))),
        }
    }

    pub fn u32(&self) -> DaftResult<&UInt32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not uint32"))),
        }
    }

    pub fn u64(&self) -> DaftResult<&UInt64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            UInt64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not uint64"))),
        }
    }

    pub fn f16(&self) -> DaftResult<&Float16Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float16 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not float16"))),
        }
    }

    pub fn f32(&self) -> DaftResult<&Float32Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float32 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not float32"))),
        }
    }

    pub fn f64(&self) -> DaftResult<&Float64Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Float64 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not float64"))),
        }
    }

    pub fn timestamp(&self) -> DaftResult<&TimestampArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Timestamp(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not timestamp"))),
        }
    }

    pub fn date(&self) -> DaftResult<&DateArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Date => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not date"))),
        }
    }

    pub fn time(&self) -> DaftResult<&TimeArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Time(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not time"))),
        }
    }

    pub fn binary(&self) -> DaftResult<&BinaryArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Binary => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not binary"))),
        }
    }

    pub fn utf8(&self) -> DaftResult<&Utf8Array> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Utf8 => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not utf8"))),
        }
    }

    pub fn fixed_size_list(&self) -> DaftResult<&FixedSizeListArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            FixedSizeList(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!(
                "{t:?} not fixed sized list"
            ))),
        }
    }

    pub fn list(&self) -> DaftResult<&ListArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            List(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not list"))),
        }
    }

    pub fn struct_(&self) -> DaftResult<&StructArray> {
        use crate::datatypes::DataType::*;
        match self.data_type() {
            Struct(..) => Ok(self.array().as_any().downcast_ref().unwrap()),
            t => Err(DaftError::SchemaMismatch(format!("{t:?} not struct"))),
        }
    }
}
