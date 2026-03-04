//! Conversion helpers between arrow-rs types and our ABI types.
//!
//! Delegates to the `define_arrow_helpers!` macro from `daft-ext-abi` for the
//! actual conversions, then wraps the `String` errors into `DaftResult`.

use arrow_array::ArrayRef;
use arrow_schema::Schema;
use daft_ext_abi::ArrowData;

use crate::error::{DaftError, DaftResult};

mod inner {
    daft_ext_abi::define_arrow_helpers!();
}

pub fn export_array(array: &dyn arrow_array::Array) -> DaftResult<ArrowData> {
    inner::export_array(array).map_err(DaftError::RuntimeError)
}

pub fn import_array(data: ArrowData) -> DaftResult<ArrayRef> {
    inner::import_array(data).map_err(DaftError::RuntimeError)
}

pub fn export_schema(schema: &Schema) -> DaftResult<daft_ext_abi::ArrowSchema> {
    inner::export_schema(schema).map_err(DaftError::RuntimeError)
}

pub fn import_schema(schema: &daft_ext_abi::ArrowSchema) -> DaftResult<Schema> {
    inner::import_schema(schema).map_err(DaftError::RuntimeError)
}

pub fn export_field(field: &arrow_schema::Field) -> DaftResult<daft_ext_abi::ArrowSchema> {
    inner::export_field(field).map_err(DaftError::RuntimeError)
}

pub fn import_field(schema: &daft_ext_abi::ArrowSchema) -> DaftResult<arrow_schema::Field> {
    inner::import_field(schema).map_err(DaftError::RuntimeError)
}
