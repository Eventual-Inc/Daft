//! Convenience helpers for converting between `daft-ext` FFI types and arrow-rs types.
//!
//! Requires one of the `arrow-56`, `arrow-57`, `arrow-58`, or `arrow-59` feature flags.

#[allow(unused_macros)]
macro_rules! impl_helpers {
    ($arrow_schema_crate:ident, $arrow_data_crate:ident, $arrow_array_crate:ident) => {
        use $arrow_array_crate::ArrayRef;
        use $arrow_schema_crate::Field;

        use crate::{
            abi::{ArgDescriptor, ArrowArray, ArrowData, ArrowSchema},
            error::{DaftError, DaftResult},
        };

        /// Convert an [`ArrowData`] (FFI) into an arrow-rs [`ArrayRef`].
        pub fn import_array(data: ArrowData) -> DaftResult<ArrayRef> {
            let ffi_array: $arrow_array_crate::ffi::FFI_ArrowArray =
                unsafe { data.array.into_owned() };
            let ffi_schema: $arrow_array_crate::ffi::FFI_ArrowSchema =
                unsafe { data.schema.into_owned() };
            let arrow_data = unsafe { $arrow_array_crate::ffi::from_ffi(ffi_array, &ffi_schema) }
                .map_err(|e| {
                DaftError::RuntimeError(format!("Arrow FFI import failed: {e}"))
            })?;
            Ok($arrow_array_crate::make_array(arrow_data))
        }

        /// Convert an arrow-rs [`ArrayRef`] into an [`ArrowData`] (FFI) with the given field name.
        pub fn export_array(array: ArrayRef, field_name: &str) -> DaftResult<ArrowData> {
            let field = Field::new(field_name, array.data_type().clone(), true);
            let ffi_schema = $arrow_array_crate::ffi::FFI_ArrowSchema::try_from(&field)
                .map_err(|e| DaftError::RuntimeError(format!("schema export failed: {e}")))?;
            let mut arrow_data = array.to_data();
            arrow_data.align_buffers();
            let ffi_array = $arrow_array_crate::ffi::FFI_ArrowArray::new(&arrow_data);
            Ok(ArrowData {
                schema: unsafe { ArrowSchema::from_owned(ffi_schema) },
                array: unsafe { ArrowArray::from_owned(ffi_array) },
            })
        }

        /// Convert an [`ArrowSchema`] (FFI) into an arrow-rs [`Field`].
        pub fn import_field(schema: &ArrowSchema) -> DaftResult<Field> {
            let ffi: &$arrow_array_crate::ffi::FFI_ArrowSchema = unsafe { schema.as_raw() };
            Field::try_from(ffi)
                .map_err(|e| DaftError::RuntimeError(format!("schema import failed: {e}")))
        }

        /// Convert an arrow-rs [`Field`] into an [`ArrowSchema`] (FFI).
        pub fn export_field(field: &Field) -> DaftResult<ArrowSchema> {
            let ffi = $arrow_array_crate::ffi::FFI_ArrowSchema::try_from(field)
                .map_err(|e| DaftError::RuntimeError(format!("schema export failed: {e}")))?;
            Ok(unsafe { ArrowSchema::from_owned(ffi) })
        }

        /// Release callback for a borrowed view over a C Data Interface struct.
        ///
        /// The buffers belong to the descriptor the view was taken from, so
        /// there is nothing to free — but the callback must be non-null for
        /// arrow-rs to accept the struct as un-released.
        unsafe extern "C" fn noop_release_array(array: *mut ArrowArray) {
            unsafe { (*array).release = None };
        }

        unsafe extern "C" fn noop_release_schema(schema: *mut ArrowSchema) {
            unsafe { (*schema).release = None };
        }

        /// Import an argument's literal value as an arrow-rs array, for the
        /// duration of `f`.
        ///
        /// Returns `Ok(None)` when the argument is not a foldable constant.
        /// Otherwise `f` is called with a length-1 array whose type is the one
        /// described by [`ArgDescriptor::field`].
        ///
        /// The array borrows the descriptor's buffers, which the host owns only
        /// for the duration of `return_field` — hence the scoped closure rather
        /// than a returned [`ArrayRef`].
        pub fn with_literal<R>(
            arg: &ArgDescriptor,
            f: impl FnOnce(&ArrayRef) -> DaftResult<R>,
        ) -> DaftResult<Option<R>> {
            let Some(literal) = arg.literal() else {
                return Ok(None);
            };

            // Shallow, non-owning views: same pointers, but a release callback
            // that frees nothing. Dropping the arrow-rs wrappers must not free
            // buffers still owned by the host.
            let mut array_view: ArrowArray = unsafe { std::ptr::read(literal) };
            array_view.release = Some(noop_release_array);
            array_view.private_data = std::ptr::null_mut();

            let mut schema_view: ArrowSchema = unsafe { std::ptr::read(arg.field()) };
            schema_view.release = Some(noop_release_schema);
            schema_view.private_data = std::ptr::null_mut();

            let ffi_array: $arrow_array_crate::ffi::FFI_ArrowArray =
                unsafe { array_view.into_owned() };
            let ffi_schema: $arrow_array_crate::ffi::FFI_ArrowSchema =
                unsafe { schema_view.into_owned() };

            let data = unsafe { $arrow_array_crate::ffi::from_ffi(ffi_array, &ffi_schema) }
                .map_err(|e| DaftError::RuntimeError(format!("literal FFI import failed: {e}")))?;
            let array = $arrow_array_crate::make_array(data);
            f(&array).map(Some)
        }

        /// Create an arrow-rs [`Field`] with the given name and [`DataType`](arrow DataType).
        ///
        /// Used by generated macro code to avoid re-exporting arrow crates.
        pub fn new_field(name: &str, dtype: $arrow_schema_crate::DataType) -> Field {
            Field::new(name, dtype, true)
        }

        /// Internal re-exports for `#[daft_func]` generated code.
        /// Not part of the public API.
        #[doc(hidden)]
        pub mod _codegen {
            pub use std::sync::Arc;

            pub use $arrow_array_crate::{
                Array, ArrayRef, BooleanArray, FixedSizeListArray, LargeBinaryArray,
                LargeListArray, LargeStringArray, PrimitiveArray,
                builder::{
                    BooleanBuilder, FixedSizeListBuilder, LargeBinaryBuilder, LargeListBuilder,
                    LargeStringBuilder, PrimitiveBuilder,
                },
                types::{
                    ArrowPrimitiveType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
                    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
                },
            };
            pub use $arrow_schema_crate::{DataType, Field as ArrowField};

            pub type Int8Array = PrimitiveArray<Int8Type>;
            pub type Int16Array = PrimitiveArray<Int16Type>;
            pub type Int32Array = PrimitiveArray<Int32Type>;
            pub type Int64Array = PrimitiveArray<Int64Type>;
            pub type UInt8Array = PrimitiveArray<UInt8Type>;
            pub type UInt16Array = PrimitiveArray<UInt16Type>;
            pub type UInt32Array = PrimitiveArray<UInt32Type>;
            pub type UInt64Array = PrimitiveArray<UInt64Type>;
            pub type Float32Array = PrimitiveArray<Float32Type>;
            pub type Float64Array = PrimitiveArray<Float64Type>;
        }
    };
}

// When multiple arrow features are enabled (e.g. --all-features), pick exactly one.
// Prefer the highest version.
#[cfg(feature = "arrow-59")]
impl_helpers!(arrow_schema_59, arrow_data_59, arrow_array_59);

#[cfg(all(feature = "arrow-58", not(feature = "arrow-59")))]
impl_helpers!(arrow_schema_58, arrow_data_58, arrow_array_58);

#[cfg(all(
    feature = "arrow-57",
    not(feature = "arrow-58"),
    not(feature = "arrow-59")
))]
impl_helpers!(arrow_schema_57, arrow_data_57, arrow_array_57);

#[cfg(all(
    feature = "arrow-56",
    not(feature = "arrow-57"),
    not(feature = "arrow-58"),
    not(feature = "arrow-59")
))]
impl_helpers!(arrow_schema_56, arrow_data_56, arrow_array_56);
