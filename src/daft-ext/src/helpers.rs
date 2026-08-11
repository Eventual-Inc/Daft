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
        /// Most functions want one of the safe scalar accessors instead
        /// ([`literal_i64`], [`literal_u64`], [`literal_f64`], [`literal_bool`],
        /// [`literal_string`], [`literal_binary`]); reach for this only when the
        /// literal is a nested or otherwise non-scalar value.
        ///
        /// # Safety
        ///
        /// - `arg` must be a descriptor supplied by the host to
        ///   [`DaftScalarFunction::return_field`](crate::function::DaftScalarFunction::return_field),
        ///   so that its `literal` really is described by its `field`. A
        ///   hand-built descriptor whose field does not describe its array makes
        ///   arrow-rs reinterpret the buffers.
        /// - Nothing derived from the array may escape `f`. The array borrows the
        ///   descriptor's buffers, and the host releases those as soon as
        ///   `return_field` returns; a clone, slice, or `ArrayData` that outlives
        ///   the call is a use-after-free. Copy values out instead.
        pub unsafe fn with_literal<R>(
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

        /// Read a scalar out of an argument's literal value.
        ///
        /// `read` is handed the length-1 literal array and returns the value at
        /// index 0, or `None` if the array is not of a type it understands.
        /// Safe: the value is copied out, so nothing borrowed escapes.
        fn read_literal<T>(
            arg: &ArgDescriptor,
            what: &str,
            read: impl FnOnce(&ArrayRef) -> Option<T>,
        ) -> DaftResult<Option<T>> {
            // SAFETY: `arg` is a host-supplied descriptor by the contract of the
            // public accessors below, and `read` returns an owned value.
            let value = unsafe {
                with_literal(arg, |array| {
                    use $arrow_array_crate::Array as _;
                    if array.is_empty() || array.is_null(0) {
                        return Ok(None);
                    }
                    read(array).map(Some).ok_or_else(|| {
                        DaftError::TypeError(format!(
                            "{what}: literal has type {:?}",
                            array.data_type()
                        ))
                    })
                })?
            };
            Ok(value.flatten())
        }

        /// Read a signed integer literal (`Int8` through `Int64`).
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not a signed
        /// integer.
        pub fn literal_i64(arg: &ArgDescriptor) -> DaftResult<Option<i64>> {
            use $arrow_array_crate::{
                cast::AsArray as _,
                types::{Int8Type, Int16Type, Int32Type, Int64Type},
            };
            read_literal(arg, "literal_i64", |array| match array.data_type() {
                $arrow_schema_crate::DataType::Int8 => {
                    Some(i64::from(array.as_primitive::<Int8Type>().value(0)))
                }
                $arrow_schema_crate::DataType::Int16 => {
                    Some(i64::from(array.as_primitive::<Int16Type>().value(0)))
                }
                $arrow_schema_crate::DataType::Int32 => {
                    Some(i64::from(array.as_primitive::<Int32Type>().value(0)))
                }
                $arrow_schema_crate::DataType::Int64 => {
                    Some(array.as_primitive::<Int64Type>().value(0))
                }
                _ => None,
            })
        }

        /// Read an unsigned integer literal (`UInt8` through `UInt64`).
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not an unsigned
        /// integer.
        pub fn literal_u64(arg: &ArgDescriptor) -> DaftResult<Option<u64>> {
            use $arrow_array_crate::{
                cast::AsArray as _,
                types::{UInt8Type, UInt16Type, UInt32Type, UInt64Type},
            };
            read_literal(arg, "literal_u64", |array| match array.data_type() {
                $arrow_schema_crate::DataType::UInt8 => {
                    Some(u64::from(array.as_primitive::<UInt8Type>().value(0)))
                }
                $arrow_schema_crate::DataType::UInt16 => {
                    Some(u64::from(array.as_primitive::<UInt16Type>().value(0)))
                }
                $arrow_schema_crate::DataType::UInt32 => {
                    Some(u64::from(array.as_primitive::<UInt32Type>().value(0)))
                }
                $arrow_schema_crate::DataType::UInt64 => {
                    Some(array.as_primitive::<UInt64Type>().value(0))
                }
                _ => None,
            })
        }

        /// Read a floating-point literal (`Float32` or `Float64`).
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not a float.
        pub fn literal_f64(arg: &ArgDescriptor) -> DaftResult<Option<f64>> {
            use $arrow_array_crate::{
                cast::AsArray as _,
                types::{Float32Type, Float64Type},
            };
            read_literal(arg, "literal_f64", |array| match array.data_type() {
                $arrow_schema_crate::DataType::Float32 => {
                    Some(f64::from(array.as_primitive::<Float32Type>().value(0)))
                }
                $arrow_schema_crate::DataType::Float64 => {
                    Some(array.as_primitive::<Float64Type>().value(0))
                }
                _ => None,
            })
        }

        /// Read a boolean literal.
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not a boolean.
        pub fn literal_bool(arg: &ArgDescriptor) -> DaftResult<Option<bool>> {
            use $arrow_array_crate::cast::AsArray as _;
            read_literal(arg, "literal_bool", |array| match array.data_type() {
                $arrow_schema_crate::DataType::Boolean => Some(array.as_boolean().value(0)),
                _ => None,
            })
        }

        /// Read a string literal (`Utf8` or `LargeUtf8`).
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not a string.
        pub fn literal_string(arg: &ArgDescriptor) -> DaftResult<Option<String>> {
            use $arrow_array_crate::cast::AsArray as _;
            read_literal(arg, "literal_string", |array| match array.data_type() {
                $arrow_schema_crate::DataType::Utf8 => {
                    Some(array.as_string::<i32>().value(0).to_owned())
                }
                $arrow_schema_crate::DataType::LargeUtf8 => {
                    Some(array.as_string::<i64>().value(0).to_owned())
                }
                _ => None,
            })
        }

        /// Read a binary literal (`Binary` or `LargeBinary`).
        ///
        /// Returns `Ok(None)` when the argument is not a literal or its value is
        /// null, and an error when the literal is present but not binary.
        pub fn literal_binary(arg: &ArgDescriptor) -> DaftResult<Option<Vec<u8>>> {
            use $arrow_array_crate::cast::AsArray as _;
            read_literal(arg, "literal_binary", |array| match array.data_type() {
                $arrow_schema_crate::DataType::Binary => {
                    Some(array.as_binary::<i32>().value(0).to_vec())
                }
                $arrow_schema_crate::DataType::LargeBinary => {
                    Some(array.as_binary::<i64>().value(0).to_vec())
                }
                _ => None,
            })
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
