//! Feature-gated conversions for arrow-rs FFI types.
//!
//! Enable one of `arrow-56`, `arrow-57`, or `arrow-58` to get:
//! - Safe `.into()` between `FFI_ArrowArray`/`FFI_ArrowSchema` and our types
//! - `TryFrom` between `arrow_schema::Field` and our `ArrowSchema`

#[allow(unused_macros)]
macro_rules! impl_arrow_conversions {
    ($arrow_schema_crate:ident, $arrow_data_crate:ident) => {
        // Compile-time layout assertions.
        const _: () = {
            assert!(
                std::mem::size_of::<crate::ArrowArray>()
                    == std::mem::size_of::<$arrow_data_crate::ffi::FFI_ArrowArray>()
            );
            assert!(
                std::mem::align_of::<crate::ArrowArray>()
                    == std::mem::align_of::<$arrow_data_crate::ffi::FFI_ArrowArray>()
            );
            assert!(
                std::mem::size_of::<crate::ArrowSchema>()
                    == std::mem::size_of::<$arrow_schema_crate::ffi::FFI_ArrowSchema>()
            );
            assert!(
                std::mem::align_of::<crate::ArrowSchema>()
                    == std::mem::align_of::<$arrow_schema_crate::ffi::FFI_ArrowSchema>()
            );
        };

        // ── FFI_ArrowArray ↔ ArrowArray ─────────────────────────────────

        impl From<$arrow_data_crate::ffi::FFI_ArrowArray> for crate::ArrowArray {
            fn from(val: $arrow_data_crate::ffi::FFI_ArrowArray) -> Self {
                unsafe { Self::from_owned(val) }
            }
        }

        impl From<crate::ArrowArray> for $arrow_data_crate::ffi::FFI_ArrowArray {
            fn from(val: crate::ArrowArray) -> Self {
                unsafe { val.into_owned() }
            }
        }

        // ── FFI_ArrowSchema ↔ ArrowSchema ───────────────────────────────

        impl From<$arrow_schema_crate::ffi::FFI_ArrowSchema> for crate::ArrowSchema {
            fn from(val: $arrow_schema_crate::ffi::FFI_ArrowSchema) -> Self {
                unsafe { Self::from_owned(val) }
            }
        }

        impl From<crate::ArrowSchema> for $arrow_schema_crate::ffi::FFI_ArrowSchema {
            fn from(val: crate::ArrowSchema) -> Self {
                unsafe { val.into_owned() }
            }
        }

        // ── Field ↔ ArrowSchema (TryFrom) ──────────────────────────────

        impl TryFrom<&crate::ArrowSchema> for $arrow_schema_crate::Field {
            type Error = $arrow_schema_crate::ArrowError;

            fn try_from(schema: &crate::ArrowSchema) -> Result<Self, Self::Error> {
                let ffi: &$arrow_schema_crate::ffi::FFI_ArrowSchema = unsafe { schema.as_raw() };
                Self::try_from(ffi)
            }
        }

        impl TryFrom<&$arrow_schema_crate::Field> for crate::ArrowSchema {
            type Error = $arrow_schema_crate::ArrowError;

            fn try_from(field: &$arrow_schema_crate::Field) -> Result<Self, Self::Error> {
                let ffi = $arrow_schema_crate::ffi::FFI_ArrowSchema::try_from(field)?;
                Ok(ffi.into())
            }
        }

        // ── Schema ↔ ArrowSchema (TryFrom) ─────────────────────────────

        impl TryFrom<&crate::ArrowSchema> for $arrow_schema_crate::Schema {
            type Error = $arrow_schema_crate::ArrowError;

            fn try_from(schema: &crate::ArrowSchema) -> Result<Self, Self::Error> {
                let ffi: &$arrow_schema_crate::ffi::FFI_ArrowSchema = unsafe { schema.as_raw() };
                Self::try_from(ffi)
            }
        }

        impl TryFrom<&$arrow_schema_crate::Schema> for crate::ArrowSchema {
            type Error = $arrow_schema_crate::ArrowError;

            fn try_from(schema: &$arrow_schema_crate::Schema) -> Result<Self, Self::Error> {
                let ffi = $arrow_schema_crate::ffi::FFI_ArrowSchema::try_from(schema)?;
                Ok(ffi.into())
            }
        }
    };
}

#[cfg(feature = "arrow-56")]
impl_arrow_conversions!(arrow_schema_56, arrow_data_56);

#[cfg(feature = "arrow-57")]
impl_arrow_conversions!(arrow_schema_57, arrow_data_57);

#[cfg(feature = "arrow-58")]
impl_arrow_conversions!(arrow_schema_58, arrow_data_58);
