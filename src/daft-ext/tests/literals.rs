//! Tests for reading literal argument values out of an [`ArgDescriptor`].
//!
//! These exercise the borrowed-view import in `helpers`: the descriptor owns the
//! buffers, the extension only reads through them, and releasing the descriptor
//! afterwards must still be correct.

#![cfg(any(feature = "arrow-56", feature = "arrow-57", feature = "arrow-58"))]

use std::sync::Arc;

use daft_ext::{
    helpers::_codegen::{
        ArrayRef, BooleanArray, DataType, Int32Array, Int64Array, LargeStringArray,
    },
    prelude::*,
};

/// Build a descriptor over a length-1 array, the way the host does.
fn descriptor(name: &str, array: ArrayRef) -> ArgDescriptor {
    let data = export_array(array, name).unwrap();
    ArgDescriptor::new(data.schema, Some(data.array))
}

fn column(name: &str, dtype: DataType) -> ArgDescriptor {
    let field = daft_ext::helpers::new_field(name, dtype);
    ArgDescriptor::new(export_field(&field).unwrap(), None)
}

#[test]
fn reads_signed_integers() {
    let mut arg = descriptor("k", Arc::new(Int64Array::from(vec![7i64])));
    assert_eq!(literal_i64(&arg).unwrap(), Some(7));
    unsafe { arg.release() };

    // Narrower signed types widen.
    let mut arg = descriptor("k", Arc::new(Int32Array::from(vec![-3i32])));
    assert_eq!(literal_i64(&arg).unwrap(), Some(-3));
    unsafe { arg.release() };
}

#[test]
fn reads_other_scalar_types() {
    let mut arg = descriptor("b", Arc::new(BooleanArray::from(vec![true])));
    assert_eq!(literal_bool(&arg).unwrap(), Some(true));
    unsafe { arg.release() };

    let mut arg = descriptor("s", Arc::new(LargeStringArray::from(vec!["hi"])));
    assert_eq!(literal_string(&arg).unwrap().as_deref(), Some("hi"));
    unsafe { arg.release() };
}

#[test]
fn absent_literal_reads_as_none() {
    // A column argument carries no literal — not an error, just absent.
    let mut arg = column("x", DataType::Int64);
    assert_eq!(literal_i64(&arg).unwrap(), None);
    assert_eq!(literal_string(&arg).unwrap(), None);
    unsafe { arg.release() };
}

#[test]
fn null_literal_reads_as_none() {
    let mut arg = descriptor("k", Arc::new(Int64Array::from(vec![None::<i64>])));
    assert_eq!(literal_i64(&arg).unwrap(), None);
    unsafe { arg.release() };
}

#[test]
fn wrong_type_is_an_error() {
    let mut arg = descriptor("s", Arc::new(LargeStringArray::from(vec!["hi"])));
    let err = literal_i64(&arg).unwrap_err();
    assert!(err.to_string().contains("literal has type"), "{err}");
    unsafe { arg.release() };
}

#[test]
fn with_literal_sees_the_whole_array() {
    let mut arg = descriptor("k", Arc::new(Int64Array::from(vec![11i64])));

    // SAFETY: a well-formed descriptor, and only a copied-out value escapes.
    let len = unsafe { with_literal(&arg, |array| Ok(array.len())) }.unwrap();
    assert_eq!(len, Some(1));

    // Releasing afterwards must still free exactly once — the borrowed view
    // handed to arrow-rs owns nothing.
    unsafe { arg.release() };
    assert!(arg.literal().is_none());
}
