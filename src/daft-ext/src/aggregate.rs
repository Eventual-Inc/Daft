use std::{
    ffi::{CStr, c_char, c_int, c_void},
    sync::Arc,
};

use crate::{
    abi::{ArrowArray, ArrowData, ArrowSchema, FFI_AggregateFunction},
    error::DaftResult,
    ffi::trampoline::trampoline,
};

/// Trait that extension authors implement to define an aggregate function (UDAF).
///
/// The execution pipeline follows three stages:
///
/// ```text
/// Aggregation:   aggregate(inputs) → Vec<ArrowData>   (partial state)
/// Combination:   combine(states)   → Vec<ArrowData>   (merge partial states)
/// Finalization:  finalize(states)  → ArrowData         (final output)
/// ```
///
/// State is exchanged as individual `ArrowData` values (one per state field).
/// The FFI layer packs/unpacks these into a Struct array transparently.
pub trait DaftAggregateFunction {
    fn name(&self) -> &CStr;
    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema>;
    fn state_fields(&self, args: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>>;
    fn aggregate(&self, inputs: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>>;
    fn combine(&self, states: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>>;
    fn finalize(&self, states: Vec<ArrowData>) -> DaftResult<ArrowData>;
}

pub type DaftAggregateFunctionRef = Arc<dyn DaftAggregateFunction>;

/// Convert a [`DaftAggregateFunctionRef`] into a [`FFI_AggregateFunction`] vtable.
pub fn into_ffi(func: DaftAggregateFunctionRef) -> FFI_AggregateFunction {
    let ctx_ptr = Box::into_raw(Box::new(func));
    FFI_AggregateFunction {
        ctx: ctx_ptr.cast(),
        name: ffi_name,
        get_return_field: ffi_get_return_field,
        get_state_schema: ffi_get_state_schema,
        aggregate: ffi_aggregate,
        combine: ffi_combine,
        finalize: ffi_finalize,
        fini: ffi_fini,
    }
}

unsafe extern "C" fn ffi_name(ctx: *const c_void) -> *const c_char {
    unsafe { &*ctx.cast::<DaftAggregateFunctionRef>() }
        .name()
        .as_ptr()
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_get_return_field(
    ctx:        *const c_void,
    args:       *const ArrowSchema,
    args_count: usize,
    ret:        *mut ArrowSchema,
    errmsg:     *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in aggregate get_return_field", || {
        let ctx = &*ctx.cast::<DaftAggregateFunctionRef>();
        let schemas = if args_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(args, args_count)
        };
        let result = ctx.return_field(schemas)?;
        std::ptr::write(ret, result);
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_get_state_schema(
    ctx:        *const c_void,
    args:       *const ArrowSchema,
    args_count: usize,
    ret:        *mut ArrowSchema,
    errmsg:     *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in aggregate get_state_schema", || {
        let ctx = &*ctx.cast::<DaftAggregateFunctionRef>();
        let schemas = if args_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(args, args_count)
        };
        let children = ctx.state_fields(schemas)?;
        let struct_schema = ArrowSchema::new_struct("state", children);
        std::ptr::write(ret, struct_schema);
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_aggregate(
    ctx:          *const c_void,
    args:         *const ArrowArray,
    args_schemas: *const ArrowSchema,
    args_count:   usize,
    ret_array:    *mut ArrowArray,
    ret_schema:   *mut ArrowSchema,
    errmsg:       *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in aggregate", || {
        let ctx = &*ctx.cast::<DaftAggregateFunctionRef>();
        let mut inputs = Vec::with_capacity(args_count);
        for i in 0..args_count {
            let array = std::ptr::read(args.add(i));
            let schema = std::ptr::read(args_schemas.add(i));
            inputs.push(ArrowData { schema, array });
        }
        let results = ctx.aggregate(inputs)?;
        pack_struct_result(results, ret_array, ret_schema);
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_combine(
    ctx:          *const c_void,
    state_array:  *const ArrowArray,
    state_schema: *const ArrowSchema,
    ret_array:    *mut ArrowArray,
    ret_schema:   *mut ArrowSchema,
    errmsg:       *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in aggregate combine", || {
        let ctx = &*ctx.cast::<DaftAggregateFunctionRef>();
        let states = unpack_struct_input(state_array, state_schema);
        let results = ctx.combine(states)?;
        pack_struct_result(results, ret_array, ret_schema);
        Ok(())
    })}
}

#[rustfmt::skip]
unsafe extern "C" fn ffi_finalize(
    ctx:          *const c_void,
    state_array:  *const ArrowArray,
    state_schema: *const ArrowSchema,
    ret_array:    *mut ArrowArray,
    ret_schema:   *mut ArrowSchema,
    errmsg:       *mut *mut c_char,
) -> c_int {
    unsafe { trampoline(errmsg, "panic in aggregate finalize", || {
        let ctx = &*ctx.cast::<DaftAggregateFunctionRef>();
        let states = unpack_struct_input(state_array, state_schema);
        let result = ctx.finalize(states)?;
        std::ptr::write(ret_array, result.array);
        std::ptr::write(ret_schema, result.schema);
        Ok(())
    })}
}

unsafe extern "C" fn ffi_fini(ctx: *mut c_void) {
    let _ = std::panic::catch_unwind(|| unsafe {
        drop(Box::from_raw(ctx.cast::<DaftAggregateFunctionRef>()));
    });
}

// ── Struct packing/unpacking helpers ───────────────────────────────

/// Pack `Vec<ArrowData>` results into a single Struct ArrowArray + ArrowSchema.
unsafe fn pack_struct_result(
    results: Vec<ArrowData>,
    ret_array: *mut ArrowArray,
    ret_schema: *mut ArrowSchema,
) {
    let mut child_arrays = Vec::with_capacity(results.len());
    let mut child_schemas = Vec::with_capacity(results.len());
    for data in results {
        child_schemas.push(data.schema);
        child_arrays.push(data.array);
    }
    let length = child_arrays.first().map_or(0, |a| a.length);
    unsafe {
        std::ptr::write(ret_array, ArrowArray::new_struct(child_arrays, length));
        std::ptr::write(ret_schema, ArrowSchema::new_struct("state", child_schemas));
    }
}

/// Unpack a Struct ArrowArray + ArrowSchema into `Vec<ArrowData>`.
unsafe fn unpack_struct_input(
    state_array: *const ArrowArray,
    state_schema: *const ArrowSchema,
) -> Vec<ArrowData> {
    let mut array = unsafe { std::ptr::read(state_array) };
    let mut schema = unsafe { std::ptr::read(state_schema) };

    let child_arrays = unsafe { array.take_struct_children() };
    let child_schemas = unsafe { schema.take_struct_children() };

    child_schemas
        .into_iter()
        .zip(child_arrays)
        .map(|(s, a)| ArrowData {
            schema: s,
            array: a,
        })
        .collect()
}

#[cfg(all(test, feature = "arrow-57"))]
mod tests {
    use arrow_schema_57::{DataType, Field, Schema};

    use super::*;
    use crate::{abi::ffi::strings::free_string, error::DaftError};

    fn export_schema(schema: &Schema) -> ArrowSchema {
        let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(schema).unwrap();
        unsafe { ArrowSchema::from_owned(ffi) }
    }

    fn export_field(field: &Field) -> ArrowSchema {
        let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(field).unwrap();
        unsafe { ArrowSchema::from_owned(ffi) }
    }

    fn import_schema(schema: &ArrowSchema) -> Schema {
        let ffi: &arrow_schema_57::ffi::FFI_ArrowSchema = unsafe { schema.as_raw() };
        Schema::try_from(ffi).unwrap()
    }

    fn make_int32(values: &[i32]) -> ArrowData {
        let schema = {
            let field = Field::new("", DataType::Int32, false);
            let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(&field).unwrap();
            unsafe { ArrowSchema::from_owned(ffi) }
        };
        let values: Box<[i32]> = values.into();
        let len = values.len();
        let buffers = Box::new([std::ptr::null::<c_void>(), values.as_ptr().cast::<c_void>()]);
        let private = Box::new((values, std::ptr::null::<c_void>()));
        let array = ArrowArray {
            length: len as i64,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: Box::into_raw(buffers).cast::<*const c_void>(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_int32),
            private_data: Box::into_raw(private).cast::<c_void>(),
        };
        ArrowData { schema, array }
    }

    unsafe extern "C" fn release_int32(array: *mut ArrowArray) {
        let a = unsafe { &mut *array };
        drop(unsafe { Box::from_raw(a.private_data.cast::<(Box<[i32]>, *const c_void)>()) });
        drop(unsafe { Box::from_raw(a.buffers.cast::<[*const c_void; 2]>()) });
        a.release = None;
    }

    fn read_int32(data: &ArrowData) -> &[i32] {
        unsafe {
            let bufs = std::slice::from_raw_parts(data.array.buffers.cast_const(), 2);
            std::slice::from_raw_parts(bufs[1].cast::<i32>(), data.array.length as usize)
        }
    }

    /// A sum aggregate: state = (sum: i32, count: i64).
    struct SumAggFn;

    impl DaftAggregateFunction for SumAggFn {
        fn name(&self) -> &CStr {
            c"test_sum"
        }

        fn return_field(&self, _args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
            Ok(export_schema(&Schema::new(vec![Field::new(
                "sum_result",
                DataType::Int32,
                false,
            )])))
        }

        fn state_fields(&self, _args: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>> {
            Ok(vec![
                export_field(&Field::new("sum", DataType::Int32, false)),
                export_field(&Field::new("count", DataType::Int64, false)),
            ])
        }

        fn aggregate(&self, inputs: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
            let values = read_int32(&inputs[0]);
            let sum: i32 = values.iter().sum();
            let count = values.len() as i64;
            Ok(vec![make_int32(&[sum]), make_int64(&[count])])
        }

        fn combine(&self, states: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
            let sums = read_int32(&states[0]);
            let counts = read_int64(&states[1]);
            let total_sum: i32 = sums.iter().sum();
            let total_count: i64 = counts.iter().sum();
            Ok(vec![make_int32(&[total_sum]), make_int64(&[total_count])])
        }

        fn finalize(&self, states: Vec<ArrowData>) -> DaftResult<ArrowData> {
            let sum = read_int32(&states[0])[0];
            Ok(make_int32(&[sum]))
        }
    }

    fn make_int64(values: &[i64]) -> ArrowData {
        let schema = {
            let field = Field::new("", DataType::Int64, false);
            let ffi = arrow_schema_57::ffi::FFI_ArrowSchema::try_from(&field).unwrap();
            unsafe { ArrowSchema::from_owned(ffi) }
        };
        let values: Box<[i64]> = values.into();
        let len = values.len();
        let buffers = Box::new([std::ptr::null::<c_void>(), values.as_ptr().cast::<c_void>()]);
        let private = Box::new((values, std::ptr::null::<c_void>()));
        let array = ArrowArray {
            length: len as i64,
            null_count: 0,
            offset: 0,
            n_buffers: 2,
            n_children: 0,
            buffers: Box::into_raw(buffers).cast::<*const c_void>(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_int64),
            private_data: Box::into_raw(private).cast::<c_void>(),
        };
        ArrowData { schema, array }
    }

    unsafe extern "C" fn release_int64(array: *mut ArrowArray) {
        let a = unsafe { &mut *array };
        drop(unsafe { Box::from_raw(a.private_data.cast::<(Box<[i64]>, *const c_void)>()) });
        drop(unsafe { Box::from_raw(a.buffers.cast::<[*const c_void; 2]>()) });
        a.release = None;
    }

    fn read_int64(data: &ArrowData) -> &[i64] {
        unsafe {
            let bufs = std::slice::from_raw_parts(data.array.buffers.cast_const(), 2);
            std::slice::from_raw_parts(bufs[1].cast::<i64>(), data.array.length as usize)
        }
    }

    #[test]
    fn vtable_name_roundtrip() {
        let vtable = into_ffi(Arc::new(SumAggFn));
        let name = unsafe { CStr::from_ptr((vtable.name)(vtable.ctx)) };
        assert_eq!(name.to_str().unwrap(), "test_sum");
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_get_return_field() {
        let vtable = into_ffi(Arc::new(SumAggFn));
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        let schema = import_schema(&ret_schema);
        assert_eq!(schema.field(0).name(), "sum_result");

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_get_state_schema() {
        let vtable = into_ffi(Arc::new(SumAggFn));
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_state_schema)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(ret_schema.n_children, 2);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_aggregate() {
        let vtable = into_ffi(Arc::new(SumAggFn));
        let input = make_int32(&[1, 2, 3, 4]);

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.aggregate)(
                vtable.ctx,
                &raw const input.array,
                &raw const input.schema,
                1,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(
            ret_array.n_children, 2,
            "should be a Struct with 2 children"
        );
        assert_eq!(ret_array.length, 1, "single-row state");

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_error_propagation() {
        struct FailAgg;
        impl DaftAggregateFunction for FailAgg {
            fn name(&self) -> &CStr {
                c"fail_agg"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                Err(DaftError::TypeError("bad return".into()))
            }
            fn state_fields(&self, _: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>> {
                Err(DaftError::TypeError("bad state".into()))
            }
            fn aggregate(&self, _: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
                Err(DaftError::RuntimeError("aggregate failed".into()))
            }
            fn combine(&self, _: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
                Err(DaftError::RuntimeError("combine failed".into()))
            }
            fn finalize(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Err(DaftError::RuntimeError("finalize failed".into()))
            }
        }

        let vtable = into_ffi(Arc::new(FailAgg));
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_ne!(rc, 0);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(msg.contains("bad return"), "error: {msg}");
        unsafe { free_string(errmsg) };

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_combine() {
        let vtable = into_ffi(Arc::new(SumAggFn));

        // First produce two partial states via aggregate
        let input1 = make_int32(&[1, 2, 3]);
        let input2 = make_int32(&[4, 5, 6]);

        let mut state1_array = ArrowArray::empty();
        let mut state1_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.aggregate)(
                vtable.ctx,
                &raw const input1.array,
                &raw const input1.schema,
                1,
                &raw mut state1_array,
                &raw mut state1_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);

        let mut state2_array = ArrowArray::empty();
        let mut state2_schema = ArrowSchema::empty();
        errmsg = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.aggregate)(
                vtable.ctx,
                &raw const input2.array,
                &raw const input2.schema,
                1,
                &raw mut state2_array,
                &raw mut state2_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);

        // Manually merge the two single-row Struct states into a multi-row Struct
        // by extracting children and concatenating them.
        let s1_sums = unsafe { std::ptr::read(state1_array.children) };
        let s2_sums = unsafe { std::ptr::read(state2_array.children) };
        let s1_counts = unsafe { std::ptr::read(state1_array.children.add(1)) };
        let s2_counts = unsafe { std::ptr::read(state2_array.children.add(1)) };

        let sum1 = read_int32_raw(s1_sums);
        let sum2 = read_int32_raw(s2_sums);
        let count1 = read_int64_raw(s1_counts);
        let count2 = read_int64_raw(s2_counts);

        let combined_sums = make_int32(&[sum1[0], sum2[0]]);
        let combined_counts = make_int64(&[count1[0], count2[0]]);

        let merged = ArrowArray::new_struct(vec![combined_sums.array, combined_counts.array], 2);
        let merged_schema =
            ArrowSchema::new_struct("state", vec![combined_sums.schema, combined_counts.schema]);

        // Now call combine
        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        errmsg = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.combine)(
                vtable.ctx,
                &raw const merged,
                &raw const merged_schema,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(
            ret_array.n_children, 2,
            "combined state should have 2 children"
        );
        assert_eq!(ret_array.length, 1, "combined state should be single-row");

        // Verify the combined sum: 1+2+3 + 4+5+6 = 21
        let result_sum = unsafe { std::ptr::read(ret_array.children) };
        let sum_values = read_int32_raw(result_sum);
        assert_eq!(sum_values[0], 21);

        // Verify the combined count: 3 + 3 = 6
        let result_count = unsafe { std::ptr::read(ret_array.children.add(1)) };
        let count_values = read_int64_raw(result_count);
        assert_eq!(count_values[0], 6);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_finalize() {
        let vtable = into_ffi(Arc::new(SumAggFn));

        // Produce a partial state via aggregate
        let input = make_int32(&[10, 20, 30]);
        let mut state_array = ArrowArray::empty();
        let mut state_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.aggregate)(
                vtable.ctx,
                &raw const input.array,
                &raw const input.schema,
                1,
                &raw mut state_array,
                &raw mut state_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);

        // Now finalize
        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        errmsg = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.finalize)(
                vtable.ctx,
                &raw const state_array,
                &raw const state_schema,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);
        assert_eq!(ret_array.length, 1, "finalize should produce single value");

        // Verify the final sum: 10+20+30 = 60
        let result_data = ArrowData {
            schema: ret_schema,
            array: ret_array,
        };
        let values = read_int32(&result_data);
        assert_eq!(values[0], 60);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_error_propagation_aggregate() {
        struct FailAgg2;
        impl DaftAggregateFunction for FailAgg2 {
            fn name(&self) -> &CStr {
                c"fail_agg2"
            }
            fn return_field(&self, _: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
                unreachable!()
            }
            fn state_fields(&self, _: &[ArrowSchema]) -> DaftResult<Vec<ArrowSchema>> {
                unreachable!()
            }
            fn aggregate(&self, _: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
                Err(DaftError::RuntimeError("aggregate exploded".into()))
            }
            fn combine(&self, _: Vec<ArrowData>) -> DaftResult<Vec<ArrowData>> {
                Err(DaftError::RuntimeError("combine exploded".into()))
            }
            fn finalize(&self, _: Vec<ArrowData>) -> DaftResult<ArrowData> {
                Err(DaftError::RuntimeError("finalize exploded".into()))
            }
        }

        let vtable = into_ffi(Arc::new(FailAgg2));
        let input = make_int32(&[1]);

        // aggregate error
        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.aggregate)(
                vtable.ctx,
                &raw const input.array,
                &raw const input.schema,
                1,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_ne!(rc, 0);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(msg.contains("aggregate exploded"), "error: {msg}");
        unsafe { free_string(errmsg) };

        // combine error
        let empty_struct = ArrowArray::new_struct(vec![], 0);
        let empty_struct_schema = ArrowSchema::new_struct("state", vec![]);
        errmsg = std::ptr::null_mut();

        let rc = unsafe {
            (vtable.combine)(
                vtable.ctx,
                &raw const empty_struct,
                &raw const empty_struct_schema,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_ne!(rc, 0);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(msg.contains("combine exploded"), "error: {msg}");
        unsafe { free_string(errmsg) };

        // finalize error
        errmsg = std::ptr::null_mut();

        let empty_struct2 = ArrowArray::new_struct(vec![], 0);
        let empty_struct_schema2 = ArrowSchema::new_struct("state", vec![]);

        let rc = unsafe {
            (vtable.finalize)(
                vtable.ctx,
                &raw const empty_struct2,
                &raw const empty_struct_schema2,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_ne!(rc, 0);
        assert!(!errmsg.is_null());
        let msg = unsafe { CStr::from_ptr(errmsg) }.to_str().unwrap();
        assert!(msg.contains("finalize exploded"), "error: {msg}");
        unsafe { free_string(errmsg) };

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn vtable_zero_args() {
        let vtable = into_ffi(Arc::new(SumAggFn));

        // get_return_field with zero args
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            (vtable.get_return_field)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);

        // get_state_schema with zero args
        let mut ret_schema2 = ArrowSchema::empty();
        errmsg = std::ptr::null_mut();
        let rc = unsafe {
            (vtable.get_state_schema)(
                vtable.ctx,
                std::ptr::null(),
                0,
                &raw mut ret_schema2,
                &raw mut errmsg,
            )
        };
        assert_eq!(rc, 0);

        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    #[test]
    fn fini_is_callable() {
        let vtable = into_ffi(Arc::new(SumAggFn));
        unsafe { (vtable.fini)(vtable.ctx.cast_mut()) };
    }

    // Helper to read i32 values from a raw ArrowArray pointer
    fn read_int32_raw(array: *mut ArrowArray) -> &'static [i32] {
        unsafe {
            let a = &*array;
            let bufs = std::slice::from_raw_parts(a.buffers.cast_const(), 2);
            std::slice::from_raw_parts(bufs[1].cast::<i32>(), a.length as usize)
        }
    }

    // Helper to read i64 values from a raw ArrowArray pointer
    fn read_int64_raw(array: *mut ArrowArray) -> &'static [i64] {
        unsafe {
            let a = &*array;
            let bufs = std::slice::from_raw_parts(a.buffers.cast_const(), 2);
            std::slice::from_raw_parts(bufs[1].cast::<i64>(), a.length as usize)
        }
    }
}
