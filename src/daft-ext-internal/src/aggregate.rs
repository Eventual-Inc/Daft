use std::{
    ffi::{CStr, c_char},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{AggFn, AggFnHandle, State};
use daft_ext::abi::{ArrowArray, ArrowSchema, FFI_AggregateFunction};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::module::ModuleHandle;

struct Inner {
    ffi: FFI_AggregateFunction,
    module: Arc<ModuleHandle>,
}

impl Inner {
    unsafe fn take_ffi_string(&self, ptr: *mut c_char) -> String {
        let s = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { self.module.free_string(ptr) };
        s
    }

    fn check(&self, rc: i32, errmsg: *mut c_char, default_msg: &str) -> DaftResult<()> {
        if rc == 0 {
            return Ok(());
        }
        let msg = if errmsg.is_null() {
            default_msg.to_string()
        } else {
            unsafe { self.take_ffi_string(errmsg) }
        };
        Err(DaftError::InternalError(msg))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        unsafe { (self.ffi.fini)(self.ffi.ctx.cast_mut()) };
    }
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

#[derive(Clone)]
pub struct AggregateFunctionHandle {
    name: &'static str,
    inner: Option<Arc<Inner>>,
}

impl AggregateFunctionHandle {
    fn new(ffi: FFI_AggregateFunction, module: Arc<ModuleHandle>) -> Self {
        let name_ptr = unsafe { (ffi.name)(ffi.ctx) };
        let name: &'static str = Box::leak(
            unsafe { CStr::from_ptr(name_ptr) }
                .to_str()
                .expect("FFI aggregate function name must be valid UTF-8")
                .to_owned()
                .into_boxed_str(),
        );
        Self {
            name,
            inner: Some(Arc::new(Inner { ffi, module })),
        }
    }

    fn inner(&self) -> DaftResult<&Inner> {
        self.inner.as_deref().ok_or_else(|| {
            DaftError::InternalError(format!(
                "extension aggregate function '{}' is not loaded",
                self.name,
            ))
        })
    }

    fn fields_to_ffi_schemas(fields: &[Field]) -> DaftResult<Vec<ArrowSchema>> {
        fields
            .iter()
            .map(|f| {
                let arrow_field = f.to_arrow()?;
                let ffi = arrow::ffi::FFI_ArrowSchema::try_from(&arrow_field)
                    .map_err(|e| DaftError::InternalError(format!("schema export: {e}")))?;
                Ok(unsafe { ArrowSchema::from_owned(ffi) })
            })
            .collect()
    }

    fn ffi_schema_to_field(schema: ArrowSchema) -> DaftResult<Field> {
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { schema.into_owned() };
        let arrow_field = arrow_schema::Field::try_from(&ffi_schema)
            .map_err(|e| DaftError::InternalError(format!("schema import: {e}")))?;
        Field::try_from(&arrow_field)
    }

    fn series_to_ffi(s: &Series) -> DaftResult<(ArrowArray, ArrowSchema)> {
        let array = s.to_arrow()?;
        let target_field = s.field().to_arrow()?;
        let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(target_field)?;
        let mut data = array.to_data();
        data.align_buffers();
        let ffi_array = arrow::ffi::FFI_ArrowArray::new(&data);
        Ok((unsafe { ArrowArray::from_owned(ffi_array) }, unsafe {
            ArrowSchema::from_owned(ffi_schema)
        }))
    }

    fn ffi_to_series(array: ArrowArray, field: Field) -> DaftResult<Series> {
        let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { array.into_owned() };
        let arrow_field = field.to_arrow()?;
        let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(&arrow_field)
            .map_err(|e| DaftError::InternalError(format!("schema export: {e}")))?;
        let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }
            .map_err(|e| DaftError::InternalError(format!("Arrow FFI import: {e}")))?;
        let result_arr = arrow_array::make_array(arrow_data);
        Series::from_arrow(field, result_arr)
    }

    fn states_to_struct_ffi(states: Vec<Series>) -> DaftResult<(ArrowArray, ArrowSchema)> {
        let mut child_arrays = Vec::with_capacity(states.len());
        let mut child_schemas = Vec::with_capacity(states.len());
        for s in &states {
            let (arr, sch) = Self::series_to_ffi(s)?;
            child_arrays.push(arr);
            child_schemas.push(sch);
        }
        let length = states.first().map_or(0, |s| s.len() as i64);
        Ok((
            ArrowArray::new_struct(child_arrays, length),
            ArrowSchema::new_struct("state", child_schemas),
        ))
    }

    fn struct_ffi_to_literals(
        mut ret_array: ArrowArray,
        mut ret_schema: ArrowSchema,
    ) -> DaftResult<Vec<State>> {
        let child_arrays = unsafe { ret_array.take_struct_children() };
        let child_schemas = unsafe { ret_schema.take_struct_children() };

        let mut states = Vec::with_capacity(child_arrays.len());
        for (arr, sch) in child_arrays.into_iter().zip(child_schemas) {
            let field = Self::ffi_schema_to_field(sch)?;
            let series = Self::ffi_to_series(arr, field)?;
            states.push(series.get_lit(0));
        }
        Ok(states)
    }

    fn literals_to_struct_ffi(
        states: Vec<State>,
        state_fields: &[Field],
    ) -> DaftResult<(ArrowArray, ArrowSchema)> {
        let mut child_arrays = Vec::with_capacity(states.len());
        let mut child_schemas = Vec::with_capacity(states.len());
        for (lit, field) in states.into_iter().zip(state_fields) {
            let series = Series::from_literals(vec![lit])?
                .rename(&field.name)
                .cast(&field.dtype)?;
            let (arr, sch) = Self::series_to_ffi(&series)?;
            child_arrays.push(arr);
            child_schemas.push(sch);
        }
        Ok((
            ArrowArray::new_struct(child_arrays, 1),
            ArrowSchema::new_struct("state", child_schemas),
        ))
    }
}

#[typetag::serde(name = "AggregateFunctionHandle")]
impl AggFn for AggregateFunctionHandle {
    fn name(&self) -> &'static str {
        self.name
    }

    fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
        let inner = self.inner()?;

        let fields: Vec<Field> = input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Field::new(format!("arg{i}"), dt.clone()))
            .collect();
        let ffi_schemas = Self::fields_to_ffi_schemas(&fields)?;

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            (inner.ffi.get_return_field)(
                inner.ffi.ctx,
                ffi_schemas.as_ptr(),
                ffi_schemas.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "error in extension get_return_field")?;

        let field = Self::ffi_schema_to_field(ret_schema)?;
        Ok(field.dtype)
    }

    fn state_fields(&self, inputs: &[Field]) -> DaftResult<Vec<Field>> {
        let inner = self.inner()?;
        let ffi_schemas = Self::fields_to_ffi_schemas(inputs)?;

        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            (inner.ffi.get_state_schema)(
                inner.ffi.ctx,
                ffi_schemas.as_ptr(),
                ffi_schemas.len(),
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "error in extension get_state_schema")?;

        let mut children = unsafe { ret_schema.take_struct_children() };
        children.drain(..).map(Self::ffi_schema_to_field).collect()
    }

    fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        let inner = self.inner()?;

        let mut ffi_arrays = Vec::with_capacity(inputs.len());
        let mut ffi_schemas = Vec::with_capacity(inputs.len());
        for s in &inputs {
            let (arr, sch) = Self::series_to_ffi(s)?;
            ffi_arrays.push(arr);
            ffi_schemas.push(sch);
        }

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.aggregate)(
                inner.ffi.ctx,
                ffi_arrays.as_ptr(),
                ffi_schemas.as_ptr(),
                ffi_arrays.len(),
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "error in extension aggregate")?;

        Self::struct_ffi_to_literals(ret_array, ret_schema)
    }

    fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
        let inner = self.inner()?;

        let (state_array, state_schema) = Self::states_to_struct_ffi(states)?;

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.combine)(
                inner.ffi.ctx,
                &raw const state_array,
                &raw const state_schema,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "error in extension combine")?;

        Self::struct_ffi_to_literals(ret_array, ret_schema)
    }

    fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State> {
        let inner = self.inner()?;

        let input_fields = self.state_fields(&[])?;
        let (state_array, state_schema) = Self::literals_to_struct_ffi(state, &input_fields)?;

        let mut ret_array = ArrowArray::empty();
        let mut ret_schema = ArrowSchema::empty();
        let mut errmsg: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (inner.ffi.finalize)(
                inner.ffi.ctx,
                &raw const state_array,
                &raw const state_schema,
                &raw mut ret_array,
                &raw mut ret_schema,
                &raw mut errmsg,
            )
        };
        inner.check(rc, errmsg, "error in extension finalize")?;

        let field = Self::ffi_schema_to_field(ret_schema)?;
        let series = Self::ffi_to_series(ret_array, field)?;
        Ok(series.get_lit(0))
    }
}

impl Serialize for AggregateFunctionHandle {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("AggregateFunctionHandle", 1)?;
        s.serialize_field("name", self.name)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for AggregateFunctionHandle {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            name: String,
        }
        let h = Helper::deserialize(deserializer)?;
        Ok(Self {
            name: Box::leak(h.name.into_boxed_str()),
            inner: None,
        })
    }
}

pub fn into_aggregate_fn_handle(
    ffi: FFI_AggregateFunction,
    module: Arc<ModuleHandle>,
) -> AggFnHandle {
    AggFnHandle::new(Arc::new(AggregateFunctionHandle::new(ffi, module)))
}

#[cfg(test)]
mod tests {
    use std::ffi::{CString, c_int, c_void};

    use arrow_array::Int32Array;

    use super::*;

    // ── Mock FFI aggregate: sum(int32) ────────────────────────────────

    struct SumCtx;

    unsafe extern "C" fn mock_name(_ctx: *const c_void) -> *const c_char {
        c"test_sum".as_ptr()
    }

    unsafe extern "C" fn mock_get_return_field(
        _ctx: *const c_void,
        _args: *const ArrowSchema,
        _args_count: usize,
        ret: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let field = arrow_schema::Field::new("sum_result", arrow_schema::DataType::Int32, false);
        let schema = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&field).unwrap())
        };
        unsafe { std::ptr::write(ret, schema) };
        0
    }

    unsafe extern "C" fn mock_get_state_schema(
        _ctx: *const c_void,
        _args: *const ArrowSchema,
        _args_count: usize,
        ret: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let sum_field = arrow_schema::Field::new("sum", arrow_schema::DataType::Int32, false);
        let count_field = arrow_schema::Field::new("count", arrow_schema::DataType::Int32, false);
        let sum_sch = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&sum_field).unwrap())
        };
        let count_sch = unsafe {
            ArrowSchema::from_owned(arrow::ffi::FFI_ArrowSchema::try_from(&count_field).unwrap())
        };
        let struct_schema = ArrowSchema::new_struct("state", vec![sum_sch, count_sch]);
        unsafe { std::ptr::write(ret, struct_schema) };
        0
    }

    unsafe extern "C" fn mock_aggregate(
        _ctx: *const c_void,
        args: *const ArrowArray,
        args_schemas: *const ArrowSchema,
        args_count: usize,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        assert_eq!(args_count, 1);
        let abi_array = unsafe { std::ptr::read(args) };
        let abi_schema = unsafe { std::ptr::read(args_schemas) };
        let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { abi_array.into_owned() };
        let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { abi_schema.into_owned() };
        let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let arr = arrow_array::make_array(data);
        let input = arr.as_any().downcast_ref::<Int32Array>().unwrap();

        let sum: i32 = input.iter().map(|v| v.unwrap_or(0)).sum();
        let count = input.len() as i32;

        let sum_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![sum]));
        let count_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![count]));

        let (sum_ffi_arr, sum_ffi_sch) = arrow::ffi::to_ffi(&sum_arr.to_data()).unwrap();
        let (count_ffi_arr, count_ffi_sch) = arrow::ffi::to_ffi(&count_arr.to_data()).unwrap();

        let child_arrays = vec![unsafe { ArrowArray::from_owned(sum_ffi_arr) }, unsafe {
            ArrowArray::from_owned(count_ffi_arr)
        }];
        let child_schemas = vec![unsafe { ArrowSchema::from_owned(sum_ffi_sch) }, unsafe {
            ArrowSchema::from_owned(count_ffi_sch)
        }];

        unsafe {
            std::ptr::write(ret_array, ArrowArray::new_struct(child_arrays, 1));
            std::ptr::write(ret_schema, ArrowSchema::new_struct("state", child_schemas));
        }
        0
    }

    unsafe fn ffi_child_to_int32_array(arr: ArrowArray, sch: ArrowSchema) -> Int32Array {
        unsafe {
            let ffi_arr: arrow::ffi::FFI_ArrowArray = arr.into_owned();
            let ffi_sch: arrow::ffi::FFI_ArrowSchema = sch.into_owned();
            let data = arrow::ffi::from_ffi(ffi_arr, &ffi_sch).unwrap();
            let array = arrow_array::make_array(data);
            array.as_any().downcast_ref::<Int32Array>().unwrap().clone()
        }
    }

    unsafe extern "C" fn mock_combine(
        _ctx: *const c_void,
        state_array: *const ArrowArray,
        state_schema: *const ArrowSchema,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let mut array = unsafe { std::ptr::read(state_array) };
        let mut schema = unsafe { std::ptr::read(state_schema) };

        let mut child_arrays = unsafe { array.take_struct_children() };
        let mut child_schemas = unsafe { schema.take_struct_children() };

        let count_arr = child_arrays.pop().unwrap();
        let count_sch = child_schemas.pop().unwrap();
        let sum_arr = child_arrays.pop().unwrap();
        let sum_sch = child_schemas.pop().unwrap();

        let sums = unsafe { ffi_child_to_int32_array(sum_arr, sum_sch) };
        let counts = unsafe { ffi_child_to_int32_array(count_arr, count_sch) };

        let total_sum: i32 = sums.iter().map(|v| v.unwrap_or(0)).sum();
        let total_count: i32 = counts.iter().map(|v| v.unwrap_or(0)).sum();

        let sum_out: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![total_sum]));
        let count_out: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![total_count]));

        let (s_arr, s_sch) = arrow::ffi::to_ffi(&sum_out.to_data()).unwrap();
        let (c_arr, c_sch) = arrow::ffi::to_ffi(&count_out.to_data()).unwrap();

        unsafe {
            std::ptr::write(
                ret_array,
                ArrowArray::new_struct(
                    vec![ArrowArray::from_owned(s_arr), ArrowArray::from_owned(c_arr)],
                    1,
                ),
            );
            std::ptr::write(
                ret_schema,
                ArrowSchema::new_struct(
                    "state",
                    vec![
                        ArrowSchema::from_owned(s_sch),
                        ArrowSchema::from_owned(c_sch),
                    ],
                ),
            );
        }
        0
    }

    unsafe extern "C" fn mock_finalize(
        _ctx: *const c_void,
        state_array: *const ArrowArray,
        state_schema: *const ArrowSchema,
        ret_array: *mut ArrowArray,
        ret_schema: *mut ArrowSchema,
        _errmsg: *mut *mut c_char,
    ) -> c_int {
        let mut array = unsafe { std::ptr::read(state_array) };
        let mut schema = unsafe { std::ptr::read(state_schema) };

        let mut child_arrays = unsafe { array.take_struct_children() };
        let mut child_schemas = unsafe { schema.take_struct_children() };

        let _count_arr = child_arrays.pop().unwrap();
        let _count_sch = child_schemas.pop().unwrap();
        let sum_arr = child_arrays.pop().unwrap();
        let sum_sch = child_schemas.pop().unwrap();

        let sums = unsafe { ffi_child_to_int32_array(sum_arr, sum_sch) };
        let sum_val = sums.value(0);

        let result: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![sum_val]));
        let (ffi_arr, ffi_sch) = arrow::ffi::to_ffi(&result.to_data()).unwrap();

        unsafe {
            std::ptr::write(ret_array, ArrowArray::from_owned(ffi_arr));
            std::ptr::write(ret_schema, ArrowSchema::from_owned(ffi_sch));
        }
        0
    }

    unsafe extern "C" fn mock_fini(ctx: *mut c_void) {
        if !ctx.is_null() {
            unsafe { drop(Box::from_raw(ctx.cast::<SumCtx>())) };
        }
    }

    unsafe extern "C" fn mock_free_string(s: *mut c_char) {
        if !s.is_null() {
            unsafe { drop(CString::from_raw(s)) };
        }
    }

    unsafe extern "C" fn mock_init(_session: *mut daft_ext::abi::FFI_SessionContext) -> c_int {
        0
    }

    fn make_mock_module_handle() -> Arc<ModuleHandle> {
        use daft_ext::abi::FFI_Module;
        let module = FFI_Module {
            daft_abi_version: daft_ext::abi::DAFT_ABI_VERSION,
            name: c"mock_agg_module".as_ptr(),
            init: mock_init,
            free_string: mock_free_string,
        };
        Arc::new(ModuleHandle::new(module))
    }

    fn make_mock_agg_handle() -> (FFI_AggregateFunction, Arc<ModuleHandle>) {
        let ctx = Box::into_raw(Box::new(SumCtx));
        let handle = FFI_AggregateFunction {
            ctx: ctx.cast(),
            name: mock_name,
            get_return_field: mock_get_return_field,
            get_state_schema: mock_get_state_schema,
            aggregate: mock_aggregate,
            combine: mock_combine,
            finalize: mock_finalize,
            fini: mock_fini,
        };
        (handle, make_mock_module_handle())
    }

    #[test]
    fn test_name() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);
        assert_eq!(AggFn::name(&handle), "test_sum");
    }

    #[test]
    fn test_return_dtype() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);
        let dt = handle.return_dtype(&[DataType::Int32]).unwrap();
        assert_eq!(dt, DataType::Int32);
    }

    #[test]
    fn test_state_fields() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);
        let fields = handle.state_fields(&[]).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name.as_ref(), "sum");
        assert_eq!(fields[1].name.as_ref(), "count");
    }

    #[test]
    fn test_call_agg_block() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let series = Series::from_arrow(Field::new("x", DataType::Int32), arrow_arr).unwrap();

        let states = handle.call_agg_block(vec![series]).unwrap();
        assert_eq!(states.len(), 2);
        // sum = 6
        assert_eq!(states[0], daft_core::prelude::Literal::Int32(6));
        // count = 3
        assert_eq!(states[1], daft_core::prelude::Literal::Int32(3));
    }

    #[test]
    fn test_call_agg_combine() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);

        // Build state Series: sums=[6,15], counts=[3,3]
        let sums: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![6, 15]));
        let counts: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![3, 3]));
        let sum_series = Series::from_arrow(Field::new("sum", DataType::Int32), sums).unwrap();
        let count_series =
            Series::from_arrow(Field::new("count", DataType::Int32), counts).unwrap();

        let states = handle
            .call_agg_combine(vec![sum_series, count_series])
            .unwrap();
        assert_eq!(states.len(), 2);
        // total sum = 21
        assert_eq!(states[0], daft_core::prelude::Literal::Int32(21));
        // total count = 6
        assert_eq!(states[1], daft_core::prelude::Literal::Int32(6));
    }

    #[test]
    fn test_call_agg_finalize() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);

        let state = vec![
            daft_core::prelude::Literal::Int32(42),
            daft_core::prelude::Literal::Int32(5),
        ];

        let result = handle.call_agg_finalize(state).unwrap();
        // finalize returns the sum value
        assert_eq!(result, daft_core::prelude::Literal::Int32(42));
    }

    #[test]
    fn test_serde_roundtrip() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = AggregateFunctionHandle::new(ffi, module);

        let json = serde_json::to_string(&handle).unwrap();
        assert!(json.contains("test_sum"));

        let deser: AggregateFunctionHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(AggFn::name(&deser), "test_sum");
        assert!(deser.inner.is_none());

        // Calling on a deserialized (unloaded) handle should error
        let err = deser.return_dtype(&[DataType::Int32]).unwrap_err();
        assert!(
            err.to_string().contains("not loaded"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_into_aggregate_fn_handle() {
        let (ffi, module) = make_mock_agg_handle();
        let handle = into_aggregate_fn_handle(ffi, module);
        assert_eq!(handle.name(), "test_sum");
    }
}
