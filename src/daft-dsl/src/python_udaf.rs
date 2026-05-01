use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Series};
use serde::{Deserialize, Serialize};

use crate::{
    AggExpr, Expr, ExprRef,
    functions::{
        AggFnHandle,
        agg_fn::{AggFn, State},
        python::RuntimePyObject,
    },
};

#[derive(Serialize, Deserialize)]
pub struct PyAggFn {
    func_id: String,
    func_name: String,
    cls_factory: RuntimePyObject,
    init_args: RuntimePyObject,
    original_args: RuntimePyObject,
    return_dtype: DataType,
    state_field_names: Vec<String>,
    state_field_dtypes: Vec<DataType>,
}

#[typetag::serde(name = "PyAggFn")]
impl AggFn for PyAggFn {
    fn name(&self) -> &str {
        &self.func_id
    }

    fn return_dtype(&self, _input_types: &[DataType]) -> DaftResult<DataType> {
        Ok(self.return_dtype.clone())
    }

    fn state_fields(&self, _inputs: &[Field]) -> DaftResult<Vec<Field>> {
        Ok(self
            .state_field_names
            .iter()
            .zip(self.state_field_dtypes.iter())
            .map(|(name, dtype)| Field::new(name.as_str(), dtype.clone()))
            .collect())
    }

    fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        self.call_agg_block_impl(inputs)
    }

    fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
        self.call_agg_combine_impl(states)
    }

    fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State> {
        self.call_agg_finalize_impl(state)
    }
}

#[cfg(feature = "python")]
impl PyAggFn {
    fn call_agg_block_impl(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.agg_execution"))?
                .getattr(pyo3::intern!(py, "call_agg_block"))?;

            let py_inputs: Vec<PySeries> = inputs.into_iter().map(PySeries::from).collect();

            let state_field_dtypes_py: Vec<daft_core::python::PyDataType> = self
                .state_field_dtypes
                .iter()
                .map(|dt| daft_core::python::PyDataType { dtype: dt.clone() })
                .collect();

            let result = func.call1((
                self.cls_factory.as_ref(),
                self.init_args.as_ref(),
                self.original_args.as_ref(),
                py_inputs,
                self.state_field_names.clone(),
                state_field_dtypes_py,
            ))?;

            let result_pyseries: Vec<PySeries> = result.extract()?;
            result_pyseries
                .into_iter()
                .map(|ps| Ok(ps.series.get_lit(0)))
                .collect::<DaftResult<Vec<_>>>()
        })
    }

    fn call_agg_combine_impl(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.agg_execution"))?
                .getattr(pyo3::intern!(py, "call_agg_combine"))?;

            let py_states: Vec<PySeries> = states.into_iter().map(PySeries::from).collect();

            let state_field_dtypes_py: Vec<daft_core::python::PyDataType> = self
                .state_field_dtypes
                .iter()
                .map(|dt| daft_core::python::PyDataType { dtype: dt.clone() })
                .collect();

            let result = func.call1((
                self.cls_factory.as_ref(),
                self.init_args.as_ref(),
                py_states,
                self.state_field_names.clone(),
                state_field_dtypes_py,
            ))?;

            let result_pyseries: Vec<PySeries> = result.extract()?;
            result_pyseries
                .into_iter()
                .map(|ps| Ok(ps.series.get_lit(0)))
                .collect::<DaftResult<Vec<_>>>()
        })
    }

    fn call_agg_finalize_impl(&self, state: Vec<State>) -> DaftResult<State> {
        use daft_core::python::PySeries;
        use pyo3::prelude::*;

        Python::attach(|py| {
            let func = py
                .import(pyo3::intern!(py, "daft.udf.agg_execution"))?
                .getattr(pyo3::intern!(py, "call_agg_finalize"))?;

            let py_state_values: Vec<Bound<PyAny>> = state
                .into_iter()
                .map(|lit| lit.into_pyobject(py))
                .collect::<Result<Vec<_>, _>>()?;

            let return_dtype_py = daft_core::python::PyDataType {
                dtype: self.return_dtype.clone(),
            };

            let result = func.call1((
                self.cls_factory.as_ref(),
                self.init_args.as_ref(),
                py_state_values,
                self.state_field_names.clone(),
                return_dtype_py,
            ))?;

            let result_pyseries: PySeries = result.extract()?;
            Ok(result_pyseries.series.get_lit(0))
        })
    }
}

#[cfg(not(feature = "python"))]
impl PyAggFn {
    fn call_agg_block_impl(&self, _inputs: Vec<Series>) -> DaftResult<Vec<State>> {
        panic!("Cannot evaluate PyAggFn without Python feature");
    }

    fn call_agg_combine_impl(&self, _states: Vec<Series>) -> DaftResult<Vec<State>> {
        panic!("Cannot evaluate PyAggFn without Python feature");
    }

    fn call_agg_finalize_impl(&self, _state: Vec<State>) -> DaftResult<State> {
        panic!("Cannot evaluate PyAggFn without Python feature");
    }
}

#[allow(clippy::too_many_arguments)]
pub fn py_udaf(
    func_id: &str,
    func_name: &str,
    cls_factory: RuntimePyObject,
    init_args: RuntimePyObject,
    original_args: RuntimePyObject,
    return_dtype: DataType,
    state_field_names: Vec<String>,
    state_field_dtypes: Vec<DataType>,
    inputs: Vec<ExprRef>,
) -> ExprRef {
    let handle = AggFnHandle::new(Arc::new(PyAggFn {
        func_id: func_id.to_string(),
        func_name: func_name.to_string(),
        cls_factory,
        init_args,
        original_args,
        return_dtype,
        state_field_names,
        state_field_dtypes,
    }));
    Arc::new(Expr::Agg(AggExpr::AggFn { handle, inputs }))
}
