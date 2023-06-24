use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::ffi;
use crate::Table;
use common_error::DaftError;
use daft_core::datatypes::Field;
use daft_core::schema::Schema;
use daft_core::series::Series;

use daft_dsl::python::PyExpr;

use daft_core::python::schema::PySchema;
use daft_core::python::series::PySeries;

#[pyclass]
#[derive(Clone)]
pub struct PyTable {
    pub table: Table,
}

#[pymethods]
impl PyTable {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: self.table.schema.clone(),
        })
    }

    pub fn cast_to_schema(&self, schema: &PySchema) -> PyResult<Self> {
        Ok(self.table.cast_to_schema(&schema.schema)?.into())
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .eval_expression_list(converted_exprs.as_slice())?
                .into())
        })
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.table.take(&idx.series)?.into()))
    }

    pub fn filter(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| Ok(self.table.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .sort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn argsort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<PySeries> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .argsort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn agg(&self, py: Python, to_agg: Vec<PyExpr>, group_by: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_agg: Vec<daft_dsl::Expr> = to_agg.into_iter().map(|e| e.into()).collect();
        let converted_group_by: Vec<daft_dsl::Expr> =
            group_by.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .agg(converted_to_agg.as_slice(), converted_group_by.as_slice())?
                .into())
        })
    }

    pub fn join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
    ) -> PyResult<Self> {
        let left_exprs: Vec<daft_dsl::Expr> = left_on.into_iter().map(|e| e.into()).collect();
        let right_exprs: Vec<daft_dsl::Expr> = right_on.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .join(&right.table, left_exprs.as_slice(), right_exprs.as_slice())?
                .into())
        })
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_explode: Vec<daft_dsl::Expr> =
            to_explode.into_iter().map(|e| e.expr).collect();

        py.allow_threads(|| Ok(self.table.explode(converted_to_explode.as_slice())?.into()))
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.table))
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(self.table.repr_html())
    }

    pub fn head(&self, py: Python, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not head table with negative number: {num}"
            )));
        }
        let num = num as usize;
        py.allow_threads(|| Ok(self.table.head(num)?.into()))
    }

    pub fn sample(&self, py: Python, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not sample table with negative number: {num}"
            )));
        }
        let num = num as usize;
        py.allow_threads(|| Ok(self.table.sample(num)?.into()))
    }

    pub fn quantiles(&self, py: Python, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not fetch quantile from table with negative number: {num}"
            )));
        }
        let num = num as usize;
        py.allow_threads(|| Ok(self.table.quantiles(num)?.into()))
    }

    pub fn partition_by_hash(
        &self,
        py: Python,
        exprs: Vec<PyExpr>,
        num_partitions: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }
        let exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .partition_by_hash(exprs.as_slice(), num_partitions as usize)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<PyTable>>())
        })
    }

    pub fn partition_by_random(
        &self,
        py: Python,
        num_partitions: i64,
        seed: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }

        if seed < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not have seed has negative number: {seed}"
            )));
        }
        py.allow_threads(|| {
            Ok(self
                .table
                .partition_by_random(num_partitions as usize, seed as u64)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<PyTable>>())
        })
    }

    pub fn partition_by_range(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
        boundaries: &Self,
        descending: Vec<bool>,
    ) -> PyResult<Vec<Self>> {
        let exprs: Vec<daft_dsl::Expr> = partition_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .table
                .partition_by_range(exprs.as_slice(), &boundaries.table, descending.as_slice())?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<PyTable>>())
        })
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.table.len())
    }

    pub fn size_bytes(&self) -> PyResult<usize> {
        Ok(self.table.size_bytes()?)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.table.column_names()
    }

    pub fn get_column(&self, name: &str) -> PyResult<PySeries> {
        Ok(self.table.get_column(name)?.clone().into())
    }

    pub fn get_column_by_index(&self, idx: i64) -> PyResult<PySeries> {
        if idx < 0 {
            return Err(PyValueError::new_err(format!(
                "Invalid index, negative numbers not supported: {idx}"
            )));
        }
        let idx = idx as usize;
        if idx >= self.table.len() {
            return Err(PyValueError::new_err(format!(
                "Invalid index, out of bounds: {idx} out of {}",
                self.table.len()
            )));
        }

        Ok(self.table.get_column_by_index(idx)?.clone().into())
    }

    #[staticmethod]
    pub fn concat(py: Python, tables: Vec<Self>) -> PyResult<Self> {
        let tables: Vec<_> = tables.iter().map(|t| &t.table).collect();
        py.allow_threads(|| Ok(Table::concat(tables.as_slice())?.into()))
    }

    pub fn slice(&self, start: i64, end: i64) -> PyResult<Self> {
        if start < 0 {
            return Err(PyValueError::new_err(format!(
                "slice start can not be negative: {start}"
            )));
        }
        if end < 0 {
            return Err(PyValueError::new_err(format!(
                "slice end can not be negative: {start}"
            )));
        }
        if start > end {
            return Err(PyValueError::new_err(format!(
                "slice length can not be negative: start: {start} end: {end}"
            )));
        }
        Ok(self.table.slice(start as usize, end as usize)?.into())
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(
        py: Python,
        record_batches: Vec<&PyAny>,
        schema: &PySchema,
    ) -> PyResult<Self> {
        let table =
            ffi::record_batches_to_table(py, record_batches.as_slice(), schema.schema.clone())?;
        Ok(PyTable { table })
    }

    #[staticmethod]
    pub fn from_pylist_series(dict: &PyDict) -> PyResult<Self> {
        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<Series> = Vec::new();
        fields.reserve(dict.len());
        columns.reserve(dict.len());

        for (k, v) in dict.iter() {
            let name = k.extract::<String>()?;
            let series = v.extract::<PySeries>()?.series;
            fields.push(Field::new(name.clone(), series.data_type().clone()));
            columns.push(series.rename(name));
        }
        if !columns.is_empty() {
            let first = columns.first().unwrap();
            for s in columns.iter().skip(1) {
                if s.len() != first.len() {
                    return Err(DaftError::ValueError(format!(
                        "Mismatch in Series lengths when making a Table, {} vs {}",
                        s.len(),
                        first.len()
                    ))
                    .into());
                }
            }
        }

        Ok(PyTable {
            table: Table::new(Schema::new(fields)?, columns)?,
        })
    }

    pub fn to_arrow_record_batch(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pyarrow = py.import("pyarrow")?;
            ffi::table_to_record_batch(&self.table, py, pyarrow)
        })
    }

    #[staticmethod]
    pub fn empty(schema: Option<PySchema>) -> PyResult<Self> {
        Ok(Table::empty(match schema {
            Some(s) => Some(s.schema),
            None => None,
        })?
        .into())
    }
}

impl From<Table> for PyTable {
    fn from(value: Table) -> Self {
        PyTable { table: value }
    }
}

impl From<PyTable> for Table {
    fn from(item: PyTable) -> Self {
        item.table
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyTable>()?;
    Ok(())
}
