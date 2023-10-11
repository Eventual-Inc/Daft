use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;
use daft_core::datatypes::logical::{DateArray, Decimal128Array, TimestampArray};
use daft_core::datatypes::{
    BinaryArray, BooleanArray, DaftNumericType, DaftPhysicalType, DataArray, Int128Array,
    Int32Array, Int64Array, Utf8Array,
};
use daft_core::schema::{Schema, SchemaRef};
use daft_core::{IntoSeries, Series};
use daft_dsl::Expr;
use daft_parquet::read::read_parquet_metadata;
use daft_table::Table;
use indexmap::IndexMap;
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};
use snafu::ResultExt;

use crate::column_stats::ColumnRangeStatistics;
use crate::DaftCoreComputeSnafu;
use crate::{column_stats::TruthValue, table_stats::TableStatistics};
use daft_io::IOConfig;

struct DeferredLoadingParams {
    filters: Vec<Expr>,
}

enum TableState {
    Unloaded(DeferredLoadingParams),
    Loaded(Vec<Table>),
}

struct MicroPartition {
    schema: SchemaRef,
    state: Mutex<TableState>,
    statistics: Option<TableStatistics>,
}

impl MicroPartition {
    pub fn new(schema: SchemaRef, state: TableState, statistics: Option<TableStatistics>) -> Self {
        MicroPartition {
            schema,
            state: Mutex::new(state),
            statistics: statistics,
        }
    }

    pub fn empty() -> Self {
        Self::new(Schema::empty().into(), TableState::Loaded(vec![]), None)
    }

    fn tables_or_read(&self) -> &[&Table] {
        todo!("to do me")
    }

    pub fn filter(&self, predicate: &[Expr]) -> super::Result<Self> {
        if predicate.is_empty() {
            return Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(vec![]),
                None,
            ));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = predicate
                .iter()
                .cloned()
                .reduce(|a, b| a.and(&b))
                .expect("should have at least 1 expr");
            let eval_result = statistics.eval_expression(&folded_expr)?;
            let tv = eval_result.to_truth_value();

            if matches!(tv, TruthValue::False) {
                return Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(vec![]),
                    None,
                ));
            }
        }

        let guard = self.state.lock().unwrap();
        let new_state = match guard.deref() {
            TableState::Unloaded(params) => {
                let mut new_filters = params.filters.clone();
                new_filters.extend(predicate.iter().cloned());
                TableState::Unloaded(DeferredLoadingParams {
                    filters: new_filters,
                })
            }
            TableState::Loaded(tables) => TableState::Loaded(
                tables
                    .iter()
                    .map(|t| t.filter(predicate))
                    .collect::<DaftResult<Vec<_>>>()
                    .context(DaftCoreComputeSnafu)?,
            ),
        };

        // TODO: We should also "filter" the TableStatistics so it's more accurate for downstream tasks
        Ok(Self::new(
            self.schema.clone(),
            new_state,
            self.statistics.clone(),
        ))
    }
}

impl From<(&BooleanStatistics)> for ColumnRangeStatistics {
    fn from(value: &BooleanStatistics) -> Self {
        let lower = value.min_value.unwrap();
        let upper = value.max_value.unwrap();

        ColumnRangeStatistics {
            lower: BooleanArray::from(("lower", [lower].as_slice())).into_series(),
            upper: BooleanArray::from(("upper", [upper].as_slice())).into_series(),
        }
    }
}

impl<T: parquet2::types::NativeType + daft_core::datatypes::NumericNative>
    From<(&PrimitiveStatistics<T>)> for ColumnRangeStatistics
{
    fn from(value: &PrimitiveStatistics<T>) -> Self {
        // TODO: dont unwrap
        let lower = value.min_value.unwrap();
        let upper = value.max_value.unwrap();

        let prim_type = &value.primitive_type;
        let ptype = prim_type.physical_type;

        if let Some(ltype) = prim_type.logical_type {
            /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
            use parquet2::schema::types::PhysicalType;
            use parquet2::schema::types::PrimitiveLogicalType;

            match (ptype, ltype) {
                (PhysicalType::Int32, PrimitiveLogicalType::Date) => {
                    let lower = Int32Array::from(("lower", [lower.to_i32().unwrap()].as_slice()));
                    let upper = Int32Array::from(("upper", [upper.to_i32().unwrap()].as_slice()));

                    let dtype = daft_core::datatypes::DataType::Date;

                    let lower = DateArray::new(
                        daft_core::datatypes::Field::new("lower", dtype.clone()),
                        lower,
                    )
                    .into_series();
                    let upper =
                        DateArray::new(daft_core::datatypes::Field::new("upper", dtype), upper)
                            .into_series();
                    return ColumnRangeStatistics { lower, upper };
                }
                (
                    PhysicalType::Int64,
                    PrimitiveLogicalType::Timestamp {
                        unit,
                        is_adjusted_to_utc,
                    },
                ) => {
                    let lower = Int64Array::from(("lower", [lower.to_i64().unwrap()].as_slice()));
                    let upper = Int64Array::from(("upper", [upper.to_i64().unwrap()].as_slice()));
                    let tu = match unit {
                        parquet2::schema::types::TimeUnit::Nanoseconds => {
                            daft_core::datatypes::TimeUnit::Nanoseconds
                        }
                        parquet2::schema::types::TimeUnit::Microseconds => {
                            daft_core::datatypes::TimeUnit::Microseconds
                        }
                        parquet2::schema::types::TimeUnit::Milliseconds => {
                            daft_core::datatypes::TimeUnit::Milliseconds
                        }
                    };
                    let tz = if is_adjusted_to_utc {
                        Some("+00:00".to_string())
                    } else {
                        None
                    };

                    let dtype = daft_core::datatypes::DataType::Timestamp(tu, tz);

                    let lower = TimestampArray::new(
                        daft_core::datatypes::Field::new("lower", dtype.clone()),
                        lower,
                    )
                    .into_series();
                    let upper = TimestampArray::new(
                        daft_core::datatypes::Field::new("upper", dtype),
                        upper,
                    )
                    .into_series();
                    return ColumnRangeStatistics { lower, upper };
                }

                _ => {}
            }
        }

        // TODO: FIX THESE STATS
        let lower = Series::try_from((
            "lower",
            Box::new(PrimitiveArray::<T>::from_vec(vec![lower])) as Box<dyn arrow2::array::Array>,
        ))
        .unwrap();
        let upper = Series::try_from((
            "upper",
            Box::new(PrimitiveArray::<T>::from_vec(vec![upper])) as Box<dyn arrow2::array::Array>,
        ))
        .unwrap();

        ColumnRangeStatistics { lower, upper }
    }
}

impl From<(&BinaryStatistics)> for ColumnRangeStatistics {
    fn from(value: &BinaryStatistics) -> Self {
        let lower = value.min_value.as_ref().unwrap();
        let upper = value.max_value.as_ref().unwrap();
        let ptype = &value.primitive_type;

        if let Some(ltype) = ptype.logical_type {
            use parquet2::schema::types::PrimitiveLogicalType;
            match ltype {
                PrimitiveLogicalType::String
                | PrimitiveLogicalType::Enum
                | PrimitiveLogicalType::Uuid
                | PrimitiveLogicalType::Json => {
                    let lower = String::from_utf8(lower.clone()).unwrap();
                    let upper = String::from_utf8(upper.clone()).unwrap();

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return ColumnRangeStatistics { lower, upper };
                }
                PrimitiveLogicalType::Decimal(p, s) => {
                    assert!(lower.len() <= 16);
                    assert!(upper.len() <= 16);
                    let l = crate::utils::deserialize::convert_i128(lower.as_slice(), lower.len());
                    let u = crate::utils::deserialize::convert_i128(upper.as_slice(), upper.len());
                    let lower = Int128Array::from(("lower", [l].as_slice()));
                    let upper = Int128Array::from(("upper", [u].as_slice()));
                    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);

                    let lower = Decimal128Array::new(
                        daft_core::datatypes::Field::new("lower", daft_type.clone()),
                        lower,
                    )
                    .into_series();
                    let upper = Decimal128Array::new(
                        daft_core::datatypes::Field::new("upper", daft_type),
                        upper,
                    )
                    .into_series();

                    return ColumnRangeStatistics { lower, upper };
                }
                _ => todo!("HANDLE BAD LOGICAL TYPE"),
            }
        } else if let Some(ctype) = ptype.converted_type {
            use parquet2::schema::types::PrimitiveConvertedType;
            match ctype {
                PrimitiveConvertedType::Utf8
                | PrimitiveConvertedType::Enum
                | PrimitiveConvertedType::Json => {
                    let lower = String::from_utf8(lower.clone()).unwrap();
                    let upper = String::from_utf8(upper.clone()).unwrap();

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return ColumnRangeStatistics { lower, upper };
                }
                PrimitiveConvertedType::Decimal(p, s) => {
                    assert!(lower.len() <= 16);
                    assert!(upper.len() <= 16);
                    let l = crate::utils::deserialize::convert_i128(lower.as_slice(), lower.len());
                    let u = crate::utils::deserialize::convert_i128(upper.as_slice(), upper.len());
                    let lower = Int128Array::from(("lower", [l].as_slice()));
                    let upper = Int128Array::from(("upper", [u].as_slice()));
                    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);

                    let lower = Decimal128Array::new(
                        daft_core::datatypes::Field::new("lower", daft_type.clone()),
                        lower,
                    )
                    .into_series();
                    let upper = Decimal128Array::new(
                        daft_core::datatypes::Field::new("upper", daft_type),
                        upper,
                    )
                    .into_series();
                    return ColumnRangeStatistics { lower, upper };
                }
                _ => todo!("HANDLE BAD CONVERTED TYPE"),
            }
        }

        let lower = BinaryArray::from(("lower", lower.as_slice())).into_series();
        let upper = BinaryArray::from(("upper", upper.as_slice())).into_series();

        return ColumnRangeStatistics { lower, upper };
    }
}

impl From<&dyn Statistics> for ColumnRangeStatistics {
    fn from(value: &dyn Statistics) -> Self {
        let ptype = value.physical_type();
        let stats = value.as_any();
        use parquet2::schema::types::PhysicalType;
        match ptype {
            PhysicalType::Boolean => stats.downcast_ref::<BooleanStatistics>().unwrap().into(),
            PhysicalType::Int32 => stats
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .into(),
            PhysicalType::Int64 => stats
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .into(),
            PhysicalType::Int96 => todo!(),
            PhysicalType::Float => stats
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .into(),
            PhysicalType::Double => stats
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .into(),
            PhysicalType::ByteArray => stats.downcast_ref::<BinaryStatistics>().unwrap().into(),
            PhysicalType::FixedLenByteArray(size) => {
                todo!()
            }
        }
    }
}

impl From<&daft_parquet::metadata::RowGroupMetaData> for TableStatistics {
    fn from(value: &daft_parquet::metadata::RowGroupMetaData) -> Self {
        let num_rows = value.num_rows();
        let mut columns = IndexMap::new();
        for col in value.columns() {
            let stats = col.statistics().unwrap().unwrap();
            let col_stats: ColumnRangeStatistics = stats.as_ref().into();
            columns.insert(
                col.descriptor().path_in_schema.get(0).unwrap().clone(),
                col_stats,
            );
        }

        TableStatistics { columns }
    }
}

fn read_parquet(uri: &str, io_config: Arc<IOConfig>) -> DaftResult<()> {
    let runtime_handle = daft_io::get_runtime(true)?;
    let io_client = daft_io::get_io_client(true, io_config)?;
    let metadata =
        runtime_handle.block_on(async move { read_parquet_metadata(uri, io_client).await })?;

    for rg in &metadata.row_groups {
        let table_stats: TableStatistics = rg.into();
        println!("{table_stats:?}");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries, Series,
    };
    use daft_dsl::{col, lit};
    use daft_io::IOConfig;
    use daft_table::Table;

    use crate::column_stats::TruthValue;

    use super::{ColumnRangeStatistics, TableStatistics};

    #[test]
    fn test_pq() -> crate::Result<()> {
        let url = "/Users/sammy/daft_200MB_lineitem_chunk.RG-2.parquet";
        super::read_parquet(&url, IOConfig::default().into());

        Ok(())
    }
}
