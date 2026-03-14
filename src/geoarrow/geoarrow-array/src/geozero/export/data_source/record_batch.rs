use std::{str::FromStr, sync::Arc};

use arrow_array::{Array, RecordBatch, cast::AsArray, timezone::Tz, types::*};
use arrow_json::writer::make_encoder;
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use geoarrow_schema::GeoArrowType;
use geozero::{
    ColumnValue, FeatureProcessor, GeomProcessor, PropertyProcessor, error::GeozeroError,
};

use crate::{
    GeoArrowArray,
    array::from_arrow_array,
    builder::geo_trait_wrappers::RectWrapper,
    cast::AsGeoArrowArray,
    geozero::export::scalar::{
        process_geometry, process_geometry_collection, process_line_string,
        process_multi_line_string, process_multi_point, process_multi_polygon, process_point,
        process_polygon,
    },
    trait_::GeoArrowArrayAccessor,
};

/// A push-based writer for creating geozero-based outputs.
pub struct GeozeroRecordBatchWriter<P: FeatureProcessor> {
    schema: SchemaRef,
    overall_row_idx: usize,
    geometry_column_index: usize,
    processor: P,
}

impl<P: FeatureProcessor> GeozeroRecordBatchWriter<P> {
    /// Create a new GeozeroRecordBatchWriter from a schema
    pub fn try_new(
        schema: SchemaRef,
        mut processor: P,
        name: Option<&str>,
    ) -> Result<Self, GeozeroError> {
        let geom_indices = geometry_columns(&schema);
        let geometry_column_index = if geom_indices.len() != 1 {
            Err(GeozeroError::Dataset(
                "Writing through geozero not supported with multiple geometries".to_string(),
            ))?
        } else {
            geom_indices[0]
        };

        processor.dataset_begin(name)?;

        Ok(Self {
            schema,
            geometry_column_index,
            overall_row_idx: 0,
            processor,
        })
    }

    /// Write a [`RecordBatch`], processing it with the given [`FeatureProcessor`].
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), GeozeroError> {
        if *batch.schema_ref() != self.schema {
            return Err(GeozeroError::Dataset(
                "Batch schema does not match writer schema".to_string(),
            ));
        }

        let num_rows = batch.num_rows();
        process_batch(
            batch,
            batch.schema_ref(),
            self.geometry_column_index,
            self.overall_row_idx,
            &mut self.processor,
        )?;
        self.overall_row_idx += num_rows;

        Ok(())
    }

    /// Finish the dataset processing and return the processor.
    pub fn finish(mut self) -> Result<P, GeozeroError> {
        self.processor.dataset_end()?;
        Ok(self.processor)
    }
}

pub(super) fn process_batch<P: FeatureProcessor>(
    batch: &RecordBatch,
    schema: &Schema,
    geometry_column_index: usize,
    batch_start_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let num_rows = batch.num_rows();
    let geometry_field = schema.field(geometry_column_index);
    let geometry_column_box = &batch.columns()[geometry_column_index];
    let geometry_column = from_arrow_array(&geometry_column_box, geometry_field)
        .map_err(|err| GeozeroError::Dataset(err.to_string()))?;

    for within_batch_row_idx in 0..num_rows {
        processor.feature_begin((within_batch_row_idx + batch_start_idx) as u64)?;

        processor.properties_begin()?;
        process_properties(
            batch,
            schema,
            within_batch_row_idx,
            geometry_column_index,
            processor,
        )?;
        processor.properties_end()?;

        processor.geometry_begin()?;
        process_geometry_n(&geometry_column, within_batch_row_idx, processor)?;
        processor.geometry_end()?;

        processor.feature_end((within_batch_row_idx + batch_start_idx) as u64)?;
    }

    Ok(())
}

fn process_properties<P: PropertyProcessor>(
    batch: &RecordBatch,
    schema: &Schema,
    within_batch_row_idx: usize,
    geometry_column_index: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    // Note: the `column_idx` will be off by one if the geometry column is not the last column in
    // the table, so we maintain a separate property index counter
    let mut property_idx = 0;
    for (column_idx, (field, array)) in schema.fields.iter().zip(batch.columns().iter()).enumerate()
    {
        // Don't include geometry column in properties
        if column_idx == geometry_column_index {
            continue;
        }
        let name = field.name();

        // Don't pass null properties to geozero
        if array.is_null(within_batch_row_idx) {
            continue;
        }

        match field.data_type() {
            DataType::Boolean => {
                let arr = array.as_boolean();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Bool(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt8 => {
                let arr = array.as_primitive::<UInt8Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UByte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int8 => {
                let arr = array.as_primitive::<Int8Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Byte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt16 => {
                let arr = array.as_primitive::<UInt16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UShort(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int16 => {
                let arr = array.as_primitive::<Int16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Short(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt32 => {
                let arr = array.as_primitive::<UInt32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UInt(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int32 => {
                let arr = array.as_primitive::<Int32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Int(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt64 => {
                let arr = array.as_primitive::<UInt64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::ULong(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int64 => {
                let arr = array.as_primitive::<Int64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Long(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float16 => {
                let arr = array.as_primitive::<Float16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx).to_f32()),
                )?;
            }
            DataType::Float32 => {
                let arr = array.as_primitive::<Float32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float64 => {
                let arr = array.as_primitive::<Float64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Double(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Utf8 => {
                let arr = array.as_string::<i32>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Binary => {
                let arr = array.as_binary::<i32>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeBinary => {
                let arr = array.as_binary::<i64>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _) => {
                // TODO(Perf): refactor so that we don't make a new encoder on every row
                let options = Default::default();
                let mut enc = make_encoder(field, array, &options)
                    .map_err(|err| GeozeroError::Property(err.to_string()))?;
                let mut out = vec![];
                enc.encode(within_batch_row_idx, &mut out);
                let json_string = String::from_utf8(out)
                    .map_err(|err| GeozeroError::Property(err.to_string()))?;
                processor.property(property_idx, name, &ColumnValue::Json(&json_string))?;
            }
            DataType::Date32 => {
                let arr = array.as_primitive::<Date32Type>();
                let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                let dt_str = datetime.and_utc().to_rfc3339();
                processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
            }
            DataType::Date64 => {
                let arr = array.as_primitive::<Date64Type>();
                let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                let dt_str = datetime.and_utc().to_rfc3339();
                processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
            }
            DataType::Timestamp(unit, tz) => {
                let arrow_tz = if let Some(tz) = tz {
                    Some(Tz::from_str(tz).map_err(|err| GeozeroError::Property(err.to_string()))?)
                } else {
                    None
                };

                macro_rules! impl_timestamp {
                    ($arrow_type:ty) => {{
                        let arr = array.as_primitive::<$arrow_type>();
                        let dt_str = if let Some(arrow_tz) = arrow_tz {
                            arr.value_as_datetime_with_tz(within_batch_row_idx, arrow_tz)
                                .unwrap()
                                .to_rfc3339()
                        } else {
                            arr.value_as_datetime(within_batch_row_idx)
                                .unwrap()
                                .and_utc()
                                .to_rfc3339()
                        };
                        processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
                    }};
                }

                match unit {
                    TimeUnit::Microsecond => impl_timestamp!(TimestampMicrosecondType),
                    TimeUnit::Millisecond => impl_timestamp!(TimestampMillisecondType),
                    TimeUnit::Nanosecond => impl_timestamp!(TimestampNanosecondType),
                    TimeUnit::Second => impl_timestamp!(TimestampSecondType),
                }
            }
            dt => {
                return Err(GeozeroError::Properties(format!(
                    "unsupported type: {dt:?}",
                )));
            }
        }
        property_idx += 1;
    }

    Ok(())
}

fn process_geometry_n<P: GeomProcessor>(
    geometry_column: &Arc<dyn GeoArrowArray>,
    within_batch_row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let arr = geometry_column.as_ref();
    let i = within_batch_row_idx;

    use GeoArrowType::*;
    // TODO: should we be passing the geom_idx down into these process* functions?
    match arr.data_type() {
        Point(_) => {
            let geom = arr.as_point().value(i).unwrap();
            process_point(&geom, 0, processor)?;
        }
        LineString(_) => {
            let geom = arr.as_line_string().value(i).unwrap();
            process_line_string(&geom, 0, processor)?;
        }
        Polygon(_) => {
            let geom = arr.as_polygon().value(i).unwrap();
            process_polygon(&geom, true, 0, processor)?;
        }
        MultiPoint(_) => {
            let geom = arr.as_multi_point().value(i).unwrap();
            process_multi_point(&geom, 0, processor)?;
        }
        MultiLineString(_) => {
            let geom = arr.as_multi_line_string().value(i).unwrap();
            process_multi_line_string(&geom, 0, processor)?;
        }
        MultiPolygon(_) => {
            let geom = arr.as_multi_polygon().value(i).unwrap();
            process_multi_polygon(&geom, 0, processor)?;
        }
        GeometryCollection(_) => {
            let geom = arr.as_geometry_collection().value(i).unwrap();
            process_geometry_collection(&geom, 0, processor)?;
        }
        Wkb(_) => {
            let geom = arr
                .as_wkb::<i32>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        LargeWkb(_) => {
            let geom = arr
                .as_wkb::<i64>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        WkbView(_) => {
            let geom = arr
                .as_wkb_view()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        Wkt(_) => {
            let geom = arr
                .as_wkt::<i32>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        LargeWkt(_) => {
            let geom = arr
                .as_wkt::<i64>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        WktView(_) => {
            let geom = arr
                .as_wkt_view()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        Rect(_) => {
            let geom = arr.as_rect().value(i).unwrap();
            let wrapper = RectWrapper::try_new(&geom)
                .map_err(|err| geozero::error::GeozeroError::Geometry(err.to_string()))?;
            process_polygon(&wrapper, true, 0, processor)?
        }
        Geometry(_) => {
            let geom = arr.as_geometry().value(i).unwrap();
            process_geometry(&geom, 0, processor)?;
        }
    }

    Ok(())
}

pub(super) fn geometry_columns(schema: &Schema) -> Vec<usize> {
    let mut geom_indices = vec![];
    for (field_idx, field) in schema.fields().iter().enumerate() {
        if let Ok(Some(_)) = GeoArrowType::from_extension_field(field.as_ref()) {
            geom_indices.push(field_idx);
        }
    }
    geom_indices
}
