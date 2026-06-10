use std::fmt::Write as _;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Literal, Series};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::sink_info::{
    KafkaKeyFormat, KafkaPartition, KafkaTopic, KafkaValueFormat, KafkaWriteInfo,
};
use daft_micropartition::MicroPartition;

#[cfg(test)]
use super::producer::KafkaHeader;
use super::{headers::headers_for_row, producer::KafkaOutgoingRecord};

pub(crate) fn validate_topic(topic: &str) -> DaftResult<()> {
    if topic.is_empty() {
        return Err(DaftError::ValueError(
            "[write_kafka] kafka topic must not be empty".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn validate_partition(partition: i32) -> DaftResult<()> {
    if partition < 0 {
        return Err(DaftError::ValueError(format!(
            "[write_kafka] kafka partition must be non-negative, got {partition}"
        )));
    }

    Ok(())
}

#[derive(Debug)]
struct Projection {
    exprs: Vec<BoundExpr>,
    topic: Option<usize>,
    value: usize,
    key: Option<usize>,
    headers: Option<usize>,
    partition: Option<usize>,
    timestamp_ms: Option<usize>,
}

impl Projection {
    fn new(info: &KafkaWriteInfo<BoundExpr>) -> Self {
        let mut exprs = Vec::new();
        let topic = match &info.topic {
            KafkaTopic::Static(_) => None,
            KafkaTopic::Dynamic(expr) => Some(push_expr(&mut exprs, expr.clone())),
        };
        let value = push_expr(&mut exprs, info.value_col.clone());
        let key = info
            .key_col
            .as_ref()
            .map(|expr| push_expr(&mut exprs, expr.clone()));
        let headers = info
            .headers_col
            .as_ref()
            .map(|expr| push_expr(&mut exprs, expr.clone()));
        let partition = match &info.partition {
            Some(KafkaPartition::Dynamic(expr)) => Some(push_expr(&mut exprs, expr.clone())),
            Some(KafkaPartition::Static(_)) | None => None,
        };
        let timestamp_ms = info
            .timestamp_ms_col
            .as_ref()
            .map(|expr| push_expr(&mut exprs, expr.clone()));

        Self {
            exprs,
            topic,
            value,
            key,
            headers,
            partition,
            timestamp_ms,
        }
    }
}

fn push_expr(exprs: &mut Vec<BoundExpr>, expr: BoundExpr) -> usize {
    let idx = exprs.len();
    exprs.push(expr);
    idx
}

pub(crate) fn records_from_micropartition(
    input: &MicroPartition,
    info: &KafkaWriteInfo<BoundExpr>,
) -> DaftResult<Vec<KafkaOutgoingRecord>> {
    if let KafkaTopic::Static(topic) = &info.topic {
        validate_topic(topic)?;
    }
    if let Some(KafkaPartition::Static(partition)) = info.partition {
        validate_partition(partition)?;
    }

    let projection = Projection::new(info);
    let mut records = Vec::with_capacity(input.len());

    for batch in input.record_batches() {
        let projected = batch.eval_expression_list(&projection.exprs)?;
        for row_idx in 0..projected.len() {
            let topic = projected_topic(info, projection.topic, &projected, row_idx)?;
            let key = projection
                .key
                .map(|idx| projected_key(projected.get_column(idx), row_idx, &info.key_format))
                .transpose()?
                .flatten();
            let value = projected_value(
                projected.get_column(projection.value),
                row_idx,
                &info.value_format,
            )?;
            let headers = projection
                .headers
                .map(|idx| headers_for_row(projected.get_column(idx), row_idx))
                .transpose()?
                .unwrap_or_default();
            let partition = projected_partition(info, projection.partition, &projected, row_idx)?;
            let timestamp_ms = projection
                .timestamp_ms
                .map(|idx| projected_timestamp(projected.get_column(idx), row_idx))
                .transpose()?
                .flatten();

            records.push(KafkaOutgoingRecord {
                topic,
                key,
                value,
                headers,
                partition,
                timestamp_ms,
            });
        }
    }

    Ok(records)
}

fn projected_topic(
    info: &KafkaWriteInfo<BoundExpr>,
    topic_idx: Option<usize>,
    projected: &daft_recordbatch::RecordBatch,
    row_idx: usize,
) -> DaftResult<String> {
    match (&info.topic, topic_idx) {
        (KafkaTopic::Static(topic), None) => Ok(topic.clone()),
        (KafkaTopic::Dynamic(_), Some(idx)) => {
            let topic = projected
                .get_column(idx)
                .utf8()
                .map_err(|err| {
                    DaftError::ValueError(format!(
                        "[write_kafka] kafka dynamic topic must be Utf8: {err}"
                    ))
                })?
                .get(row_idx)
                .ok_or_else(|| {
                    DaftError::ValueError(
                        "[write_kafka] kafka dynamic topic must not be null".to_string(),
                    )
                })?;
            validate_topic(topic)?;
            Ok(topic.to_string())
        }
        _ => unreachable!("topic projection index must match topic configuration"),
    }
}

fn projected_value(
    value: &Series,
    row_idx: usize,
    format: &KafkaValueFormat,
) -> DaftResult<Option<Vec<u8>>> {
    match format {
        KafkaValueFormat::Raw => Ok(value
            .binary()
            .map_err(|err| {
                DaftError::ValueError(format!(
                    "[write_kafka] kafka raw value must be Binary: {err}"
                ))
            })?
            .get(row_idx)
            .map(<[u8]>::to_vec)),
        KafkaValueFormat::Utf8 => Ok(value
            .utf8()
            .map_err(|err| {
                DaftError::ValueError(format!(
                    "[write_kafka] kafka utf8 value must be Utf8: {err}"
                ))
            })?
            .get(row_idx)
            .map(|value| value.as_bytes().to_vec())),
        KafkaValueFormat::Json => literal_to_json_bytes(&value.get_lit(row_idx)),
    }
}

fn projected_key(
    key: &Series,
    row_idx: usize,
    format: &KafkaKeyFormat,
) -> DaftResult<Option<Vec<u8>>> {
    match format {
        KafkaKeyFormat::Raw => Ok(key
            .binary()
            .map_err(|err| {
                DaftError::ValueError(format!("[write_kafka] kafka raw key must be Binary: {err}"))
            })?
            .get(row_idx)
            .map(<[u8]>::to_vec)),
        KafkaKeyFormat::Utf8 => Ok(key
            .utf8()
            .map_err(|err| {
                DaftError::ValueError(format!("[write_kafka] kafka utf8 key must be Utf8: {err}"))
            })?
            .get(row_idx)
            .map(|value| value.as_bytes().to_vec())),
    }
}

fn projected_partition(
    info: &KafkaWriteInfo<BoundExpr>,
    partition_idx: Option<usize>,
    projected: &daft_recordbatch::RecordBatch,
    row_idx: usize,
) -> DaftResult<Option<i32>> {
    match (&info.partition, partition_idx) {
        (Some(KafkaPartition::Static(partition)), None) => Ok(Some(*partition)),
        (Some(KafkaPartition::Dynamic(_)), Some(idx)) => {
            let partition = projected.get_column(idx);
            let partition = match partition.data_type() {
                DataType::Int32 => partition.i32()?.get(row_idx),
                DataType::Int64 => partition
                    .i64()?
                    .get(row_idx)
                    .map(|value| {
                        i32::try_from(value).map_err(|_| {
                            DaftError::ValueError(format!(
                                "[write_kafka] kafka partition must fit Int32, got {value}"
                            ))
                        })
                    })
                    .transpose()?,
                other => {
                    return Err(DaftError::ValueError(format!(
                        "[write_kafka] kafka dynamic partition must be Int32 or Int64, got {other}"
                    )));
                }
            };
            if let Some(partition) = partition {
                validate_partition(partition)?;
            }
            Ok(partition)
        }
        (None, None) => Ok(None),
        _ => unreachable!("partition projection index must match partition configuration"),
    }
}

fn projected_timestamp(timestamp: &Series, row_idx: usize) -> DaftResult<Option<i64>> {
    timestamp
        .i64()
        .map_err(|err| {
            DaftError::ValueError(format!(
                "[write_kafka] kafka timestamp_ms must be Int64: {err}"
            ))
        })
        .map(|timestamp| timestamp.get(row_idx))
}

fn literal_to_json_bytes(value: &Literal) -> DaftResult<Option<Vec<u8>>> {
    let json = match value {
        Literal::Null => return Ok(None),
        Literal::Boolean(value) => value.to_string(),
        Literal::Utf8(value) => quote_json_string(value),
        Literal::Int8(value) => value.to_string(),
        Literal::UInt8(value) => value.to_string(),
        Literal::Int16(value) => value.to_string(),
        Literal::UInt16(value) => value.to_string(),
        Literal::Int32(value) => value.to_string(),
        Literal::UInt32(value) => value.to_string(),
        Literal::Int64(value) => value.to_string(),
        Literal::UInt64(value) => value.to_string(),
        Literal::Float32(value) => {
            if !value.is_finite() {
                return Err(DaftError::ValueError(
                    "[write_kafka] kafka JSON numeric value must be finite".to_string(),
                ));
            }
            value.to_string()
        }
        Literal::Float64(value) => {
            if !value.is_finite() {
                return Err(DaftError::ValueError(
                    "[write_kafka] kafka JSON numeric value must be finite".to_string(),
                ));
            }
            value.to_string()
        }
        other => {
            return Err(DaftError::ValueError(format!(
                "[write_kafka] kafka JSON value does not yet support {}",
                other.get_type()
            )));
        }
    };

    Ok(Some(json.into_bytes()))
}

fn quote_json_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\u{08}' => quoted.push_str("\\b"),
            '\u{0c}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            ch if ch < '\u{20}' => {
                write!(quoted, "\\u{:04x}", ch as u32).expect("writing to String cannot fail");
            }
            ch => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
pub(crate) fn make_test_record(
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: Vec<KafkaHeader>,
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
) -> DaftResult<KafkaOutgoingRecord> {
    validate_topic(topic)?;
    if let Some(partition) = partition {
        validate_partition(partition)?;
    }

    Ok(KafkaOutgoingRecord {
        topic: topic.to_string(),
        key: key.map(<[u8]>::to_vec),
        value: value.map(<[u8]>::to_vec),
        headers,
        partition,
        timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use daft_core::prelude::{
        BinaryArray, Field, Float32Array, Float64Array, Int32Array, Int64Array, IntoSeries,
        Utf8Array,
    };
    use daft_dsl::bound_col;
    use daft_logical_plan::sink_info::{KafkaKeyFormat, KafkaTopic};
    use daft_recordbatch::RecordBatch;

    use super::*;
    use crate::kafka::headers::make_header;

    fn bound(index: usize, name: &str, dtype: DataType) -> BoundExpr {
        BoundExpr::new_unchecked(bound_col(index, Field::new(name, dtype)))
    }

    fn micropartition(batch: RecordBatch) -> MicroPartition {
        MicroPartition::new_loaded(batch.schema.clone(), Arc::new(vec![batch]), None)
    }

    fn kafka_info(value_format: KafkaValueFormat) -> KafkaWriteInfo<BoundExpr> {
        KafkaWriteInfo {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: KafkaTopic::Static("topic".to_string()),
            value_col: bound(1, "value", DataType::Binary),
            key_col: Some(bound(0, "key", DataType::Binary)),
            headers_col: None,
            partition: None,
            timestamp_ms_col: None,
            value_format,
            key_format: KafkaKeyFormat::Raw,
            kafka_client_config: BTreeMap::new(),
            timeout_ms: 10,
        }
    }

    #[test]
    fn makes_valid_record_and_counts_bytes() {
        let record = make_test_record(
            "topic",
            Some(b"key"),
            Some(b"value"),
            vec![make_header("h", Some(b"v")).unwrap()],
            Some(3),
            Some(456),
        )
        .unwrap();

        assert_eq!(record.topic, "topic");
        assert_eq!(record.key.as_deref(), Some(&b"key"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"value"[..]));
        assert_eq!(record.partition, Some(3));
        assert_eq!(record.timestamp_ms, Some(456));
        assert_eq!(record.delivered_bytes(), 3 + 5 + 1 + 1);
    }

    #[test]
    fn supports_tombstone_value() {
        let record =
            make_test_record("topic", Some(b"key"), None, vec![], None, Some(789)).unwrap();

        assert_eq!(record.value, None);
        assert_eq!(record.timestamp_ms, Some(789));
        assert_eq!(record.delivered_bytes(), 3);
    }

    #[test]
    fn rejects_empty_topic() {
        let err = make_test_record("", None, Some(b"value"), vec![], None, None).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn rejects_negative_partition() {
        let err =
            make_test_record("topic", None, Some(b"value"), vec![], Some(-1), None).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("partition"));
    }

    #[test]
    fn projects_raw_binary_records_from_micropartition() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            BinaryArray::from_iter("key", [Some(b"k1".as_slice()), None]).into_series(),
            BinaryArray::from_iter("value", [Some(b"v1".as_slice()), None]).into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);

        let records =
            records_from_micropartition(&input, &kafka_info(KafkaValueFormat::Raw)).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].topic, "topic");
        assert_eq!(records[0].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(records[0].value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(records[1].key, None);
        assert_eq!(records[1].value, None);
    }

    #[test]
    fn projects_dynamic_topic_partition_and_timestamp() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_iter("topic", [Some("a"), Some("b")]).into_series(),
            BinaryArray::from_iter("value", [Some(b"v1".as_slice()), Some(b"v2".as_slice())])
                .into_series(),
            Int64Array::from_iter(Field::new("partition", DataType::Int64), [Some(2), None])
                .into_series(),
            Int64Array::from_iter(Field::new("ts", DataType::Int64), [Some(1000), None])
                .into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);
        let mut info = kafka_info(KafkaValueFormat::Raw);
        info.topic = KafkaTopic::Dynamic(bound(0, "topic", DataType::Utf8));
        info.value_col = bound(1, "value", DataType::Binary);
        info.key_col = None;
        info.partition = Some(KafkaPartition::Dynamic(bound(
            2,
            "partition",
            DataType::Int64,
        )));
        info.timestamp_ms_col = Some(bound(3, "ts", DataType::Int64));

        let records = records_from_micropartition(&input, &info).unwrap();

        assert_eq!(records[0].topic, "a");
        assert_eq!(records[0].partition, Some(2));
        assert_eq!(records[0].timestamp_ms, Some(1000));
        assert_eq!(records[1].topic, "b");
        assert_eq!(records[1].partition, None);
        assert_eq!(records[1].timestamp_ms, None);
    }

    #[test]
    fn rejects_null_or_empty_dynamic_topic() {
        for topic in [None, Some("")] {
            let batch = RecordBatch::from_nonempty_columns(vec![
                Utf8Array::from_iter("topic", [topic]).into_series(),
                BinaryArray::from_iter("value", [Some(b"value".as_slice())]).into_series(),
            ])
            .unwrap();
            let input = micropartition(batch);
            let mut info = kafka_info(KafkaValueFormat::Raw);
            info.topic = KafkaTopic::Dynamic(bound(0, "topic", DataType::Utf8));
            info.value_col = bound(1, "value", DataType::Binary);
            info.key_col = None;

            let err = records_from_micropartition(&input, &info).unwrap_err();

            assert!(err.to_string().contains("[write_kafka]"));
            assert!(err.to_string().contains("topic"));
        }
    }

    #[test]
    fn json_utf8_value_is_quoted_and_null_is_tombstone() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_iter("value", [Some("hello\nworld"), None]).into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);
        let mut info = kafka_info(KafkaValueFormat::Json);
        info.value_col = bound(0, "value", DataType::Utf8);
        info.key_col = None;

        let records = records_from_micropartition(&input, &info).unwrap();

        assert_eq!(records[0].value.as_deref(), Some(&b"\"hello\\nworld\""[..]));
        assert_eq!(records[1].value, None);
    }

    #[test]
    fn rejects_non_finite_json_float_values() {
        for (value, dtype) in [
            (
                Float32Array::from_slice("value", &[f32::NAN]).into_series(),
                DataType::Float32,
            ),
            (
                Float32Array::from_slice("value", &[f32::INFINITY]).into_series(),
                DataType::Float32,
            ),
            (
                Float64Array::from_slice("value", &[f64::NEG_INFINITY]).into_series(),
                DataType::Float64,
            ),
        ] {
            let batch = RecordBatch::from_nonempty_columns(vec![value]).unwrap();
            let input = micropartition(batch);
            let mut info = kafka_info(KafkaValueFormat::Json);
            info.value_col = bound(0, "value", dtype);
            info.key_col = None;

            let err = records_from_micropartition(&input, &info).unwrap_err();

            assert!(err.to_string().contains("[write_kafka]"));
            assert!(
                err.to_string()
                    .contains("JSON numeric value must be finite")
            );
        }
    }

    #[test]
    fn projects_int32_dynamic_partition() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            BinaryArray::from_iter("value", [Some(b"value".as_slice())]).into_series(),
            Int32Array::from_iter(Field::new("partition", DataType::Int32), [Some(7)])
                .into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);
        let mut info = kafka_info(KafkaValueFormat::Raw);
        info.value_col = bound(0, "value", DataType::Binary);
        info.key_col = None;
        info.partition = Some(KafkaPartition::Dynamic(bound(
            1,
            "partition",
            DataType::Int32,
        )));

        let records = records_from_micropartition(&input, &info).unwrap();

        assert_eq!(records[0].partition, Some(7));
    }

    #[test]
    fn rejects_int64_dynamic_partition_overflow() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            BinaryArray::from_iter("value", [Some(b"value".as_slice())]).into_series(),
            Int64Array::from_iter(
                Field::new("partition", DataType::Int64),
                [Some(i64::from(i32::MAX) + 1)],
            )
            .into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);
        let mut info = kafka_info(KafkaValueFormat::Raw);
        info.value_col = bound(0, "value", DataType::Binary);
        info.key_col = None;
        info.partition = Some(KafkaPartition::Dynamic(bound(
            1,
            "partition",
            DataType::Int64,
        )));

        let err = records_from_micropartition(&input, &info).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("must fit Int32"));
    }

    #[test]
    fn rejects_negative_dynamic_partition() {
        let batch = RecordBatch::from_nonempty_columns(vec![
            BinaryArray::from_iter("value", [Some(b"value".as_slice())]).into_series(),
            Int64Array::from_iter(Field::new("partition", DataType::Int64), [Some(-1)])
                .into_series(),
        ])
        .unwrap();
        let input = micropartition(batch);
        let mut info = kafka_info(KafkaValueFormat::Raw);
        info.value_col = bound(0, "value", DataType::Binary);
        info.key_col = None;
        info.partition = Some(KafkaPartition::Dynamic(bound(
            1,
            "partition",
            DataType::Int64,
        )));

        let err = records_from_micropartition(&input, &info).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("partition"));
    }
}
