use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Series};

use super::producer::KafkaHeader;

pub(crate) fn validate_header_key(key: &str) -> DaftResult<()> {
    if key.is_empty() {
        return Err(DaftError::ValueError(
            "[write_kafka] kafka header key must not be empty".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn make_header(key: &str, value: Option<&[u8]>) -> DaftResult<KafkaHeader> {
    validate_header_key(key)?;

    Ok(KafkaHeader {
        key: key.to_string(),
        value: value.map(<[u8]>::to_vec),
    })
}

pub(crate) fn headers_for_row(headers: &Series, row_idx: usize) -> DaftResult<Vec<KafkaHeader>> {
    let Some(entries) = headers
        .list()
        .map_err(|err| {
            DaftError::ValueError(format!(
                "[write_kafka] kafka headers must be List(Struct([key, value])): {err}"
            ))
        })?
        .get(row_idx)
    else {
        return Ok(vec![]);
    };

    let entries = entries.struct_().map_err(|err| {
        DaftError::ValueError(format!(
            "[write_kafka] kafka headers must contain struct entries: {err}"
        ))
    })?;
    let keys = entries.get("key")?;
    let values = entries.get("value")?;
    let keys = keys.utf8().map_err(|err| {
        DaftError::ValueError(format!(
            "[write_kafka] kafka header key must be Utf8: {err}"
        ))
    })?;

    let mut parsed = Vec::with_capacity(keys.len());
    for header_idx in 0..keys.len() {
        let key = keys.get(header_idx).ok_or_else(|| {
            DaftError::ValueError("[write_kafka] kafka header key must not be null".to_string())
        })?;
        let value = match values.data_type() {
            DataType::Binary => values.binary()?.get(header_idx).map(<[u8]>::to_vec),
            DataType::Utf8 => values
                .utf8()?
                .get(header_idx)
                .map(|value| value.as_bytes().to_vec()),
            DataType::Null => None,
            other => {
                return Err(DaftError::ValueError(format!(
                    "[write_kafka] kafka header value must be Binary, Utf8, or Null, got {other}"
                )));
            }
        };
        parsed.push(make_header(key, value.as_deref())?);
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{BinaryBuilder, LargeListBuilder, StringBuilder, StructBuilder};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields};
    use daft_core::prelude::Field;

    use super::*;

    #[test]
    fn preserves_duplicate_keys_and_order() {
        let headers = vec![
            make_header("same", Some(b"first")).unwrap(),
            make_header("other", Some(b"middle")).unwrap(),
            make_header("same", Some(b"second")).unwrap(),
        ];

        assert_eq!(headers[0].key, "same");
        assert_eq!(headers[0].value.as_deref(), Some(&b"first"[..]));
        assert_eq!(headers[1].key, "other");
        assert_eq!(headers[1].value.as_deref(), Some(&b"middle"[..]));
        assert_eq!(headers[2].key, "same");
        assert_eq!(headers[2].value.as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn preserves_null_header_values() {
        let header = make_header("nullable", None).unwrap();

        assert_eq!(header.key, "nullable");
        assert_eq!(header.value, None);
    }

    #[test]
    fn rejects_empty_header_key() {
        let err = validate_header_key("").unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("header key"));
    }

    fn header_series() -> Series {
        let mut builder = arrow_header_builder(4);
        append_header(&mut builder, "same", Some(b"first"));
        append_header(&mut builder, "other", Some(b"middle"));
        append_header(&mut builder, "same", Some(b"second"));
        builder.append(true);
        builder.append(false);
        append_header(&mut builder, "nullable", None);
        builder.append(true);

        Series::from_arrow(
            Arc::new(Field::new(
                "headers",
                DataType::List(Box::new(header_struct_dtype())),
            )),
            Arc::new(builder.finish()),
        )
        .unwrap()
    }

    #[test]
    fn parses_list_of_struct_headers_for_row() {
        let headers = header_series();

        let first_row = headers_for_row(&headers, 0).unwrap();
        let null_row = headers_for_row(&headers, 1).unwrap();
        let third_row = headers_for_row(&headers, 2).unwrap();

        assert_eq!(
            first_row,
            vec![
                make_header("same", Some(b"first")).unwrap(),
                make_header("other", Some(b"middle")).unwrap(),
                make_header("same", Some(b"second")).unwrap(),
            ]
        );
        assert!(null_row.is_empty());
        assert_eq!(third_row, vec![make_header("nullable", None).unwrap()]);
    }

    #[test]
    fn rejects_null_header_key_from_projection() {
        let mut builder = arrow_header_builder(1);
        {
            let values = builder.values();
            values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_null();
            values
                .field_builder::<BinaryBuilder>(1)
                .unwrap()
                .append_value(b"value");
            values.append(true);
        }
        builder.append(true);
        let headers = Series::from_arrow(
            Arc::new(Field::new(
                "headers",
                DataType::List(Box::new(header_struct_dtype())),
            )),
            Arc::new(builder.finish()),
        )
        .unwrap();

        let err = headers_for_row(&headers, 0).unwrap_err();

        assert!(err.to_string().contains("[write_kafka]"));
        assert!(err.to_string().contains("header key"));
    }

    fn header_struct_dtype() -> DataType {
        DataType::Struct(vec![
            Field::new("key", DataType::Utf8),
            Field::new("value", DataType::Binary),
        ])
    }

    fn arrow_header_builder(capacity: usize) -> LargeListBuilder<StructBuilder> {
        LargeListBuilder::new(StructBuilder::from_fields(
            Fields::from(vec![
                ArrowField::new("key", ArrowDataType::Utf8, true),
                ArrowField::new("value", ArrowDataType::Binary, true),
            ]),
            capacity,
        ))
    }

    fn append_header(
        builder: &mut LargeListBuilder<StructBuilder>,
        key: &str,
        value: Option<&[u8]>,
    ) {
        let values = builder.values();
        values
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(key);
        let value_builder = values.field_builder::<BinaryBuilder>(1).unwrap();
        match value {
            Some(value) => value_builder.append_value(value),
            None => value_builder.append_null(),
        }
        values.append(true);
    }
}
