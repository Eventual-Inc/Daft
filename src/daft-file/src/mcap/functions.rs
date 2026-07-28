use std::collections::BTreeSet;

use common_error::{DaftError, DaftResult};
use daft_core::{
    file::{MediaType, MediaTypeMcap},
    lit::Literal,
    prelude::*,
    series::from_lit::series_from_literals_iter,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg},
};
use serde::{Deserialize, Serialize};

use crate::{
    DaftFile,
    mcap::{
        BUFFER_SIZE_MCAP_METADATA, McapMetadata as ParsedMcapMetadata, ReadMetadataOptions,
        read_metadata_with_options,
    },
};

fn time_range_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("start_time", DataType::UInt64),
        Field::new("end_time", DataType::UInt64),
    ])
}

fn summary_projection_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("topics", DataType::List(Box::new(DataType::Utf8))),
        Field::new("message_count", DataType::UInt64),
        Field::new("time_range", time_range_dtype()),
    ])
}

fn struct_literal<const N: usize>(entries: [(String, Literal); N]) -> Literal {
    Literal::Struct(entries.into_iter().collect())
}

fn list_literal(values: Vec<Literal>, child_dtype: DataType) -> DaftResult<Literal> {
    let values =
        series_from_literals_iter(values.into_iter().map(DaftResult::Ok), Some(child_dtype))?;
    Ok(Literal::List(values))
}

fn summary_projection_literal(metadata: ParsedMcapMetadata) -> DaftResult<Literal> {
    let mut seen = BTreeSet::new();
    let topics = metadata
        .channels
        .into_iter()
        .filter_map(|channel| seen.insert(channel.topic.clone()).then_some(channel.topic))
        .map(Literal::Utf8)
        .collect();
    let topics = list_literal(topics, DataType::Utf8)?;

    let (message_count, start_time, end_time) = metadata.statistics.map_or_else(
        || (Literal::Null, Literal::Null, Literal::Null),
        |statistics| {
            let (start_time, end_time) = if statistics.message_count == 0 {
                (Literal::Null, Literal::Null)
            } else {
                (
                    Literal::UInt64(statistics.message_start_time),
                    Literal::UInt64(statistics.message_end_time),
                )
            };
            (
                Literal::UInt64(statistics.message_count),
                start_time,
                end_time,
            )
        },
    );
    let time_range = struct_literal([
        ("start_time".to_string(), start_time),
        ("end_time".to_string(), end_time),
    ]);

    Ok(struct_literal([
        ("topics".to_string(), topics),
        ("message_count".to_string(), message_count),
        ("time_range".to_string(), time_range),
    ]))
}

fn read_metadata(
    file_ref: daft_core::file::FileReference,
    options: ReadMetadataOptions,
) -> DaftResult<ParsedMcapMetadata> {
    let mut file = DaftFile::load_blocking(file_ref, false, Some(BUFFER_SIZE_MCAP_METADATA))?;
    read_metadata_with_options(&mut file, options)
}

fn validate_mcap_input(field: &Field, function_name: &str) -> DaftResult<()> {
    if matches!(field.dtype, DataType::File(MediaType::Mcap)) {
        Ok(())
    } else {
        Err(DaftError::TypeError(format!(
            "{function_name} requires a File(Mcap) input, got {field}"
        )))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct McapSummaryProjection;

#[typetag::serde]
impl ScalarUDF for McapSummaryProjection {
    fn name(&self) -> &'static str {
        "_mcap_summary_projection"
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = args.try_into()?;
        let files = input.file::<MediaTypeMcap>()?;
        let values = files
            .into_iter()
            .map(|file_ref| {
                file_ref.map_or(Ok(Literal::Null), |file_ref| {
                    let metadata = read_metadata(
                        file_ref,
                        ReadMetadataOptions {
                            include_schema_data: false,
                            include_metadata_records: false,
                            include_chunk_indexes: false,
                        },
                    )?;
                    summary_projection_literal(metadata)
                })
            })
            .collect::<Vec<_>>();
        Ok(
            series_from_literals_iter(values.into_iter(), Some(summary_projection_dtype()))?
                .rename(input.name()),
        )
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = args.try_into()?;
        let field = input.to_field(schema)?;
        validate_mcap_input(&field, self.name())?;
        Ok(Field::new(field.name, summary_projection_dtype()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcap::{ChannelMetadata, HeaderMetadata, StatisticsMetadata};

    fn channel(id: u16, topic: &str) -> ChannelMetadata {
        ChannelMetadata {
            id,
            topic: topic.to_string(),
            message_encoding: "protobuf".to_string(),
            schema_id: None,
            schema_name: None,
            message_count: None,
            metadata: Default::default(),
        }
    }

    fn metadata(
        channels: Vec<ChannelMetadata>,
        statistics: Option<StatisticsMetadata>,
    ) -> ParsedMcapMetadata {
        ParsedMcapMetadata {
            file_size: 0,
            has_summary: true,
            indexed: false,
            has_chunk_indexes: false,
            has_message_indexes: false,
            header: HeaderMetadata {
                profile: String::new(),
                library: String::new(),
            },
            statistics,
            schemas: vec![],
            channels,
            chunks: vec![],
            attachments: vec![],
            metadata: vec![],
        }
    }

    fn statistics(message_count: u64) -> StatisticsMetadata {
        StatisticsMetadata {
            message_count,
            schema_count: 0,
            channel_count: 0,
            attachment_count: 0,
            metadata_count: 0,
            chunk_count: 0,
            message_start_time: 100,
            message_end_time: 200,
        }
    }

    #[test]
    fn summary_projection_deduplicates_topics_and_reports_statistics() {
        let projection = summary_projection_literal(metadata(
            vec![
                channel(1, "/state"),
                channel(2, "/camera"),
                channel(3, "/state"),
            ],
            Some(statistics(3)),
        ))
        .unwrap();

        let Literal::Struct(fields) = projection else {
            panic!("summary projection must be a struct");
        };
        let Literal::List(topics) = fields.get("topics").unwrap() else {
            panic!("topics must be a list");
        };
        assert_eq!(
            (0..topics.len())
                .map(|index| topics.get_lit(index))
                .collect::<Vec<_>>(),
            vec![
                Literal::Utf8("/state".to_string()),
                Literal::Utf8("/camera".to_string()),
            ]
        );
        assert_eq!(fields.get("message_count"), Some(&Literal::UInt64(3)));
        assert_eq!(
            fields.get("time_range"),
            Some(&struct_literal([
                ("start_time".to_string(), Literal::UInt64(100)),
                ("end_time".to_string(), Literal::UInt64(200)),
            ]))
        );
    }

    #[test]
    fn summary_projection_has_null_counts_and_bounds_without_statistics() {
        let projection = summary_projection_literal(metadata(vec![], None)).unwrap();

        let Literal::Struct(fields) = projection else {
            panic!("summary projection must be a struct");
        };
        assert_eq!(fields.get("message_count"), Some(&Literal::Null));
        assert_eq!(
            fields.get("time_range"),
            Some(&struct_literal([
                ("start_time".to_string(), Literal::Null),
                ("end_time".to_string(), Literal::Null),
            ]))
        );
    }

    #[test]
    fn summary_projection_has_null_bounds_for_an_empty_file() {
        let projection = summary_projection_literal(metadata(vec![], Some(statistics(0)))).unwrap();

        let Literal::Struct(fields) = projection else {
            panic!("summary projection must be a struct");
        };
        assert_eq!(fields.get("message_count"), Some(&Literal::UInt64(0)));
        assert_eq!(
            fields.get("time_range"),
            Some(&struct_literal([
                ("start_time".to_string(), Literal::Null),
                ("end_time".to_string(), Literal::Null),
            ]))
        );
    }
}
