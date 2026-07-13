use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use arrow_array::{
    ArrayRef,
    builder::{
        ArrayBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int8Builder,
        Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, LargeListBuilder,
        LargeStringBuilder, NullBuilder, StructBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
        UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use async_stream::try_stream;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use common_error::{DaftError, DaftResult};
use daft_recordbatch::RecordBatch;
use daft_scan::{MongoSourceConfig, ScanTask};
use daft_schema::{
    dtype::DataType,
    field::Field,
    schema::{Schema, SchemaRef},
    time_unit::TimeUnit,
};
use futures::{
    TryStreamExt,
    stream::{self, BoxStream},
};
use mongodb::{
    Client,
    bson::{Binary, Bson, Document, doc, oid::ObjectId, spec::BinarySubtype},
    options::Hint,
};

static CLIENT_CACHE: OnceLock<Mutex<HashMap<String, Client>>> = OnceLock::new();

pub async fn stream_mongodb(
    scan_task: &ScanTask,
    config: &MongoSourceConfig,
    chunk_size: usize,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let config = config.clone();
    let schema = projected_schema(scan_task);
    let limit = scan_task.pushdowns.limit;
    if matches!(limit, Some(0)) {
        return Ok(Box::pin(stream::empty::<DaftResult<RecordBatch>>()));
    }
    let batch_size = if config.batch_size > 0 {
        config.batch_size as usize
    } else {
        chunk_size.max(1)
    };
    let chunk_size = chunk_size.max(1);
    let projection = projection_document(&config, &schema)?;
    let filter = query_document(&config)?;
    let hint = hint_from_json(&config)?;

    let stream = try_stream! {
        let client = client_for_uri(&config.uri).await?;
        let collection = client
            .database(&config.database)
            .collection::<Document>(&config.collection);

        let mut find = collection.find(filter).batch_size(batch_size as u32);
        if let Some(projection) = projection {
            find = find.projection(projection);
        }
        if let Some(hint) = hint {
            find = find.hint(hint);
        }
        if let Some(max_time_ms) = config.max_time_ms {
            find = find.max_time(Duration::from_millis(max_time_ms));
        }
        if let Some(limit) = limit {
            find = find.limit(i64::try_from(limit).map_err(|_| {
                DaftError::ValueError(format!("MongoDB read limit is too large for i64: {limit}"))
            })?);
        }

        let mut cursor = find.await.map_err(mongo_err)?;
        let mut builder = MongoBatchBuilder::new(schema.clone())?;
        let mut rows_left = limit;

        while let Some(doc) = cursor.try_next().await.map_err(mongo_err)? {
            if matches!(rows_left, Some(0)) {
                break;
            }
            builder.append_document(&doc)?;
            if let Some(left) = rows_left.as_mut() {
                *left = left.saturating_sub(1);
            }
            if builder.len() >= chunk_size {
                yield builder.finish()?;
                builder = MongoBatchBuilder::new(schema.clone())?;
            }
        }

        if !builder.is_empty() {
            yield builder.finish()?;
        }
    };

    Ok(Box::pin(stream))
}

async fn client_for_uri(uri: &str) -> DaftResult<Client> {
    let cache = CLIENT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(client) = lock_client_cache(cache)?.get(uri).cloned() {
        return Ok(client);
    }

    let client = Client::with_uri_str(uri).await.map_err(mongo_err)?;
    let mut cache = lock_client_cache(cache)?;
    let client = cache
        .entry(uri.to_string())
        .or_insert_with(|| client.clone())
        .clone();
    Ok(client)
}

fn lock_client_cache(
    cache: &Mutex<HashMap<String, Client>>,
) -> DaftResult<MutexGuard<'_, HashMap<String, Client>>> {
    cache
        .lock()
        .map_err(|_| DaftError::InternalError("MongoDB client cache lock was poisoned".to_string()))
}

fn projected_schema(scan_task: &ScanTask) -> SchemaRef {
    let Some(columns) = &scan_task.pushdowns.columns else {
        return scan_task.schema.clone();
    };
    let wanted = columns.iter().map(String::as_str).collect::<BTreeSet<_>>();
    Arc::new(Schema::new(
        scan_task
            .schema
            .fields()
            .iter()
            .filter(|field| wanted.contains(field.name.as_ref()))
            .cloned(),
    ))
}

fn projection_document(
    config: &MongoSourceConfig,
    schema: &SchemaRef,
) -> DaftResult<Option<Document>> {
    let mut projection = config
        .projection_json
        .as_deref()
        .map(parse_document_json)
        .transpose()?;

    let fields_to_fetch = schema
        .fields()
        .iter()
        .filter(|field| !matches!(field.dtype, DataType::Null))
        .map(|field| field.name.as_ref())
        .collect::<Vec<_>>();

    if fields_to_fetch.is_empty() {
        return Ok(Some(doc! { "_id": 1 }));
    }

    let schema_includes_id = fields_to_fetch.contains(&"_id");
    if !projection.as_ref().is_some_and(is_exclusion_projection) {
        let projection = projection.get_or_insert_with(Document::new);
        for field in fields_to_fetch {
            if projection.get(field).is_none() {
                projection.insert(field, 1);
            }
        }
    }
    if !schema_includes_id
        && let Some(projection) = projection.as_mut()
        && projection.get("_id").is_none()
    {
        projection.insert("_id", 0);
    }

    Ok(projection)
}

fn is_exclusion_projection(projection: &Document) -> bool {
    projection
        .iter()
        .any(|(field, value)| field != "_id" && is_projection_exclusion_value(value))
}

fn is_projection_exclusion_value(value: &Bson) -> bool {
    matches!(
        value,
        Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)
    )
}

fn hint_from_json(config: &MongoSourceConfig) -> DaftResult<Option<Hint>> {
    config
        .hint_json
        .as_deref()
        .map(|json| match parse_bson_json(json)? {
            Bson::Document(doc) => Ok(Hint::Keys(doc)),
            Bson::String(name) => Ok(Hint::Name(name)),
            other => Err(DaftError::ValueError(format!(
                "MongoDB hint must be an index key document or index name string, got {other:?}"
            ))),
        })
        .transpose()
}

fn query_document(config: &MongoSourceConfig) -> DaftResult<Document> {
    let base = config
        .filter_json
        .as_deref()
        .map(parse_document_json)
        .transpose()?
        .unwrap_or_default();

    let Some(partition_field) = &config.partition_field else {
        return Ok(base);
    };

    let mut range = Document::new();
    if let Some(lower) = &config.partition_lower_json {
        range.insert("$gte", parse_bson_json(lower)?);
    }
    if let Some(upper) = &config.partition_upper_json {
        range.insert("$lt", parse_bson_json(upper)?);
    }
    if range.is_empty() {
        return Ok(base);
    }

    let mut range_doc = Document::new();
    range_doc.insert(partition_field.clone(), range);
    if base.is_empty() {
        Ok(range_doc)
    } else {
        Ok(doc! { "$and": [base, range_doc] })
    }
}

fn parse_document_json(json: &str) -> DaftResult<Document> {
    match parse_bson_json(json)? {
        Bson::Document(doc) => Ok(doc),
        other => Err(DaftError::ValueError(format!(
            "Expected MongoDB JSON document, got {other:?}"
        ))),
    }
}

fn parse_bson_json(json: &str) -> DaftResult<Bson> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    json_value_to_bson(value)
}

fn json_value_to_bson(value: serde_json::Value) -> DaftResult<Bson> {
    Ok(match value {
        serde_json::Value::Null => Bson::Null,
        serde_json::Value::Bool(v) => Bson::Boolean(v),
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64() {
                if let Ok(v) = i32::try_from(v) {
                    Bson::Int32(v)
                } else {
                    Bson::Int64(v)
                }
            } else if n.as_u64().is_some() {
                return Err(DaftError::ValueError(format!(
                    "MongoDB JSON integer is outside i64 range: {n}"
                )));
            } else if let Some(v) = n.as_f64() {
                if !v.is_finite() {
                    return Err(DaftError::ValueError(format!(
                        "MongoDB JSON double must be finite: {n}"
                    )));
                }
                Bson::Double(v)
            } else {
                return Err(DaftError::ValueError(format!(
                    "Unsupported MongoDB JSON number: {n}"
                )));
            }
        }
        serde_json::Value::String(v) => Bson::String(v),
        serde_json::Value::Array(values) => Bson::Array(
            values
                .into_iter()
                .map(json_value_to_bson)
                .collect::<DaftResult<Vec<_>>>()?,
        ),
        serde_json::Value::Object(mut values) => {
            if values.len() != 1
                && let Some(key) = values.keys().find(|key| is_extended_json_key(key))
            {
                return Err(DaftError::ValueError(format!(
                    "MongoDB {key} extended JSON object cannot contain sibling keys"
                )));
            }
            if values.len() == 1 {
                if let Some(value) = values.remove("$oid") {
                    let serde_json::Value::String(value) = value else {
                        return Err(DaftError::ValueError(
                            "MongoDB $oid extended JSON value must be a string".to_string(),
                        ));
                    };
                    return ObjectId::parse_str(value).map(Bson::ObjectId).map_err(|e| {
                        DaftError::ValueError(format!("Invalid MongoDB ObjectId: {e}"))
                    });
                }
                if let Some(value) = values.remove("$numberLong") {
                    let serde_json::Value::String(value) = value else {
                        return Err(DaftError::ValueError(
                            "MongoDB $numberLong extended JSON value must be a string".to_string(),
                        ));
                    };
                    return value.parse::<i64>().map(Bson::Int64).map_err(|e| {
                        DaftError::ValueError(format!("Invalid MongoDB $numberLong value: {e}"))
                    });
                }
                if let Some(value) = values.remove("$numberInt") {
                    let serde_json::Value::String(value) = value else {
                        return Err(DaftError::ValueError(
                            "MongoDB $numberInt extended JSON value must be a string".to_string(),
                        ));
                    };
                    return value.parse::<i32>().map(Bson::Int32).map_err(|e| {
                        DaftError::ValueError(format!("Invalid MongoDB $numberInt value: {e}"))
                    });
                }
                if let Some(value) = values.remove("$numberDouble") {
                    let serde_json::Value::String(value) = value else {
                        return Err(DaftError::ValueError(
                            "MongoDB $numberDouble extended JSON value must be a string"
                                .to_string(),
                        ));
                    };
                    let value = value.parse::<f64>().map_err(|e| {
                        DaftError::ValueError(format!("Invalid MongoDB $numberDouble value: {e}"))
                    })?;
                    if !value.is_finite() {
                        return Err(DaftError::ValueError(
                            "MongoDB $numberDouble value must be finite".to_string(),
                        ));
                    }
                    return Ok(Bson::Double(value));
                }
                if let Some(value) = values.remove("$date") {
                    return extended_json_date_to_bson(value);
                }
                if let Some(value) = values.remove("$binary") {
                    return extended_json_binary_to_bson(value);
                }
            }

            Bson::Document(
                values
                    .into_iter()
                    .map(|(key, value)| Ok((key, json_value_to_bson(value)?)))
                    .collect::<DaftResult<Document>>()?,
            )
        }
    })
}

fn is_extended_json_key(key: &str) -> bool {
    matches!(
        key,
        "$oid" | "$numberLong" | "$numberInt" | "$numberDouble" | "$date" | "$binary"
    )
}

fn extended_json_date_to_bson(value: serde_json::Value) -> DaftResult<Bson> {
    let millis = match value {
        serde_json::Value::Number(n) => n.as_i64().ok_or_else(|| {
            DaftError::ValueError(format!(
                "MongoDB $date milliseconds must be an integer: {n}"
            ))
        })?,
        serde_json::Value::Object(mut values) => {
            let Some(serde_json::Value::String(value)) = values.remove("$numberLong") else {
                return Err(DaftError::ValueError(
                    "MongoDB $date object must contain a string $numberLong value".to_string(),
                ));
            };
            if !values.is_empty() {
                return Err(DaftError::ValueError(
                    "MongoDB $date object must only contain $numberLong".to_string(),
                ));
            }
            value.parse::<i64>().map_err(|e| {
                DaftError::ValueError(format!("Invalid MongoDB $date $numberLong value: {e}"))
            })?
        }
        other => {
            return Err(DaftError::ValueError(format!(
                "Unsupported MongoDB $date JSON value: {other}"
            )));
        }
    };
    Ok(Bson::DateTime(mongodb::bson::DateTime::from_millis(millis)))
}

fn extended_json_binary_to_bson(value: serde_json::Value) -> DaftResult<Bson> {
    let serde_json::Value::Object(mut values) = value else {
        return Err(DaftError::ValueError(
            "MongoDB $binary value must be an object".to_string(),
        ));
    };
    let Some(serde_json::Value::String(base64_value)) = values.remove("base64") else {
        return Err(DaftError::ValueError(
            "MongoDB $binary object must contain a string base64 value".to_string(),
        ));
    };
    let Some(serde_json::Value::String(subtype_value)) = values.remove("subType") else {
        return Err(DaftError::ValueError(
            "MongoDB $binary object must contain a string subType value".to_string(),
        ));
    };
    if !values.is_empty() {
        return Err(DaftError::ValueError(
            "MongoDB $binary object must only contain base64 and subType".to_string(),
        ));
    }
    let subtype = parse_binary_subtype(&subtype_value)?;
    let bytes = BASE64_STANDARD
        .decode(base64_value)
        .map_err(|e| DaftError::ValueError(format!("Invalid MongoDB $binary base64 value: {e}")))?;
    Ok(Bson::Binary(Binary {
        subtype: BinarySubtype::from(subtype),
        bytes,
    }))
}

fn parse_binary_subtype(value: &str) -> DaftResult<u8> {
    if value.len() != 2 {
        return Err(DaftError::ValueError(
            "MongoDB $binary subType value must be two hex characters".to_string(),
        ));
    }
    u8::from_str_radix(value, 16)
        .map_err(|e| DaftError::ValueError(format!("Invalid MongoDB $binary subType value: {e}")))
}

fn mongo_err(error: mongodb::error::Error) -> DaftError {
    DaftError::External(Box::new(error))
}

enum ColumnBuilder {
    Null(NullBuilder),
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(LargeStringBuilder),
    Binary(LargeBinaryBuilder),
    Date(Date32Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
    Struct(StructBuilder, Vec<Field>),
    List(LargeListBuilder<Box<dyn ArrayBuilder>>, Field),
}

impl ColumnBuilder {
    fn try_new(field: &Field) -> DaftResult<Self> {
        Ok(match &field.dtype {
            DataType::Null => Self::Null(NullBuilder::new()),
            DataType::Boolean => Self::Boolean(BooleanBuilder::new()),
            DataType::Int8 => Self::Int8(Int8Builder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::UInt8 => Self::UInt8(UInt8Builder::new()),
            DataType::UInt16 => Self::UInt16(UInt16Builder::new()),
            DataType::UInt32 => Self::UInt32(UInt32Builder::new()),
            DataType::UInt64 => Self::UInt64(UInt64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Utf8 => Self::Utf8(LargeStringBuilder::new()),
            DataType::Binary => Self::Binary(LargeBinaryBuilder::new()),
            DataType::Date => Self::Date(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Seconds, _) => {
                Self::TimestampSecond(TimestampSecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Milliseconds, _) => {
                Self::TimestampMillisecond(TimestampMillisecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microseconds, _) => {
                Self::TimestampMicrosecond(TimestampMicrosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Nanoseconds, _) => {
                Self::TimestampNanosecond(TimestampNanosecondBuilder::new())
            }
            DataType::Struct(fields) => {
                let arrow_fields = fields
                    .iter()
                    .map(Field::to_arrow)
                    .collect::<DaftResult<Vec<_>>>()?;
                let field_builders = fields
                    .iter()
                    .map(|field| {
                        Self::try_new(field)
                            .map(|builder| Box::new(builder) as Box<dyn ArrayBuilder>)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;
                Self::Struct(
                    StructBuilder::new(arrow_fields, field_builders),
                    fields.clone(),
                )
            }
            DataType::List(child_dtype) => {
                let child_field = Field::new("item", child_dtype.as_ref().clone());
                let child_arrow_field = Arc::new(child_field.to_arrow()?);
                let child_builder = Box::new(Self::try_new(&child_field)?) as Box<dyn ArrayBuilder>;
                Self::List(
                    LargeListBuilder::new(child_builder).with_field(child_arrow_field),
                    child_field,
                )
            }
            dtype => {
                return Err(DaftError::NotImplemented(format!(
                    "read_mongodb does not support BSON conversion for Daft dtype {dtype}"
                )));
            }
        })
    }

    fn append(&mut self, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
        match self {
            Self::Null(builder) => {
                builder.append_null();
                Ok(())
            }
            Self::Boolean(builder) => append_bool(builder, field, value),
            Self::Int8(builder) => append_i8(builder, field, value),
            Self::Int16(builder) => append_i16(builder, field, value),
            Self::Int32(builder) => append_i32(builder, field, value),
            Self::Int64(builder) => append_i64(builder, field, value),
            Self::UInt8(builder) => append_u8(builder, field, value),
            Self::UInt16(builder) => append_u16(builder, field, value),
            Self::UInt32(builder) => append_u32(builder, field, value),
            Self::UInt64(builder) => append_u64(builder, field, value),
            Self::Float32(builder) => append_f32(builder, field, value),
            Self::Float64(builder) => append_f64(builder, field, value),
            Self::Utf8(builder) => append_utf8(builder, field, value),
            Self::Binary(builder) => append_binary(builder, field, value),
            Self::Date(builder) => append_date(builder, field, value),
            Self::TimestampSecond(builder) => {
                append_timestamp(builder, TimeUnit::Seconds, field, value)
            }
            Self::TimestampMillisecond(builder) => {
                append_timestamp(builder, TimeUnit::Milliseconds, field, value)
            }
            Self::TimestampMicrosecond(builder) => {
                append_timestamp(builder, TimeUnit::Microseconds, field, value)
            }
            Self::TimestampNanosecond(builder) => {
                append_timestamp(builder, TimeUnit::Nanoseconds, field, value)
            }
            Self::Struct(builder, fields) => append_struct(builder, fields, field, value),
            Self::List(builder, child_field) => append_list(builder, child_field, field, value),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Null(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Int8(builder) => Arc::new(builder.finish()),
            Self::Int16(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt8(builder) => Arc::new(builder.finish()),
            Self::UInt16(builder) => Arc::new(builder.finish()),
            Self::UInt32(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Date(builder) => Arc::new(builder.finish()),
            Self::TimestampSecond(builder) => Arc::new(builder.finish()),
            Self::TimestampMillisecond(builder) => Arc::new(builder.finish()),
            Self::TimestampMicrosecond(builder) => Arc::new(builder.finish()),
            Self::TimestampNanosecond(builder) => Arc::new(builder.finish()),
            Self::Struct(builder, _) => Arc::new(builder.finish()),
            Self::List(builder, _) => Arc::new(builder.finish()),
        }
    }
}

impl ArrayBuilder for ColumnBuilder {
    fn len(&self) -> usize {
        match self {
            Self::Null(builder) => builder.len(),
            Self::Boolean(builder) => builder.len(),
            Self::Int8(builder) => builder.len(),
            Self::Int16(builder) => builder.len(),
            Self::Int32(builder) => builder.len(),
            Self::Int64(builder) => builder.len(),
            Self::UInt8(builder) => builder.len(),
            Self::UInt16(builder) => builder.len(),
            Self::UInt32(builder) => builder.len(),
            Self::UInt64(builder) => builder.len(),
            Self::Float32(builder) => builder.len(),
            Self::Float64(builder) => builder.len(),
            Self::Utf8(builder) => builder.len(),
            Self::Binary(builder) => builder.len(),
            Self::Date(builder) => builder.len(),
            Self::TimestampSecond(builder) => builder.len(),
            Self::TimestampMillisecond(builder) => builder.len(),
            Self::TimestampMicrosecond(builder) => builder.len(),
            Self::TimestampNanosecond(builder) => builder.len(),
            Self::Struct(builder, _) => builder.len(),
            Self::List(builder, _) => builder.len(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Self::finish(self)
    }

    fn finish_cloned(&self) -> ArrayRef {
        match self {
            Self::Null(builder) => Arc::new(builder.finish_cloned()),
            Self::Boolean(builder) => Arc::new(builder.finish_cloned()),
            Self::Int8(builder) => Arc::new(builder.finish_cloned()),
            Self::Int16(builder) => Arc::new(builder.finish_cloned()),
            Self::Int32(builder) => Arc::new(builder.finish_cloned()),
            Self::Int64(builder) => Arc::new(builder.finish_cloned()),
            Self::UInt8(builder) => Arc::new(builder.finish_cloned()),
            Self::UInt16(builder) => Arc::new(builder.finish_cloned()),
            Self::UInt32(builder) => Arc::new(builder.finish_cloned()),
            Self::UInt64(builder) => Arc::new(builder.finish_cloned()),
            Self::Float32(builder) => Arc::new(builder.finish_cloned()),
            Self::Float64(builder) => Arc::new(builder.finish_cloned()),
            Self::Utf8(builder) => Arc::new(builder.finish_cloned()),
            Self::Binary(builder) => Arc::new(builder.finish_cloned()),
            Self::Date(builder) => Arc::new(builder.finish_cloned()),
            Self::TimestampSecond(builder) => Arc::new(builder.finish_cloned()),
            Self::TimestampMillisecond(builder) => Arc::new(builder.finish_cloned()),
            Self::TimestampMicrosecond(builder) => Arc::new(builder.finish_cloned()),
            Self::TimestampNanosecond(builder) => Arc::new(builder.finish_cloned()),
            Self::Struct(builder, _) => Arc::new(builder.finish_cloned()),
            Self::List(builder, _) => Arc::new(builder.finish_cloned()),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

struct MongoBatchBuilder {
    schema: SchemaRef,
    fields: Vec<Field>,
    builders: Vec<ColumnBuilder>,
    len: usize,
}

impl MongoBatchBuilder {
    fn new(schema: SchemaRef) -> DaftResult<Self> {
        let fields = schema.fields().to_vec();
        let builders = fields
            .iter()
            .map(ColumnBuilder::try_new)
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Self {
            schema,
            fields,
            builders,
            len: 0,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn append_document(&mut self, document: &Document) -> DaftResult<()> {
        for (field, builder) in self.fields.iter().zip(self.builders.iter_mut()) {
            builder.append(field, document_get_field(document, field.name.as_ref()))?;
        }
        self.len += 1;
        Ok(())
    }

    fn finish(mut self) -> DaftResult<RecordBatch> {
        if self.builders.is_empty() {
            return RecordBatch::new_with_size(self.schema, vec![], self.len);
        }
        let arrays = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        RecordBatch::from_arrow(self.schema, arrays)
    }
}

fn document_get_field<'a>(document: &'a Document, field_name: &str) -> Option<&'a Bson> {
    if let Some(value) = document.get(field_name) {
        return Some(value);
    }

    let mut parts = field_name.split('.');
    let mut value = document.get(parts.next()?)?;
    for part in parts {
        value = match value {
            Bson::Document(nested) => nested.get(part)?,
            Bson::Array(values) => values.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}

fn append_null_for_missing_or_null<T>(
    builder: &mut T,
    value: Option<&Bson>,
    append_null: impl FnOnce(&mut T),
) -> bool {
    if matches!(value, None | Some(Bson::Null) | Some(Bson::Undefined)) {
        append_null(builder);
        true
    } else {
        false
    }
}

fn append_bool(
    builder: &mut BooleanBuilder,
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, BooleanBuilder::append_null) {
        return Ok(());
    }
    match value {
        Some(Bson::Boolean(value)) => builder.append_value(*value),
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_i8(builder: &mut Int8Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Int8Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_i8) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_i16(builder: &mut Int16Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Int16Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_i16) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_i32(builder: &mut Int32Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Int32Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_i32) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_i64(builder: &mut Int64Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, |builder| builder.append_null()) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_i64) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_u8(builder: &mut UInt8Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, UInt8Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_u8) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_u16(builder: &mut UInt16Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, UInt16Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_u16) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_u32(builder: &mut UInt32Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, UInt32Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_u32) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_u64(builder: &mut UInt64Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, UInt64Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_u64) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_f32(builder: &mut Float32Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Float32Builder::append_null) {
        return Ok(());
    }
    let Some(value) = value.and_then(bson_to_f32) else {
        return type_error(field, value);
    };
    builder.append_value(value);
    Ok(())
}

fn append_f64(builder: &mut Float64Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Float64Builder::append_null) {
        return Ok(());
    }
    match value {
        Some(Bson::Int32(value)) => builder.append_value(f64::from(*value)),
        Some(Bson::Int64(value)) => builder.append_value(*value as f64),
        Some(Bson::Double(value)) => builder.append_value(*value),
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_utf8(
    builder: &mut LargeStringBuilder,
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, LargeStringBuilder::append_null) {
        return Ok(());
    }
    match value {
        Some(Bson::String(value)) => builder.append_value(value),
        Some(Bson::ObjectId(value)) => builder.append_value(value.to_hex()),
        Some(Bson::Int32(value)) => builder.append_value(value.to_string()),
        Some(Bson::Int64(value)) => builder.append_value(value.to_string()),
        Some(Bson::Double(value)) => builder.append_value(value.to_string()),
        Some(Bson::Boolean(value)) => builder.append_value(value.to_string()),
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_binary(
    builder: &mut LargeBinaryBuilder,
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, LargeBinaryBuilder::append_null) {
        return Ok(());
    }
    match value {
        Some(Bson::Binary(Binary { bytes, .. })) => builder.append_value(bytes),
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_date(builder: &mut Date32Builder, field: &Field, value: Option<&Bson>) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, Date32Builder::append_null) {
        return Ok(());
    }
    match value {
        Some(Bson::DateTime(value)) => {
            builder.append_value(epoch_millis_to_date_days(value.timestamp_millis())?);
        }
        Some(Bson::Int64(value)) => {
            builder.append_value(epoch_millis_to_date_days(*value)?);
        }
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_timestamp(
    builder: &mut arrow_array::builder::PrimitiveBuilder<
        impl arrow_array::types::ArrowPrimitiveType<Native = i64>,
    >,
    unit: TimeUnit,
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if append_null_for_missing_or_null(builder, value, |builder| builder.append_null()) {
        return Ok(());
    }
    match value {
        Some(Bson::DateTime(value)) => {
            builder.append_value(epoch_millis_to_timeunit(value.timestamp_millis(), unit)?);
        }
        Some(Bson::Int64(value)) => {
            builder.append_value(epoch_millis_to_timeunit(*value, unit)?);
        }
        other => return type_error(field, other),
    }
    Ok(())
}

fn append_struct(
    builder: &mut StructBuilder,
    fields: &[Field],
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if matches!(value, None | Some(Bson::Null) | Some(Bson::Undefined)) {
        append_null_struct_children(builder, fields)?;
        builder.append(false);
        return Ok(());
    }

    if let Some(Bson::ObjectId(object_id)) = value {
        if fields.len() == 1 && fields[0].name.as_ref().eq_ignore_ascii_case("oid") {
            let child_field = &fields[0];
            let child_builder = builder.field_builders_mut()[0]
                .as_any_mut()
                .downcast_mut::<ColumnBuilder>()
                .ok_or_else(|| {
                    DaftError::InternalError(format!(
                        "MongoDB struct child builder for field {} is not a ColumnBuilder",
                        child_field.name
                    ))
                })?;
            child_builder.append(child_field, Some(&Bson::String(object_id.to_hex())))?;
            builder.append(true);
            return Ok(());
        }
        return type_error(field, value);
    }

    let Some(Bson::Document(document)) = value else {
        return type_error(field, value);
    };

    for (idx, child_field) in fields.iter().enumerate() {
        let child_builder = builder.field_builders_mut()[idx]
            .as_any_mut()
            .downcast_mut::<ColumnBuilder>()
            .ok_or_else(|| {
                DaftError::InternalError(format!(
                    "MongoDB struct child builder for field {} is not a ColumnBuilder",
                    child_field.name
                ))
            })?;
        child_builder.append(
            child_field,
            document_get_field(document, child_field.name.as_ref()),
        )?;
    }
    builder.append(true);
    Ok(())
}

fn append_null_struct_children(builder: &mut StructBuilder, fields: &[Field]) -> DaftResult<()> {
    for (idx, child_field) in fields.iter().enumerate() {
        let child_builder = builder.field_builders_mut()[idx]
            .as_any_mut()
            .downcast_mut::<ColumnBuilder>()
            .ok_or_else(|| {
                DaftError::InternalError(format!(
                    "MongoDB struct child builder for field {} is not a ColumnBuilder",
                    child_field.name
                ))
            })?;
        child_builder.append(child_field, None)?;
    }
    Ok(())
}

fn append_list(
    builder: &mut LargeListBuilder<Box<dyn ArrayBuilder>>,
    child_field: &Field,
    field: &Field,
    value: Option<&Bson>,
) -> DaftResult<()> {
    if matches!(value, None | Some(Bson::Null) | Some(Bson::Undefined)) {
        builder.append(false);
        return Ok(());
    }

    let Some(Bson::Array(values)) = value else {
        return type_error(field, value);
    };

    let child_builder = builder
        .values()
        .as_any_mut()
        .downcast_mut::<ColumnBuilder>()
        .ok_or_else(|| {
            DaftError::InternalError(format!(
                "MongoDB list child builder for field {} is not a ColumnBuilder",
                field.name
            ))
        })?;
    for value in values {
        child_builder.append(child_field, Some(value))?;
    }
    builder.append(true);
    Ok(())
}

fn bson_to_i8(value: &Bson) -> Option<i8> {
    bson_to_i64(value).and_then(|value| i8::try_from(value).ok())
}

fn bson_to_i16(value: &Bson) -> Option<i16> {
    bson_to_i64(value).and_then(|value| i16::try_from(value).ok())
}

fn bson_to_i32(value: &Bson) -> Option<i32> {
    match value {
        Bson::Int32(value) => Some(*value),
        Bson::Int64(value) => i32::try_from(*value).ok(),
        Bson::Double(value) if double_fits_i32(*value) => Some(*value as i32),
        _ => None,
    }
}

fn bson_to_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(i64::from(*value)),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if double_fits_i64(*value) => Some(*value as i64),
        _ => None,
    }
}

fn bson_to_u8(value: &Bson) -> Option<u8> {
    bson_to_u64(value).and_then(|value| u8::try_from(value).ok())
}

fn bson_to_u16(value: &Bson) -> Option<u16> {
    bson_to_u64(value).and_then(|value| u16::try_from(value).ok())
}

fn bson_to_u32(value: &Bson) -> Option<u32> {
    bson_to_u64(value).and_then(|value| u32::try_from(value).ok())
}

fn bson_to_u64(value: &Bson) -> Option<u64> {
    match value {
        Bson::Int32(value) => u64::try_from(*value).ok(),
        Bson::Int64(value) => u64::try_from(*value).ok(),
        Bson::Double(value) if double_fits_u64(*value) => Some(*value as u64),
        _ => None,
    }
}

fn bson_to_f32(value: &Bson) -> Option<f32> {
    match value {
        Bson::Int32(value) => Some(*value as f32),
        Bson::Int64(value) => Some(*value as f32),
        Bson::Double(value) if double_fits_f32(*value) => Some(*value as f32),
        _ => None,
    }
}

fn double_fits_i32(value: f64) -> bool {
    value.is_finite()
        && value.fract() == 0.0
        && value >= f64::from(i32::MIN)
        && value <= f64::from(i32::MAX)
}

fn double_fits_i64(value: f64) -> bool {
    value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value < 9_223_372_036_854_775_808.0
}

fn double_fits_u64(value: f64) -> bool {
    value.is_finite()
        && value.fract() == 0.0
        && (0.0..18_446_744_073_709_551_616.0).contains(&value)
}

fn double_fits_f32(value: f64) -> bool {
    value.is_nan()
        || value.is_infinite()
        || (value >= f64::from(f32::MIN) && value <= f64::from(f32::MAX))
}

fn epoch_millis_to_timeunit(millis: i64, unit: TimeUnit) -> DaftResult<i64> {
    match unit {
        TimeUnit::Seconds => Ok(millis.div_euclid(1_000)),
        TimeUnit::Milliseconds => Ok(millis),
        TimeUnit::Microseconds => millis.checked_mul(1_000).ok_or_else(|| {
            DaftError::ValueError(format!(
                "MongoDB timestamp millis value {millis} overflows microseconds"
            ))
        }),
        TimeUnit::Nanoseconds => millis.checked_mul(1_000_000).ok_or_else(|| {
            DaftError::ValueError(format!(
                "MongoDB timestamp millis value {millis} overflows nanoseconds"
            ))
        }),
    }
}

fn epoch_millis_to_date_days(millis: i64) -> DaftResult<i32> {
    let days = millis.div_euclid(86_400_000);
    i32::try_from(days).map_err(|_| {
        DaftError::ValueError(format!(
            "MongoDB date millis value {millis} overflows Daft date range"
        ))
    })
}

fn type_error<T>(field: &Field, value: Option<&Bson>) -> DaftResult<T> {
    Err(DaftError::TypeError(format!(
        "Cannot convert MongoDB value {value:?} for field {} to Daft dtype {}",
        field.name, field.dtype
    )))
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, Int64Array, LargeListArray, LargeStringArray, StructArray, TimestampMillisecondArray,
    };

    use super::*;

    #[test]
    fn converts_epoch_millis_to_date_days_with_flooring() {
        assert_eq!(epoch_millis_to_date_days(0).unwrap(), 0);
        assert_eq!(epoch_millis_to_date_days(86_399_999).unwrap(), 0);
        assert_eq!(epoch_millis_to_date_days(86_400_000).unwrap(), 1);
        assert_eq!(epoch_millis_to_date_days(-1).unwrap(), -1);
    }

    #[test]
    fn converts_epoch_millis_to_timestamp_units() {
        assert_eq!(
            epoch_millis_to_timeunit(1_234, TimeUnit::Seconds).unwrap(),
            1
        );
        assert_eq!(epoch_millis_to_timeunit(-1, TimeUnit::Seconds).unwrap(), -1);
        assert_eq!(
            epoch_millis_to_timeunit(1_234, TimeUnit::Milliseconds).unwrap(),
            1_234
        );
        assert_eq!(
            epoch_millis_to_timeunit(1_234, TimeUnit::Microseconds).unwrap(),
            1_234_000
        );
        assert_eq!(
            epoch_millis_to_timeunit(1_234, TimeUnit::Nanoseconds).unwrap(),
            1_234_000_000
        );
    }

    #[test]
    fn supports_null_schema_fields() {
        let field = Field::new("always_null", DataType::Null);
        let mut builder = ColumnBuilder::try_new(&field).unwrap();

        builder
            .append(&field, Some(&Bson::String("ignored".to_string())))
            .unwrap();

        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert!(array.as_any().is::<arrow_array::NullArray>());
    }

    #[test]
    fn converts_nested_bson_documents_to_struct_arrays() {
        let field = Field::new(
            "profile",
            DataType::Struct(vec![
                Field::new("name", DataType::Utf8),
                Field::new("score", DataType::Int64),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Milliseconds, None),
                ),
            ]),
        );
        let mut builder = ColumnBuilder::try_new(&field).unwrap();

        builder
            .append(
                &field,
                Some(&Bson::Document(doc! {
                    "name": "Ada",
                    "score": 7_i64,
                    "created_at": Bson::DateTime(mongodb::bson::DateTime::from_millis(1_234)),
                })),
            )
            .unwrap();
        builder.append(&field, None).unwrap();

        let array = builder.finish();
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.len(), 2);
        assert!(struct_array.is_null(1));
        assert_eq!(
            struct_array
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(0),
            "Ada"
        );
        assert_eq!(
            struct_array
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert_eq!(
            struct_array
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_234
        );
    }

    #[test]
    fn converts_object_id_to_spark_oid_struct_shape() {
        let field = Field::new(
            "_id",
            DataType::Struct(vec![Field::new("oid", DataType::Utf8)]),
        );
        let mut builder = ColumnBuilder::try_new(&field).unwrap();
        let object_id = ObjectId::parse_str("000000000000000000000001").unwrap();

        builder
            .append(&field, Some(&Bson::ObjectId(object_id)))
            .unwrap();

        let array = builder.finish();
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            struct_array
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(0),
            "000000000000000000000001"
        );
    }

    #[test]
    fn converts_bson_arrays_to_list_arrays() {
        let field = Field::new("tags", DataType::List(Box::new(DataType::Utf8)));
        let mut builder = ColumnBuilder::try_new(&field).unwrap();

        builder
            .append(
                &field,
                Some(&Bson::Array(vec![
                    Bson::String("new".to_string()),
                    Bson::String("paid".to_string()),
                ])),
            )
            .unwrap();
        builder.append(&field, Some(&Bson::Null)).unwrap();
        builder
            .append(
                &field,
                Some(&Bson::Array(vec![Bson::String("late".to_string())])),
            )
            .unwrap();

        let array = builder.finish();
        let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(list_array.len(), 3);
        assert!(list_array.is_null(1));
        assert_eq!(list_array.value_offsets(), &[0, 2, 2, 3]);
        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        assert_eq!(values.value(0), "new");
        assert_eq!(values.value(1), "paid");
        assert_eq!(values.value(2), "late");
    }

    #[test]
    fn converts_bson_arrays_of_documents_to_list_of_struct_arrays() {
        let field = Field::new(
            "events",
            DataType::List(Box::new(DataType::Struct(vec![
                Field::new("kind", DataType::Utf8),
                Field::new("at", DataType::Timestamp(TimeUnit::Milliseconds, None)),
            ]))),
        );
        let mut builder = ColumnBuilder::try_new(&field).unwrap();

        builder
            .append(
                &field,
                Some(&Bson::Array(vec![
                    Bson::Document(doc! {
                        "kind": "created",
                        "at": Bson::DateTime(mongodb::bson::DateTime::from_millis(100)),
                    }),
                    Bson::Document(doc! {
                        "kind": "updated",
                        "at": Bson::DateTime(mongodb::bson::DateTime::from_millis(200)),
                    }),
                ])),
            )
            .unwrap();

        let array = builder.finish();
        let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(list_array.value_offsets(), &[0, 2]);
        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(1),
            "updated"
        );
        assert_eq!(
            values
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(1),
            200
        );
    }

    #[test]
    fn rejects_non_integral_and_out_of_range_integer_doubles() {
        assert_eq!(bson_to_i32(&Bson::Double(1.0)), Some(1));
        assert_eq!(bson_to_i32(&Bson::Double(1.5)), None);
        assert_eq!(bson_to_i32(&Bson::Double(f64::INFINITY)), None);
        assert_eq!(bson_to_i32(&Bson::Int64(i64::from(i32::MAX) + 1)), None);
    }

    #[test]
    fn converts_narrow_and_unsigned_integer_types_with_range_checks() {
        assert_eq!(bson_to_i8(&Bson::Int32(127)), Some(127));
        assert_eq!(bson_to_i8(&Bson::Int32(128)), None);
        assert_eq!(bson_to_u8(&Bson::Int32(255)), Some(255));
        assert_eq!(bson_to_u8(&Bson::Int32(-1)), None);
        assert_eq!(bson_to_u64(&Bson::Double(42.0)), Some(42));
        assert_eq!(bson_to_u64(&Bson::Double(-1.0)), None);
        assert_eq!(bson_to_u64(&Bson::Double(1.5)), None);
    }

    #[test]
    fn converts_float32_with_range_checks() {
        assert_eq!(bson_to_f32(&Bson::Int32(42)), Some(42.0));
        assert_eq!(bson_to_f32(&Bson::Double(1.25)), Some(1.25));
        assert!(bson_to_f32(&Bson::Double(f64::INFINITY)).is_some());
        assert!(bson_to_f32(&Bson::Double(f64::from(f32::MAX) * 2.0)).is_none());
    }

    #[test]
    fn gets_direct_or_nested_document_fields() {
        let document = doc! {
            "user": { "id": 7 },
            "literal.dot": 9,
            "items": [{ "name": "first" }],
        };

        assert_eq!(
            document_get_field(&document, "user.id"),
            Some(&Bson::Int32(7))
        );
        assert_eq!(
            document_get_field(&document, "literal.dot"),
            Some(&Bson::Int32(9))
        );
        assert_eq!(
            document_get_field(&document, "items.0.name"),
            Some(&Bson::String("first".to_string()))
        );
        assert_eq!(document_get_field(&document, "user.missing"), None);
    }

    #[test]
    fn rejects_non_finite_extended_json_doubles() {
        assert_eq!(
            parse_bson_json(r#"{"$numberDouble":"1.25"}"#).unwrap(),
            Bson::Double(1.25)
        );
        assert!(parse_bson_json(r#"{"$numberDouble":"NaN"}"#).is_err());
        assert!(parse_bson_json(r#"{"$numberDouble":"Infinity"}"#).is_err());
    }

    #[test]
    fn rejects_non_canonical_extended_json_date_objects() {
        assert!(parse_bson_json(r#"{"$date":0,"extra":1}"#).is_err());
        assert!(parse_bson_json(r#"{"$date":{"$numberLong":"0","extra":1}}"#).is_err());
    }

    #[test]
    fn rejects_extended_json_objects_with_sibling_keys() {
        assert!(parse_bson_json(r#"{"$oid":"000000000000000000000000","extra":1}"#).is_err());
        assert!(parse_bson_json(r#"{"$numberLong":"1","extra":1}"#).is_err());
        assert!(parse_bson_json(r#"{"$numberInt":"1","extra":1}"#).is_err());
        assert!(parse_bson_json(r#"{"$numberDouble":"1.0","extra":1}"#).is_err());
        assert!(
            parse_bson_json(r#"{"$binary":{"base64":"aGVsbG8=","subType":"00"},"extra":1}"#)
                .is_err()
        );
    }

    #[test]
    fn parses_canonical_extended_json_binary() {
        assert_eq!(
            parse_bson_json(r#"{"$binary":{"base64":"aGVsbG8=","subType":"00"}}"#).unwrap(),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: b"hello".to_vec(),
            })
        );
        assert!(parse_bson_json(r#"{"$binary":{"base64":"not base64","subType":"00"}}"#).is_err());
        assert!(parse_bson_json(r#"{"$binary":{"base64":"aGVsbG8=","subType":"0"}}"#).is_err());
        assert!(
            parse_bson_json(r#"{"$binary":{"base64":"aGVsbG8=","subType":"00","extra":1}}"#)
                .is_err()
        );
    }

    #[test]
    fn rejects_json_integer_outside_i64_range() {
        assert_eq!(
            parse_bson_json("2147483647").unwrap(),
            Bson::Int32(i32::MAX)
        );
        assert_eq!(
            parse_bson_json("2147483648").unwrap(),
            Bson::Int64(i64::from(i32::MAX) + 1)
        );
        assert!(parse_bson_json("9223372036854775808").is_err());
    }

    #[test]
    fn empty_projected_schema_uses_id_only_projection() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            Some(r#"{"payload":0}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::empty());

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("_id"), Some(&Bson::Int32(1)));
    }

    #[test]
    fn projection_excludes_id_when_schema_does_not_need_it() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64)]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("value"), Some(&Bson::Int32(1)));
        assert_eq!(projection.get("_id"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn exclusion_projection_does_not_mix_in_schema_fields() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            Some(r#"{"payload":0}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64)]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("payload"), Some(&Bson::Int32(0)));
        assert_eq!(projection.get("value"), None);
        assert_eq!(projection.get("_id"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn projection_does_not_fetch_null_schema_fields() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64),
            Field::new("always_null", DataType::Null),
        ]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("value"), Some(&Bson::Int32(1)));
        assert_eq!(projection.get("always_null"), None);
    }

    #[test]
    fn projection_for_null_only_schema_uses_id_marker() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("always_null", DataType::Null)]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("_id"), Some(&Bson::Int32(1)));
        assert_eq!(projection.get("always_null"), None);
    }

    #[test]
    fn projection_honors_user_explicit_id_projection() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            Some(r#"{"_id":1,"value":1}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64)]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("_id"), Some(&Bson::Int32(1)));
    }

    #[test]
    fn projection_honors_user_explicit_id_exclusion() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            Some(r#"{"_id":0}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Utf8)]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert_eq!(projection.get("_id"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn projection_does_not_overwrite_user_field_projection() {
        let config = MongoSourceConfig::new_internal(
            "mongodb://localhost:27017".to_string(),
            "db".to_string(),
            "events".to_string(),
            None,
            Some(r#"{"_id":0,"items":{"$slice":5}}"#.to_string()),
            None,
            None,
            None,
            None,
            None,
            1_000,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("items", DataType::Utf8),
            Field::new("value", DataType::Int64),
        ]));

        let projection = projection_document(&config, &schema).unwrap().unwrap();

        assert!(matches!(projection.get("items"), Some(Bson::Document(_))));
        assert_eq!(projection.get("value"), Some(&Bson::Int32(1)));
        assert_eq!(projection.get("_id"), Some(&Bson::Int32(0)));
    }
}
