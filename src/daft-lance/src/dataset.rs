use std::{
    collections::HashMap,
    env,
    sync::{Arc, OnceLock},
};

use common_error::{DaftError, DaftResult};
use daft_arrow::datatypes::Schema as ArrowSchema;
use daft_schema::schema::Schema;
use lance::{
    Dataset,
    dataset::{ReadParams, builder::DatasetBuilder},
    datatypes::Schema as LanceSchema,
    io::ObjectStoreParams,
};
use moka::future::Cache;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct DatasetCacheKey {
    uri: String,
    version: Option<u64>,
    tag: Option<String>,
}

type DatasetCache = Cache<DatasetCacheKey, Arc<Dataset>>;

static DEFAULT_DATASET_CACHE_CAPACITY: u64 = 16;

static DATASET_CACHE: OnceLock<DatasetCache> = OnceLock::new();

fn get_or_init_dataset_cache() -> &'static DatasetCache {
    DATASET_CACHE.get_or_init(|| {
        let max_capacity = env::var("DAFT_LANCE_DATASET_CACHE_CAPACITY")
            .ok()
            .and_then(|val| val.parse::<u64>().ok())
            .unwrap_or(DEFAULT_DATASET_CACHE_CAPACITY);

        Cache::builder()
            .name("lance-dataset-cache")
            .max_capacity(max_capacity)
            .build()
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn try_open_dataset(
    uri: &str,
    version: Option<u64>,
    tag: Option<String>,
    block_size: Option<usize>,
    index_cache_size: Option<usize>,
    metadata_cache_size: Option<usize>,
    storage_options: Option<HashMap<String, String>>,
    force_open: bool,
) -> DaftResult<Arc<Dataset>> {
    if force_open {
        do_open_dataset(
            uri,
            version,
            tag,
            block_size,
            index_cache_size,
            metadata_cache_size,
            storage_options,
        )
        .await
        .map(Arc::new)
    } else {
        let cache_key = DatasetCacheKey {
            uri: uri.to_string(),
            version,
            tag: tag.clone(),
        };
        let cache = get_or_init_dataset_cache();
        cache
            .try_get_with(cache_key, async move {
                do_open_dataset(
                    uri,
                    version,
                    tag,
                    block_size,
                    index_cache_size,
                    metadata_cache_size,
                    storage_options,
                )
                .await
                .map(Arc::new)
            })
            .await
            .map_err(|e| DaftError::External(Box::new(e)))
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_open_dataset(
    uri: &str,
    version: Option<u64>,
    tag: Option<String>,
    block_size: Option<usize>,
    index_cache_size: Option<usize>,
    metadata_cache_size: Option<usize>,
    storage_options: Option<HashMap<String, String>>,
) -> DaftResult<Dataset> {
    let mut params = ReadParams {
        store_options: Some(ObjectStoreParams {
            block_size,
            ..Default::default()
        }),
        ..Default::default()
    };

    if let Some(index_cache_size) = index_cache_size {
        params.index_cache_size = index_cache_size;
    }

    if let Some(metadata_cache_size) = metadata_cache_size {
        params.metadata_cache_size = metadata_cache_size;
    }

    let mut builder: DatasetBuilder = DatasetBuilder::from_uri(uri).with_read_params(params);
    builder = match (version, tag) {
        (Some(v), None) => builder.with_version(v),
        (None, Some(t)) => builder.with_tag(t.as_str()),
        (None, None) => builder,
        (Some(_), Some(_)) => {
            return Err(DaftError::ValueError(
                "Lance version and tag cannot be specified at the same time.".to_string(),
            ));
        }
    };

    if let Some(mut storage_options) = storage_options {
        if let Some(user_agent) = storage_options.get_mut("user_agent") {
            user_agent.push_str(format!(" daft/{}", env!("CARGO_PKG_VERSION")).as_str());
        } else {
            storage_options.insert(
                "user_agent".to_string(),
                format!("daft/{}", env!("CARGO_PKG_VERSION")),
            );
        }

        builder = builder.with_storage_options(storage_options);
    }

    builder
        .load()
        .await
        .map_err(|e| DaftError::External(Box::new(e)))
}

pub fn to_daft_schema(schema: &LanceSchema) -> Schema {
    let schema: arrow_schema::Schema = schema.into();
    let schema: ArrowSchema = schema.into();
    schema.into()
}
