use std::collections::{BTreeMap, HashMap};

use common_error::{DaftError, DaftResult};
use daft_arrow::datatypes::Schema as ArrowSchema;
use daft_schema::schema::Schema;
use lance::{
    Dataset,
    dataset::{ReadParams, builder::DatasetBuilder},
    datatypes::Schema as LanceSchema,
    io::ObjectStoreParams,
};

pub fn to_daft_schema(schema: &LanceSchema) -> Schema {
    let schema: arrow_schema::Schema = schema.into();
    let schema: ArrowSchema = schema.into();
    schema.into()
}

pub async fn try_get_fragment_ids(ds: &Dataset, filter: Option<String>) -> DaftResult<Vec<usize>> {
    let fragments = ds.get_fragments();
    let mut fragment_ids = vec![];
    for fragment in fragments {
        let count = fragment
            .count_rows(filter.clone())
            .await
            .map_err(|e| DaftError::External(Box::new(e)))?;
        if count > 0 {
            fragment_ids.push(fragment.id());
        }
    }

    Ok(fragment_ids)
}

#[allow(clippy::too_many_arguments)]
pub async fn try_open_dataset(
    uri: &str,
    version: Option<u64>,
    tag: Option<String>,
    block_size: Option<usize>,
    index_cache_size: Option<usize>,
    metadata_cache_size: Option<usize>,
    // commit_handler: Option<PyObject>, FIXME by zhenchao
    storage_options: Option<BTreeMap<String, String>>,
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

    //
    // if let Some(commit_handler) = commit_handler {
    //     let py_commit_lock = PyCommitLock::new(commit_handler);
    //     params.set_commit_lock(Arc::new(py_commit_lock));
    // }

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

        let storage_options: HashMap<String, String> = storage_options.into_iter().collect();
        builder = builder.with_storage_options(storage_options);
    }

    builder
        .load()
        .await
        .map_err(|e| DaftError::External(Box::new(e)))
}
