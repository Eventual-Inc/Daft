use std::sync::Arc;

use daft_common::checkpoint_config::CheckpointStoreConfig;

use crate::CheckpointStoreRef;

/// Construct a live checkpoint store from its serializable configuration.
pub fn build_store(config: &CheckpointStoreConfig) -> CheckpointStoreRef {
    match config {
        #[cfg(feature = "s3")]
        CheckpointStoreConfig::ObjectStore { prefix, io_config } => {
            use crate::impls::S3CheckpointStore;
            Arc::new(
                S3CheckpointStore::new(prefix.clone(), Arc::new(io_config.as_ref().clone()))
                    .expect("failed to build S3CheckpointStore"),
            )
        }
        _ => panic!(
            "build_store: unsupported CheckpointStoreConfig variant: {:?}",
            config
        ),
    }
}
