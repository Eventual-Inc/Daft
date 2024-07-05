use std::{pin::Pin, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::Stream;

pub trait Source: Send + Sync + dyn_clone::DynClone {
    fn get_data(&self) -> Pin<Box<dyn Stream<Item = DaftResult<Arc<MicroPartition>>> + Send>>;
}

dyn_clone::clone_trait_object!(Source);
