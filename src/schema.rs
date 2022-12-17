use indexmap::IndexMap;
use std::sync::Arc;

use crate::field::Field;

type SchemaRef = Arc<Schema>;
pub struct Schema {
    fields: indexmap::IndexMap<String, Field>,
}
