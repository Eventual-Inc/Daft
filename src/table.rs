use std::sync::Arc;

use crate::schema::Schema;
use crate::series::Series;

pub struct Table {
    schema: Arc<Schema>,
    columns: Vec<Series>,
}
