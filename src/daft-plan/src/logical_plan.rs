use daft_core::schema::SchemaRef;

use crate::ops::*;

#[derive(Clone, Debug)]
pub enum LogicalPlan {
    Source(Source),
    Filter(Filter),
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Source(Source { schema, .. }) => schema.clone(),
            Self::Filter(Filter { input, .. }) => input.schema(),
        }
    }

    pub fn children(&self) -> Vec<&Self> {
        match self {
            Self::Source(..) => vec![],
            Self::Filter(filter) => vec![&filter.input],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Source(source) => source.multiline_display(),
            Self::Filter(Filter { predicate, .. }) => vec![format!("Filter: {predicate}")],
        }
    }

    pub fn repr_ascii(&self) -> String {
        let mut s = String::new();
        crate::display::TreeDisplay::fmt_tree(self, &mut s).unwrap();
        s
    }
}
