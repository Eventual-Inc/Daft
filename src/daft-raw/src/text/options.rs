use daft_core::prelude::SchemaRef;
use serde::{Deserialize, Serialize};

/// Options for converting text data to Daft data.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TextConvertOptions {
    pub encoding: String,
    pub skip_blank_lines: bool,
    pub whole_text: bool,
    pub schema: Option<SchemaRef>,
    pub limit: Option<usize>,
}

impl TextConvertOptions {
    #[must_use]
    pub fn new(
        encoding: &str,
        skip_blank_lines: bool,
        whole_text: bool,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            encoding: encoding.to_string(),
            skip_blank_lines,
            whole_text,
            schema,
            limit,
        }
    }
}

impl Default for TextConvertOptions {
    fn default() -> Self {
        Self::new("utf-8", true, false, None, None)
    }
}

/// Options for reading text files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TextReadOptions {
    /// Size of the buffer (in bytes) used by the streaming reader.
    pub buffer_size: Option<usize>,
    /// Size of the chunks (in rows) emitted by the streaming reader.
    pub chunk_size: Option<usize>,
}

impl TextReadOptions {
    #[must_use]
    pub fn new(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size,
        }
    }

    #[must_use]
    pub fn with_buffer_size(self, buffer_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            ..self
        }
    }

    #[must_use]
    pub fn with_chunk_size(self, chunk_size: Option<usize>) -> Self {
        Self { chunk_size, ..self }
    }
}

impl Default for TextReadOptions {
    fn default() -> Self {
        Self::new(None, None)
    }
}
