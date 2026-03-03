#[derive(Clone, Copy, Debug)]
pub struct BinaryConvertOptions {
    pub include_bytes: bool,
    pub limit: Option<usize>,
}

impl BinaryConvertOptions {
    #[must_use]
    pub fn new_internal(include_bytes: bool, limit: Option<usize>) -> Self {
        Self {
            include_bytes,
            limit,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BinaryReadOptions {
    pub buffer_size: Option<usize>,
    pub max_bytes: Option<usize>,
}

impl BinaryReadOptions {
    #[must_use]
    pub fn new_internal(buffer_size: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            buffer_size,
            max_bytes,
        }
    }
}
