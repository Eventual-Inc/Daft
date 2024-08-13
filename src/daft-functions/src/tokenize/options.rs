use daft_io::IOConfig;

type TokenIdType = u32;

#[derive(Default)]
pub struct SpecialTokenOptions {
    pub bos: Option<TokenIdType>,
    pub eos: Option<TokenIdType>,
    pub pad: Option<TokenIdType>,
    pub unk: Option<TokenIdType>,
    pub additional: Option<Vec<(String, TokenIdType)>>,
}

pub struct TokenizerOptions {
    pub io_config: IOConfig,
    pub source: String,
    pub special_tokens: SpecialTokenOptions,
    pub tiktoken_pattern: Option<String>,
    pub use_special_tokens: bool,
}

impl Default for TokenizerOptions {
    fn default() -> Self {
        Self {
            io_config: Default::default(),
            source: "auto".to_string(),
            special_tokens: Default::default(),
            tiktoken_pattern: None,
            use_special_tokens: true,
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use std::collections::HashMap;

    use daft_io::python::IOConfig as PyIOConfig;
    use pyo3::prelude::*;

    use super::{SpecialTokenOptions, TokenizerOptions};

    macro_rules! dict_extract {
        ($dict:ident, $py:expr, $key:expr) => {
            $dict.get($key).map(|o| o.extract($py)).transpose()?
        };
    }

    impl SpecialTokenOptions {
        pub fn from_pyobj(py: Python, dict: &PyObject) -> PyResult<Self> {
            let dict: HashMap<String, PyObject> = dict.extract(py)?;
            Ok(Self {
                bos: dict_extract!(dict, py, "bos"),
                eos: dict_extract!(dict, py, "eos"),
                pad: dict_extract!(dict, py, "pad"),
                unk: dict_extract!(dict, py, "unk"),
                additional: dict_extract!(dict, py, "additional"),
            })
        }
    }

    impl TokenizerOptions {
        fn from_pyobj(py: Python, dict: &PyObject) -> PyResult<Self> {
            let dict: HashMap<String, PyObject> = dict.extract(py)?;
            let special_tokens: Option<SpecialTokenOptions> = dict
                .get("special_tokens")
                .map(|o| SpecialTokenOptions::from_pyobj(py, o))
                .transpose()?;
            Ok(Self {
                io_config: dict_extract!(dict, py, "io_config")
                    .map(|x: PyIOConfig| x.config)
                    .unwrap_or_default(),
                source: dict_extract!(dict, py, "source").unwrap_or("auto".to_string()),
                special_tokens: special_tokens.unwrap_or_default(),
                tiktoken_pattern: dict_extract!(dict, py, "tiktoken_pattern"),
                use_special_tokens: dict_extract!(dict, py, "use_special_tokens").unwrap_or(true),
            })
        }
    }
}
