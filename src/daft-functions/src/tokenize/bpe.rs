use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
    num::ParseIntError,
    str::Utf8Error,
    sync::Arc,
};

use base64::{engine::general_purpose, DecodeError, Engine};
use common_error::{DaftError, DaftResult};
use daft_io::{get_io_client, get_runtime, IOConfig};
use snafu::prelude::*;
use snafu::Snafu;
use tiktoken_rs::CoreBPE;

use super::special_tokens::get_special_tokens;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Error decoding base 64 token {} with rank {}: {}",
        token,
        rank,
        source
    ))]
    Base64Decode {
        token: String,
        rank: String,
        source: DecodeError,
    },

    #[snafu(display("Error parsing rank number {}: {}", rank, source))]
    RankNumberParse { rank: String, source: ParseIntError },

    #[snafu(display("Invalid UTF-8 sequence in token file: {}", source))]
    InvalidUtf8Sequence { source: Utf8Error },

    #[snafu(display("Invalid line in token file: {}", line))]
    InvalidTokenLine { line: String },

    #[snafu(display("Token file has no tokens"))]
    EmptyTokenFile {},

    #[snafu(display("Error creating BPE: {}", err))]
    BPECreation { err: DynError },

    #[snafu(display("Input has bad token {}", token))]
    BadToken { token: u32 },

    #[snafu(display("Error decoding tokens: {}", err))]
    Decode { err: DynError },

    #[snafu(display("Pattern must be provided for non-builtin token sets"))]
    MissingPattern {},

    #[snafu(display("Provided special token set is not supported: {}", name))]
    UnsupportedSpecialTokens { name: String },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> Self {
        use Error::*;
        match err {
            Base64Decode { .. } => DaftError::ValueError(err.to_string()),
            RankNumberParse { .. } => DaftError::ValueError(err.to_string()),
            InvalidUtf8Sequence { .. } => DaftError::ValueError(err.to_string()),
            InvalidTokenLine { .. } => DaftError::ValueError(err.to_string()),
            EmptyTokenFile {} => DaftError::ValueError(err.to_string()),
            BPECreation { .. } => DaftError::ComputeError(err.to_string()),
            BadToken { .. } => DaftError::ValueError(err.to_string()),
            Decode { .. } => DaftError::ComputeError(err.to_string()),
            MissingPattern {} => DaftError::ValueError(err.to_string()),
            UnsupportedSpecialTokens { .. } => DaftError::ValueError(err.to_string()),
        }
    }
}

// Wrapper around a tiktoken-rs CoreBPE for storing token data.
pub struct DaftBPE {
    bpe: CoreBPE,

    // the maximum of non-special token ids
    // assumed that all tokens in range 0..=max_token_id are valid
    max_token_id: u32,

    // list of special tokens, used for checking token validity
    specials: HashSet<u32>,
}

// Fetch a BPE from the builtin tiktoken-rs ones
fn get_builtin_bpe(name: &str) -> Option<DaftBPE> {
    match name {
        "cl100k_base" => Some(DaftBPE {
            bpe: tiktoken_rs::cl100k_base().unwrap(),
            max_token_id: 100255,
            specials: HashSet::from([100257, 100258, 100259, 100260, 100276]),
        }),
        "o200k_base" => Some(DaftBPE {
            bpe: tiktoken_rs::o200k_base().unwrap(),
            max_token_id: 199997,
            specials: HashSet::from([199999, 200018]),
        }),
        "p50k_base" => Some(DaftBPE {
            bpe: tiktoken_rs::p50k_base().unwrap(),
            max_token_id: 50280,
            specials: HashSet::from([50256]),
        }),
        "p50k_edit" => Some(DaftBPE {
            bpe: tiktoken_rs::p50k_edit().unwrap(),
            max_token_id: 50280,
            specials: HashSet::from([50256, 50281, 50282, 50283]),
        }),
        "r50k_base" => Some(DaftBPE {
            bpe: tiktoken_rs::r50k_base().unwrap(),
            max_token_id: 50255,
            specials: HashSet::from([50256]),
        }),
        _ => None,
    }
}

// This function is templated because tiktoken-rs uses a special hasher for the HashMap.
fn parse_tokens<H>(s: &str) -> DaftResult<HashMap<Vec<u8>, usize, H>>
where
    H: BuildHasher + Default,
{
    s.lines()
        .map(|l| match l.split(' ').take(2).collect::<Vec<&str>>()[..] {
            [token, rank] => {
                let token = general_purpose::STANDARD.decode(token).with_context(|_| {
                    Base64DecodeSnafu {
                        token: token.to_string(),
                        rank: rank.to_string(),
                    }
                })?;
                let rank: u32 = rank.parse().with_context(|_| RankNumberParseSnafu {
                    rank: rank.to_string(),
                })?;
                Ok((token, rank as usize))
            }
            _ => Err(Error::InvalidTokenLine {
                line: l.to_string(),
            }
            .into()),
        })
        .collect::<DaftResult<HashMap<Vec<u8>, usize, H>>>()
}

fn get_file_bpe(
    path: &str,
    io_config: Arc<IOConfig>,
    pattern: &str,
    special_tokens: Vec<String>,
) -> DaftResult<DaftBPE> {
    // Fetch the token file as a string
    let client = get_io_client(false, io_config)?;
    let runtime = get_runtime(false)?;
    let get_future = client.single_url_get(path.to_string(), None, None);
    let get_res = runtime.block_on(get_future)?;
    let file_bytes = runtime.block_on(get_res.bytes())?;
    let file_str = std::str::from_utf8(&file_bytes).with_context(|_| InvalidUtf8SequenceSnafu)?;

    let tokens_res = parse_tokens(file_str)?;
    let max_token = *tokens_res.values().max().ok_or(Error::EmptyTokenFile {})?;

    // Get the token->id mappings for special tokens
    let mut special_hashmap = HashMap::default();
    let mut special_hashset = HashSet::default();
    for (i, token) in special_tokens.into_iter().enumerate() {
        special_hashmap.insert(token, max_token + 1 + i);
        special_hashset.insert((max_token + 1 + i) as u32);
    }

    let core_bpe = CoreBPE::new(tokens_res, special_hashmap, pattern).map_err(|e| {
        // e is anyhow::Error and I don't want to add an anyhow dependency
        Error::BPECreation { err: e.into() }
    })?;
    Ok(DaftBPE {
        bpe: core_bpe,
        max_token_id: max_token as u32,
        specials: special_hashset,
    })
}

impl DaftBPE {
    pub fn new(
        tokens_path: &str,
        io_config: Option<Arc<IOConfig>>,
        pattern: Option<&str>,
        special_tokens: Option<&str>,
    ) -> DaftResult<Self> {
        if let Some(bpe) = get_builtin_bpe(tokens_path) {
            return Ok(bpe);
        }

        let special_tokens = if let Some(special_tokens_name) = special_tokens {
            get_special_tokens(special_tokens_name).ok_or(Error::UnsupportedSpecialTokens {
                name: special_tokens_name.to_string(),
            })?
        } else {
            Vec::new()
        };

        if let Some(pattern) = pattern {
            get_file_bpe(
                tokens_path,
                io_config.unwrap_or_default(),
                pattern,
                special_tokens,
            )
        } else {
            Err(Error::MissingPattern {}.into())
        }
    }

    // use u32s because there shouldn't be tokens > 4 billion
    // (and other libraries use u32)
    pub fn encode(&self, s: &str, use_special: bool) -> Vec<u32> {
        let encode_res = if use_special {
            self.bpe.encode_with_special_tokens(s)
        } else {
            self.bpe.encode_ordinary(s)
        };
        encode_res.into_iter().map(|x| x as u32).collect()
    }

    pub fn decode(&self, tokens: &[u32]) -> DaftResult<String> {
        // invalid token check
        if let Some(&bad_token) = tokens
            .iter()
            .find(|&&x| x > self.max_token_id || self.specials.contains(&x))
        {
            Err(Error::BadToken { token: bad_token }.into())
        } else {
            let casted_tokens = tokens.iter().map(|x| *x as usize).collect::<Vec<usize>>();
            self.bpe
                .decode(casted_tokens)
                .map_err(|err| Error::Decode { err: err.into() }.into())
        }
    }
}
