use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher,
    sync::Arc,
};

use base64::{engine::general_purpose, Engine};
use common_error::{DaftError, DaftResult};
use daft_io::{get_io_client, get_runtime, IOConfig};
use tiktoken_rs::CoreBPE;

// Wrapper around a tiktoken-rs CoreBPE for storing token data.
pub struct DaftBPE {
    bpe: CoreBPE,

    // the maximum of non-special token ids
    // assumed that all tokens in range 0..=max_token_id are valid
    max_token_id: u32,

    // list of special tokens, used for checking token validity
    specials: HashSet<u32>,
}

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

fn parse_tokens<H>(s: &str) -> DaftResult<HashMap<Vec<u8>, usize, H>>
where
    H: BuildHasher + Default,
{
    s.lines()
        .map(|l| match l.split(' ').collect::<Vec<&str>>()[..2] {
            [token, rank] => {
                let token = general_purpose::STANDARD.decode(token).map_err(|e| {
                    DaftError::ValueError(format!("Error decoding token {}: {}", token, e))
                })?;
                let rank: u32 = rank.parse().map_err(|e| {
                    DaftError::ValueError(format!("Error parsing rank number: {}", e))
                })?;
                Ok((token, rank as usize))
            }
            _ => Err(DaftError::ValueError(format!(
                "Invalid line in token file: \"{}\"",
                l
            ))),
        })
        .collect::<DaftResult<HashMap<Vec<u8>, usize, H>>>()
}

fn get_file_bpe(path: &str, io_config: Arc<IOConfig>) -> DaftResult<DaftBPE> {
    let client = get_io_client(false, io_config)?;
    let runtime = get_runtime(false)?;
    let get_future = client.single_url_get(path.to_string(), None, None);
    let get_res = runtime.block_on(get_future)?;
    let file_bytes = runtime.block_on(get_res.bytes())?;
    let file_str = std::str::from_utf8(&file_bytes).map_err(|e| {
        DaftError::ValueError(format!("Invalid UTF-8 sequence in token file: {}", e))
    })?;

    let tokens_res = parse_tokens(file_str)?;
    let max_token = *tokens_res
        .values()
        .max()
        .ok_or(DaftError::ValueError("Tokens file has no tokens".into()))?;
    // TODO: pass in pattern as an argument
    let core_bpe = CoreBPE::new(
        tokens_res,
        HashMap::default(),
        "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)|\\s+",
    )
    .map_err(|e| DaftError::ComputeError(format!("Error creating BPE: {}", e)))?;
    Ok(DaftBPE {
        bpe: core_bpe,
        max_token_id: max_token as u32,
        specials: HashSet::default(),
    })
}

impl DaftBPE {
    pub fn new(tokens_path: &str, io_config: Arc<IOConfig>) -> DaftResult<Self> {
        if let Some(bpe) = get_builtin_bpe(tokens_path) {
            return Ok(bpe);
        }

        get_file_bpe(tokens_path, io_config)
    }

    // use u32s because surely there won't be tokens > 4 billion...
    pub fn encode(&self, s: &str) -> Vec<u32> {
        let encode_res = self.bpe.encode_ordinary(s);
        encode_res.into_iter().map(|x| x as u32).collect()
    }

    pub fn decode(&self, tokens: &[u32]) -> DaftResult<String> {
        // invalid token check
        if let Some(&bad_token) = tokens
            .iter()
            .find(|&&x| x > self.max_token_id || self.specials.contains(&x))
        {
            Err(DaftError::ValueError(format!(
                "Input has token that is invalid with this tokenizer: {}",
                bad_token
            )))
        } else {
            let casted_tokens = tokens.iter().map(|x| *x as usize).collect::<Vec<usize>>();
            self.bpe
                .decode(casted_tokens)
                .map_err(|err| DaftError::ValueError(format!("Failed to decode tokens: {}", err)))
        }
    }
}
