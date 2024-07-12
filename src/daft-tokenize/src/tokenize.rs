use std::{collections::HashSet, str::FromStr};

use common_error::{DaftError, DaftResult};
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

impl FromStr for DaftBPE {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // check builtin bpes
        let builtin_bpe = match s {
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
        };
        if let Some(bpe) = builtin_bpe {
            return Ok(bpe);
        }

        Err(DaftError::ValueError(format!("tokenizer not found: {}", s)))
    }
}

impl DaftBPE {
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
