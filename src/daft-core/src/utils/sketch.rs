use common_error::{DaftError, DaftResult};
use sketches_ddsketch::{Config, DDSketch};

pub fn sketch_from_value(value: f64) -> DDSketch {
    let mut sketch = DDSketch::new(Config::defaults());
    sketch.add(value);
    sketch
}

pub fn sketch_merge(sketch: &mut DDSketch, other: &DDSketch) -> DaftResult<()> {
    let result = sketch.merge(other);

    match result {
        Ok(result) => Ok(result),
        Err(err) => Err(DaftError::ValueError(err.to_string())),
    }
}

pub fn sketch_to_binary(sketch: &DDSketch) -> DaftResult<Vec<u8>> {
    let sketch_str = serde_json::to_string(sketch);

    match sketch_str {
        Ok(s) => Ok(s.as_bytes().to_vec()),
        Err(err) => Err(DaftError::ValueError(err.to_string())),
    }
}

pub fn sketch_from_binary(binary: &[u8]) -> DaftResult<DDSketch> {
    let sketch_str = std::str::from_utf8(binary);
    match sketch_str {
        Ok(str) => {
            let sketch: DDSketch = serde_json::from_str(str)?;
            Ok(sketch)
        }
        Err(err) => Err(DaftError::ValueError(err.to_string())),
    }
}
