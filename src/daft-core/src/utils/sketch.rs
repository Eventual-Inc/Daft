use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

pub struct Sketch {
    ddsketch: DDSketch,
}

impl Sketch {
    pub fn new() -> Self {
        Sketch {
            ddsketch: DDSketch::new(Config::defaults()),
        }
    }

    pub fn add(&mut self, value: f64) -> &mut Self {
        self.ddsketch.add(value);
        self
    }

    pub fn merge(&mut self, other: &Sketch) -> DaftResult<&mut Self> {
        self.ddsketch.merge(&other.ddsketch)?;
        Ok(self)
    }

    pub fn quantile(&self, q: f64) -> DaftResult<Option<f64>> {
        Ok(self.ddsketch.quantile(q)?)
    }

    pub fn from_value(value: f64) -> Sketch {
        let mut sketch = Sketch::new();
        sketch.add(value);
        sketch
    }

    pub fn from_binary(binary: &[u8]) -> DaftResult<Sketch> {
        let sketch_str = std::str::from_utf8(binary)?;
        Ok(Sketch {
            ddsketch: serde_json::from_str(sketch_str)?,
        })
    }

    pub fn to_binary(&self) -> DaftResult<Vec<u8>> {
        let sketch_str = serde_json::to_string(&self.ddsketch)?;
        Ok(sketch_str.as_bytes().to_vec())
    }
}

impl Default for Sketch {
    fn default() -> Self {
        Self::new()
    }
}
