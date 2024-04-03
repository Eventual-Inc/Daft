use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

pub struct Sketch {
    sketch: DDSketch,
}

impl Sketch {
    pub fn new() -> Self {
        Sketch {
            sketch: DDSketch::new(Config::defaults()),
        }
    }

    pub fn add(&mut self, value: f64) -> &mut Self {
        self.sketch.add(value);
        self
    }

    pub fn merge(&mut self, other: &Sketch) -> DaftResult<&mut Self> {
        self.sketch.merge(&other.sketch)?;
        Ok(self)
    }

    pub fn quantile(&self, q: f64) -> DaftResult<Option<f64>> {
        Ok(self.sketch.quantile(q)?)
    }

    pub fn from_value(value: f64) -> Self {
        let mut sketch = Sketch::new();
        sketch.add(value);
        sketch
    }

    pub fn from_binary(binary: &[u8]) -> DaftResult<Self> {
        let sketch_str = std::str::from_utf8(binary)?;
        let sketch: DDSketch = serde_json::from_str(sketch_str)?;
        Ok(Sketch { sketch })
    }

    pub fn to_binary(&self) -> DaftResult<Vec<u8>> {
        let sketch_str = serde_json::to_string(&self.sketch)?;
        Ok(sketch_str.as_bytes().to_vec())
    }
}

impl Default for Sketch {
    fn default() -> Self {
        Self::new()
    }
}
