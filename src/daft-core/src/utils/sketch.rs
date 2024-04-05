use common_error::DaftResult;
use sketches_ddsketch::{Config, DDSketch};

pub struct Sketch(DDSketch);

impl Sketch {
    pub fn new() -> Self {
        Sketch(DDSketch::new(Config::defaults()))
    }

    pub fn add(&mut self, value: f64) -> &mut Self {
        self.0.add(value);
        self
    }

    pub fn merge(&mut self, other: &Sketch) -> DaftResult<&mut Self> {
        self.0.merge(&other.0)?;
        Ok(self)
    }

    pub fn quantile(&self, q: f64) -> DaftResult<Option<f64>> {
        Ok(self.0.quantile(q)?)
    }

    pub fn from_binary(binary: &[u8]) -> DaftResult<Self> {
        Ok(Sketch(bincode::deserialize(binary)?))
    }

    pub fn to_binary(&self) -> DaftResult<Vec<u8>> {
        Ok(bincode::serialize(&self.0)?)
    }
}

impl Default for Sketch {
    fn default() -> Self {
        Self::new()
    }
}
