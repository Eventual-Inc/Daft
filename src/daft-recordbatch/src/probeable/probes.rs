use smallvec::SmallVec;

pub trait ProbeContent: Default + Send + Sync + 'static {
    type ProbeOutput<'a>;

    fn add_row(&mut self, idx: u64);
    fn probe_out(&self) -> Self::ProbeOutput<'_>;

    /// Convert the probe output to indices for probe_indices
    fn to_indices(output: Option<Self::ProbeOutput<'_>>) -> Option<&[u64]>;

    /// Convert the probe output to boolean for probe_exists
    fn to_exists(output: Option<Self::ProbeOutput<'_>>) -> bool;
}

/// For set / exist ops
#[derive(Default)]
pub struct ProbeExists;

impl ProbeContent for ProbeExists {
    type ProbeOutput<'a> = ();

    fn add_row(&mut self, _: u64) {}
    fn probe_out(&self) -> Self::ProbeOutput<'_> {}

    fn to_indices(_output: Option<Self::ProbeOutput<'_>>) -> Option<&[u64]> {
        panic!("to_indices should not be called on ProbeExists")
    }

    fn to_exists(_output: Option<Self::ProbeOutput<'_>>) -> bool {
        true
    }
}

/// For map / indices ops
#[derive(Default)]
pub struct ProbeIndices(SmallVec<[u64; 2]>);

impl ProbeContent for ProbeIndices {
    type ProbeOutput<'a> = &'a [u64];

    fn add_row(&mut self, idx: u64) {
        self.0.push(idx);
    }

    fn probe_out(&self) -> Self::ProbeOutput<'_> {
        self.0.as_slice()
    }

    fn to_indices(output: Option<Self::ProbeOutput<'_>>) -> Option<&[u64]> {
        output
    }

    fn to_exists(output: Option<Self::ProbeOutput<'_>>) -> bool {
        output.is_some()
    }
}
