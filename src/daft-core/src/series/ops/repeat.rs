use common_error::DaftResult;
use itertools::Itertools;

use crate::series::Series;

impl Series {
    pub(crate) fn repeat(&self, n: usize) -> DaftResult<Self> {
        let many_self = std::iter::repeat(self).take(n).collect_vec();
        Self::concat(&many_self)
    }
}
