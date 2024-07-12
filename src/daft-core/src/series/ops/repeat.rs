use crate::series::Series;

use common_error::DaftResult;
use itertools::Itertools;

impl Series {
    pub(crate) fn repeat(&self, n: usize) -> DaftResult<Self> {
        let many_self = std::iter::repeat(self).take(n).collect_vec();
        Series::concat(&many_self)
    }
}
