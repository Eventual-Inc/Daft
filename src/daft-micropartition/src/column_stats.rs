use daft_core::Series;

struct ColumnStatistics {
    lower: Series,
    upper: Series,
    count: usize,
    null_count: usize,
    num_bytes: usize
}


impl std::ops::Add for &ColumnStatistics {
    type Output = ColumnStatistics;
    fn add(self, rhs: Self) -> Self::Output {
        ColumnStatistics {
            lower: ((&self.lower) + &rhs.lower).unwrap(),
            upper: ((&self.upper) + &rhs.upper).unwrap(),
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes)
        }
    }
}

impl std::ops::Sub for &ColumnStatistics {
    type Output = ColumnStatistics;
    fn sub(self, rhs: Self) -> Self::Output {
        ColumnStatistics {
            lower: ((&self.lower) - &rhs.upper).unwrap(),
            upper: ((&self.upper) - &rhs.lower).unwrap(),
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes)
        }
    }
}