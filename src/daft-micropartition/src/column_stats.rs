use common_error::DaftResult;
use daft_core::{
    array::ops::{DaftCompare, DaftLogical},
    IntoSeries, Series,
};

struct ColumnStatistics {
    lower: Series,
    upper: Series,
    count: usize,
    null_count: usize,
    num_bytes: usize,
}

enum TruthValue {
    Never,
    Sometimes,
    Always,
}

impl ColumnStatistics {
    fn as_truth_value(&self) -> TruthValue {
        let lower = self.lower.bool().unwrap().get(0).unwrap();
        let upper = self.upper.bool().unwrap().get(0).unwrap();
        match (lower, upper) {
            (false, false) => TruthValue::Never,
            (false, true) => TruthValue::Sometimes,
            (true, true) => TruthValue::Always,
            (true, false) => panic!("Upper is false and lower is true; Invalid states!"),
        }
    }
}

impl std::fmt::Display for ColumnStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ColumnStatistics:
lower:\n{}
upper:\n{}
count: {}
null_count: {}
num_bytes: {}
",
            self.lower, self.upper, self.count, self.null_count, self.num_bytes
        )
    }
}

impl std::ops::Add for &ColumnStatistics {
    type Output = ColumnStatistics;
    fn add(self, rhs: Self) -> Self::Output {
        ColumnStatistics {
            lower: ((&self.lower) + &rhs.lower).unwrap(),
            upper: ((&self.upper) + &rhs.upper).unwrap(),
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
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
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        }
    }
}

impl DaftCompare<&ColumnStatistics> for ColumnStatistics {
    type Output = ColumnStatistics;
    fn equal(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: do they exactly overlap
        // upper_bound: is there any overlap

        let exactly_overlap = self
            .lower
            .equal(&rhs.lower)
            .unwrap()
            .and(&self.upper.equal(&rhs.upper).unwrap())
            .unwrap()
            .into_series();
        let self_lower_in_rhs_bounds = self
            .lower
            .gte(&rhs.lower)
            .unwrap()
            .and(&self.lower.lte(&rhs.upper).unwrap())
            .unwrap();
        let rhs_lower_in_self_bounds = rhs
            .lower
            .gte(&self.lower)
            .unwrap()
            .and(&rhs.lower.lte(&self.upper).unwrap())
            .unwrap();
        let any_overlap = self_lower_in_rhs_bounds
            .or(&rhs_lower_in_self_bounds)
            .unwrap()
            .into_series();
        ColumnStatistics {
            lower: exactly_overlap,
            upper: any_overlap,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        }
    }
    fn not_equal(&self, rhs: &ColumnStatistics) -> Self::Output {
        // invert of equal
        todo!()
    }
    fn gt(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: some value that can be greater (self.upper > rhs.lower)
        // upper_bound: always greater (self.lower > rhs.upper)
        let sometimes_greater = self.upper.gt(&rhs.lower).unwrap().into_series();
        let always_greater = self.lower.gt(&rhs.upper).unwrap().into_series();
        ColumnStatistics {
            lower: sometimes_greater,
            upper: always_greater,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        }
    }
    fn gte(&self, rhs: &ColumnStatistics) -> Self::Output {
        todo!()
    }
    fn lt(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: some value that can be less than (self.lower < rhs.upper)
        // upper_bound: always less than (self.upper < rhs.lower)
        let sometimes_lt = self.lower.lt(&self.upper).unwrap().into_series();
        let always_lt = self.upper.lt(&self.lower).unwrap().into_series();
        ColumnStatistics {
            lower: sometimes_lt,
            upper: always_lt,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        }
    }
    fn lte(&self, rhs: &ColumnStatistics) -> Self::Output {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries, Series,
    };

    use super::ColumnStatistics;

    #[test]
    fn test_equal() -> DaftResult<()> {
        let l = ColumnStatistics {
            lower: Int64Array::from(("l", vec![1])).into_series(),
            upper: Int64Array::from(("l", vec![5])).into_series(),
            count: 1,
            null_count: 0,
            num_bytes: 8,
        };
        let r = ColumnStatistics {
            lower: Int32Array::from(("r", vec![4])).into_series(),
            upper: Int32Array::from(("r", vec![6])).into_series(),
            count: 1,
            null_count: 0,
            num_bytes: 8,
        };
        println!("{l}");
        println!("{r}");
        println!("{}", l.equal(&r));

        Ok(())
    }
}
