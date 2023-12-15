use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
    num::NonZeroUsize,
};

use crate::LogicalPlan;

/// A logical plan tracker that uses logical plan digests to detect optimization cycles.
///
/// The digests are cheaply hashable + comparable, and have the following guarantees:
///
///   hash(digest(p1)) != hash(digest(p2)) -> digest(p1) != digest(p2) -> p1 != p2
///   hash(p1) != hash(p2) -> hash(digest(p1)) != hash(digest(p2))
///
/// These guarantees allow us to use a HashSet of such digests as a cheap cycle detector in
/// our optimizer, with the negligible possibility of plan hash + node count collisions leading
/// to "already seen" false positives and too-early optimization termination. Cycle detection,
/// however, is guaranteed.
pub struct LogicalPlanTracker {
    // A set of the initial unoptimized plan and plans from all optimization passes.
    past_plans: HashSet<LogicalPlanDigest>,
    // A Hasher builder that's used to generate new Hashers for hashing logical plans.
    // We need to use a new hasher when hashing each logical plan in order to see the same hash
    // for the same logical plan.
    hasher_builder: BuildHasherDefault<DefaultHasher>,
}

impl LogicalPlanTracker {
    pub fn new(capacity: usize) -> Self {
        Self {
            past_plans: HashSet::with_capacity(capacity),
            hasher_builder: Default::default(),
        }
    }

    pub fn add_plan(&mut self, plan: &LogicalPlan) -> bool {
        self.past_plans.insert(LogicalPlanDigest::new(
            plan,
            &mut self.hasher_builder.build_hasher(),
        ))
    }
}

/// A simple logical plan summary that's cheaply hashable + comparable, and that has the
/// following guarantees:
///
///   hash(digest(p1)) != hash(digest(p2)) -> digest(p1) != digest(p2) -> p1 != p2
///   hash(p1) != hash(p2) -> hash(digest(p1)) != hash(digest(p2))
///
/// The incorporation of the node count with the plan hash makes hash collisions less likely across
/// distinctly different plans.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LogicalPlanDigest {
    plan_hash: u64,
    node_count: NonZeroUsize,
}

impl LogicalPlanDigest {
    fn new(plan: &LogicalPlan, hasher: &mut DefaultHasher) -> Self {
        plan.hash(hasher);
        Self {
            plan_hash: hasher.finish(),
            node_count: plan.node_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit};

    use crate::{optimization::logical_plan_tracker::LogicalPlanDigest, test::dummy_scan_node};

    #[test]
    fn node_count() -> DaftResult<()> {
        // plan is Filter -> Concat -> {Projection -> Source, Projection -> Source},
        // and should have a node count of 6.
        let builder1 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        assert_eq!(
            LogicalPlanDigest::new(builder1.plan.as_ref(), &mut Default::default()).node_count,
            1usize.try_into().unwrap()
        );
        let builder1 = builder1.project(vec![col("a")], Default::default())?;
        assert_eq!(
            LogicalPlanDigest::new(builder1.plan.as_ref(), &mut Default::default()).node_count,
            2usize.try_into().unwrap()
        );
        let builder2 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        assert_eq!(
            LogicalPlanDigest::new(builder2.plan.as_ref(), &mut Default::default()).node_count,
            1usize.try_into().unwrap()
        );
        let builder2 = builder2.project(vec![col("a")], Default::default())?;
        assert_eq!(
            LogicalPlanDigest::new(builder2.plan.as_ref(), &mut Default::default()).node_count,
            2usize.try_into().unwrap()
        );
        let builder = builder1.concat(&builder2)?;
        assert_eq!(
            LogicalPlanDigest::new(builder.plan.as_ref(), &mut Default::default()).node_count,
            5usize.try_into().unwrap()
        );
        let plan = builder.filter(col("a").lt(&lit(2)))?.build();
        assert_eq!(
            LogicalPlanDigest::new(plan.as_ref(), &mut Default::default()).node_count,
            6usize.try_into().unwrap()
        );
        Ok(())
    }

    #[test]
    fn same_plans_eq() -> DaftResult<()> {
        // Both plan1 and plan2 are Filter -> Project -> Source
        let plan1 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .filter(col("a").lt(&lit(2)))?
        .build();
        let plan2 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .filter(col("a").lt(&lit(2)))?
        .build();
        // Double-check that logical plans are equal.
        assert_eq!(plan1, plan2);

        // Plans should have the same digest.
        let digest1 = LogicalPlanDigest::new(&plan1, &mut Default::default());
        let digest2 = LogicalPlanDigest::new(&plan2, &mut Default::default());
        assert_eq!(digest1, digest2);
        let mut hasher1 = DefaultHasher::new();
        digest1.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        digest2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
        Ok(())
    }

    #[test]
    fn different_plans_not_eq_op_ordering() -> DaftResult<()> {
        // plan1 is Project -> Filter -> Source, while plan2 is Filter -> Project -> Source.
        let plan1 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .filter(col("a").lt(&lit(2)))?
        .project(vec![col("a")], Default::default())?
        .build();
        let plan2 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .filter(col("a").lt(&lit(2)))?
        .build();
        // Double-check that logical plans are NOT equal.
        assert_ne!(plan1, plan2);

        // Plans should NOT have the same digest.
        let digest1 = LogicalPlanDigest::new(&plan1, &mut Default::default());
        let digest2 = LogicalPlanDigest::new(&plan2, &mut Default::default());
        assert_ne!(digest1, digest2);
        let mut hasher1 = DefaultHasher::new();
        digest1.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        digest2.hash(&mut hasher2);
        assert_ne!(hasher1.finish(), hasher2.finish());
        Ok(())
    }

    #[test]
    fn different_plans_not_eq_same_order_diff_config() -> DaftResult<()> {
        // Both plan1 and plan2 are Filter -> Project -> Source, but with different filter predicates.
        let plan1 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .filter(col("a").lt(&lit(2)))?
        .build();
        let plan2 = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .filter(col("a").lt(&lit(4)))?
        .build();
        // Double-check that logical plans are NOT equal.
        assert_ne!(plan1, plan2);

        // Plans should have the same digest.
        let digest1 = LogicalPlanDigest::new(&plan1, &mut Default::default());
        let digest2 = LogicalPlanDigest::new(&plan2, &mut Default::default());
        assert_ne!(digest1, digest2);
        let mut hasher1 = DefaultHasher::new();
        digest1.hash(&mut hasher1);
        let mut hasher2 = DefaultHasher::new();
        digest2.hash(&mut hasher2);
        assert_ne!(hasher1.finish(), hasher2.finish());
        Ok(())
    }
}
