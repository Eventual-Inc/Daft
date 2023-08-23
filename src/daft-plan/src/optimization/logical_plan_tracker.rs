use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
    num::NonZeroUsize,
};

use crate::LogicalPlan;

pub struct LogicalPlanTracker {
    past_plans: HashSet<LogicalPlanDigest>,
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
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::{col, lit};

    use crate::{
        ops::{Concat, Filter, Project},
        optimization::logical_plan_tracker::LogicalPlanDigest,
        test::dummy_scan_node,
        LogicalPlan,
    };

    #[test]
    fn node_count() -> DaftResult<()> {
        // plan is Filter -> Concat -> {Projection -> Source, Projection -> Source},
        // and should have a node count of 6.
        let plan1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        assert_eq!(
            LogicalPlanDigest::new(&plan1, &mut Default::default()).node_count,
            1usize.try_into().unwrap()
        );
        let plan1: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan1.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan1.into(),
        )
        .into();
        assert_eq!(
            LogicalPlanDigest::new(&plan1, &mut Default::default()).node_count,
            2usize.try_into().unwrap()
        );
        let plan2: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        assert_eq!(
            LogicalPlanDigest::new(&plan2, &mut Default::default()).node_count,
            1usize.try_into().unwrap()
        );
        let plan2: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan2.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan2.into(),
        )
        .into();
        assert_eq!(
            LogicalPlanDigest::new(&plan2, &mut Default::default()).node_count,
            2usize.try_into().unwrap()
        );
        let plan: LogicalPlan = Concat::new(plan1.into(), plan2.into()).into();
        assert_eq!(
            LogicalPlanDigest::new(&plan, &mut Default::default()).node_count,
            5usize.try_into().unwrap()
        );
        let plan: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan.into()).into();
        assert_eq!(
            LogicalPlanDigest::new(&plan, &mut Default::default()).node_count,
            6usize.try_into().unwrap()
        );
        Ok(())
    }

    #[test]
    fn same_plans_eq() -> DaftResult<()> {
        // Both plan1 and plan2 are Filter -> Project -> Source
        let plan1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let plan1: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan1.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan1.into(),
        )
        .into();
        let plan1: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan1.into()).into();
        let plan2: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let plan2: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan2.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan2.into(),
        )
        .into();
        let plan2: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan2.into()).into();
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
    fn different_plans_not_eq() -> DaftResult<()> {
        // plan1 is Project -> Filter -> Source, while plan2 is Filter -> Project -> Source.
        let plan1: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let plan1: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan1.into()).into();
        let plan1: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan1.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan1.into(),
        )
        .into();
        let plan2: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let plan2: LogicalPlan = Project::new(
            vec![col("a")],
            Schema::new(vec![plan2.schema().get_field("a")?.clone()])?.into(),
            Default::default(),
            plan2.into(),
        )
        .into();
        let plan2: LogicalPlan = Filter::new(col("a").lt(&lit(2)), plan2.into()).into();
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
}
