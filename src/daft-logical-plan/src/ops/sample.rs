use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    LogicalPlan,
    stats::{PlanStats, StatsState},
};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Sample {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub fraction: Option<f64>,
    pub size: Option<usize>,
    pub with_replacement: bool,
    pub seed: Option<u64>,
    pub stats_state: StatsState,
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.plan_id.hash(state);
        self.node_id.hash(state);
        self.input.hash(state);
        self.fraction.map(canonical_fraction_bits).hash(state);
        self.size.hash(state);
        self.with_replacement.hash(state);
        self.seed.hash(state);
        self.stats_state.hash(state);
    }
}

fn canonical_fraction_bits(fraction: f64) -> u64 {
    if fraction == 0.0 {
        0.0f64.to_bits()
    } else {
        fraction.to_bits()
    }
}

impl Sample {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        fraction: Option<f64>,
        size: Option<usize>,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            input,
            fraction,
            size,
            with_replacement,
            seed,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        let approx_stats = if let Some(fraction) = self.fraction {
            input_stats
                .approx_stats
                .apply(|v| ((v as f64) * fraction) as usize)
        } else if let Some(size) = self.size {
            input_stats.approx_stats.apply(|v| v.min(size))
        } else {
            input_stats.approx_stats.clone()
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(fraction) = self.fraction {
            res.push(format!("Sample: {} (fraction)", fraction));
        } else if let Some(size) = self.size {
            res.push(format!("Sample: {} rows", size));
        }
        res.push(format!("With replacement = {}", self.with_replacement));
        res.push(format!("Seed = {:?}", self.seed));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        sync::Arc,
    };

    use daft_core::prelude::*;
    use daft_schema::schema::Schema;

    use super::Sample;
    use crate::{
        LogicalPlan,
        ops::Source,
        source_info::{InMemoryInfo, SourceInfo},
        stats::{ApproxStats, PlanStats, StatsState},
    };

    fn compute_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn dummy_input() -> Arc<LogicalPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]));
        let info = InMemoryInfo::new(
            schema.clone(),
            "sample_hash_test".to_string(),
            None,
            1,
            0,
            0,
            None,
            None,
        );
        let source = Source::new(schema, Arc::new(SourceInfo::InMemory(info)));
        Arc::new(LogicalPlan::Source(source))
    }

    fn sample_with_fraction(fraction: f64) -> Sample {
        Sample::new(dummy_input(), Some(fraction), None, false, Some(42))
    }

    #[test]
    fn test_hash_preserves_fraction_precision() {
        let sample_a = sample_with_fraction(0.1234561);
        let sample_b = sample_with_fraction(0.1234564);

        assert_ne!(sample_a, sample_b);
        assert_ne!(compute_hash(&sample_a), compute_hash(&sample_b));
    }

    #[test]
    fn test_hash_normalizes_signed_zero_fraction() {
        let sample_a = sample_with_fraction(-0.0);
        let sample_b = sample_with_fraction(0.0);

        assert_eq!(sample_a, sample_b);
        assert_eq!(compute_hash(&sample_a), compute_hash(&sample_b));
    }

    #[test]
    fn test_hash_includes_plan_identity_fields() {
        let input = dummy_input();
        let sample_a = Sample::new(input.clone(), Some(0.5), None, false, Some(42))
            .with_plan_id(100)
            .with_node_id(10);
        let sample_b = Sample::new(input, Some(0.5), None, false, Some(42))
            .with_plan_id(200)
            .with_node_id(20);

        assert_ne!(sample_a, sample_b);
        assert_ne!(compute_hash(&sample_a), compute_hash(&sample_b));
    }

    #[test]
    fn test_hash_includes_stats_materialization_state() {
        let input = dummy_input();
        let sample_a = Sample::new(input.clone(), Some(0.5), None, false, Some(42));
        let mut sample_b = Sample::new(input, Some(0.5), None, false, Some(42));
        sample_b.stats_state = StatsState::Materialized(
            PlanStats::new(ApproxStats {
                num_rows: 10,
                size_bytes: 80,
                acc_selectivity: 0.5,
            })
            .into(),
        );

        assert_ne!(sample_a, sample_b);
        assert_ne!(compute_hash(&sample_a), compute_hash(&sample_b));
    }
}
