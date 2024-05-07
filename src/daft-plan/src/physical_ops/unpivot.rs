use std::sync::Arc;

use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};

use itertools::Itertools;

use crate::{
    partitioning::translate_clustering_spec, physical_plan::PhysicalPlanRef, ClusteringSpec,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Unpivot {
    pub input: PhysicalPlanRef,
    pub ids: Vec<ExprRef>,
    pub values: Vec<ExprRef>,
    pub variable_name: String,
    pub value_name: String,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl Unpivot {
    pub(crate) fn new(
        input: PhysicalPlanRef,
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: &str,
        value_name: &str,
    ) -> Self {
        let clustering_spec = translate_clustering_spec(input.clustering_spec(), &values);

        Self {
            input,
            ids,
            values,
            variable_name: variable_name.to_string(),
            value_name: value_name.to_string(),
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Unpivot: {}",
            self.values.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Ids = {}",
            self.ids.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Variable name = {}", self.variable_name));
        res.push(format!("Value name = {}", self.value_name));
        res
    }
}
