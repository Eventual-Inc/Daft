use std::sync::Arc;

use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::{Column, ExprRef};
use daft_functions_uri::UrlDownloadArgs;
use daft_schema::{dtype::DataType, field::Field};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    logical_plan::LogicalPlan,
    stats::{ApproxStats, PlanStats, StatsState},
};

/// UrlDownload operator for downloading data from a URL.
///
/// It is currently unavailable in the Python API and only constructed by the
/// Daft logical optimizer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UrlDownload {
    /// An id for the plan.
    pub plan_id: Option<usize>,
    /// An id for the node.
    pub node_id: Option<usize>,
    /// Upstream node.
    pub input: Arc<LogicalPlan>,
    /// Output column and schema.
    pub output_column: String,
    pub output_schema: SchemaRef,
    /// URL download arguments.
    pub args: UrlDownloadArgs<ExprRef>,
    /// Passthrough columns (for column pruning)
    pub passthrough_columns: Vec<Column>,
    /// The plan statistics.
    pub stats_state: StatsState,
}

impl UrlDownload {
    pub fn gen_output_schema(
        input_schema: &SchemaRef,
        passthrough_columns: &[Column],
        output_column: &str,
    ) -> SchemaRef {
        let mut fields = passthrough_columns
            .iter()
            .map(|c| input_schema.get_field(&c.name()).unwrap())
            .cloned()
            .collect::<Vec<_>>();
        fields.push(Field::new(output_column.to_string(), DataType::Binary));
        Arc::new(Schema::new(fields))
    }

    pub fn new(
        input: Arc<LogicalPlan>,
        args: UrlDownloadArgs<ExprRef>,
        output_column: String,
        passthrough_columns: Vec<Column>,
    ) -> Self {
        let output_schema =
            Self::gen_output_schema(&input.schema(), &passthrough_columns, &output_column);

        Self {
            plan_id: None,
            node_id: None,
            input,
            output_column,
            output_schema,
            args,
            passthrough_columns,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn input_column(&self) -> &ExprRef {
        &self.args.input
    }

    pub fn with_plan_id(mut self, id: usize) -> Self {
        self.plan_id = Some(id);
        self
    }

    pub fn with_node_id(mut self, id: usize) -> Self {
        self.node_id = Some(id);
        self
    }

    pub fn with_passthrough_columns(mut self, passthrough_columns: Vec<Column>) -> Self {
        self.passthrough_columns = passthrough_columns;
        self.output_schema = Self::gen_output_schema(
            &self.input.schema(),
            &self.passthrough_columns,
            &self.output_column,
        );
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let input_stats = self.input.materialized_stats();
        let num_rows = input_stats.approx_stats.num_rows;

        // We assume that the size of the downloaded column is 8KB per row.
        // TODO: Some sort of estimation or sampling of the download file size.
        let approx_stats = ApproxStats {
            num_rows,
            size_bytes: input_stats.approx_stats.size_bytes + (8 * 1024 * num_rows),
            acc_selectivity: input_stats.approx_stats.acc_selectivity,
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "UrlDownload: {} -> {}",
            self.args.input, self.output_column
        ));
        res.push(format!("Multi-Thread: {:?}", self.args.multi_thread));
        res.push(format!("Max Connections: {:?}", self.args.max_connections));
        res.push(format!("On Error: {:?}", self.args.on_error));
        res.push(format!(
            "Passthrough columns: {}",
            self.passthrough_columns
                .iter()
                .map(|c| c.to_string())
                .join(", ")
        ));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
