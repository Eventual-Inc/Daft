use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::Aggregation;
use common_treenode::Transformed;
use daft_core::count_mode::CountMode;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_schema::{dtype::DataType, field::Field, schema::Schema};

use crate::{
    logical_plan::{Aggregate, LogicalPlan},
    ops::Source as LogicalSource,
    optimization::rules::OptimizerRule,
    source_info::SourceInfo,
};

/// Optimization rules for pushing Filters further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownAggregation;

impl OptimizerRule for PushDownAggregation {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
                ..
            }) if groupby.is_empty() => {
                // 只处理全局聚合（没有GROUP BY）

                // 检查是否只有一个count聚合
                if aggregations.len() == 1 {
                    if let Some(count_expr) = is_count_expr(&aggregations[0]) {
                        // 将 match 替换为 if let 来匹配 Source 节点
                        if let LogicalPlan::Source(source) = input.as_ref() {
                            let source_info = source.source_info.as_ref();
                            if let SourceInfo::Physical(external_info) = source_info {
                                // 检查扫描操作符是否支持count下推
                                let scan_op = external_info.scan_state.get_scan_op().clone().0;
                                if scan_op.can_absorb_aggregation()
                                    && scan_op.supports_count_pushdown()
                                {
                                    // 创建带有count聚合的pushdowns
                                    let count_mode = extract_count_mode(count_expr);
                                    let new_pushdowns = external_info
                                        .pushdowns
                                        .with_aggregation(Some(Aggregation::Count(count_mode)));
                                    let new_external_info =
                                        external_info.with_pushdowns(new_pushdowns);

                                    // 创建新的数据源节点，包含count聚合pushdown
                                    let new_source = LogicalPlan::Source(LogicalSource::new(
                                        // 输出模式应该是单列UInt64类型
                                        Schema::new(vec![Field::new("count", DataType::UInt64)])
                                            .into(),
                                        SourceInfo::Physical(new_external_info).into(),
                                    ))
                                    .into();

                                    return Ok(Transformed::yes(new_source));
                                }
                            }
                        }
                    }
                }

                // 如果不能下推，则保持原样
                Ok(Transformed::no(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

// 辅助函数，检查表达式是否为count聚合
fn is_count_expr(expr: &ExprRef) -> Option<&CountMode> {
    match expr.as_ref() {
        Expr::Agg(agg) => {
            // 匹配Count和CountDistinct两种聚合表达式
            match agg {
                // 标准Count表达式，包含计数模式
                AggExpr::Count(_, count_mode) => Some(count_mode),
                // CountDistinct表达式，固定为Distinct模式
                AggExpr::CountDistinct(_) => Some(&CountMode::All),
                _ => None,
            }
        }
        _ => None,
    }
}

// 辅助函数，提取count模式
fn extract_count_mode(count_mode: &CountMode) -> CountMode {
    *count_mode
}
