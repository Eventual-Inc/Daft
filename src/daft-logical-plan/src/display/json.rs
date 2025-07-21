use std::{collections::HashMap, fmt::Write};

use common_error::{DaftError, DaftResult};
use common_treenode::TreeNodeVisitor;
use daft_dsl::{resolved_col, Literal};
use serde_json::json;

use crate::{LogicalPlan, LogicalPlanRef};
pub struct JsonVisitor<'a, W>
where
    W: std::fmt::Write,
{
    f: &'a mut W,
    objects: HashMap<u32, serde_json::Value>,
    /// if true, include the summarized schema information
    with_schema: bool,
    next_id: u32,
    parent_ids: Vec<u32>,
}

impl<'a, W> JsonVisitor<'a, W>
where
    W: std::fmt::Write,
{
    pub fn new(f: &'a mut W) -> Self {
        Self {
            f,
            objects: HashMap::new(),
            with_schema: false,
            next_id: 0,
            parent_ids: Vec::new(),
        }
    }
    pub fn with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }
    fn to_json_value(node: &LogicalPlan) -> serde_json::Value {
        match node {
            LogicalPlan::Source(_) => json!({}),
            // TODO(desmond): is this correct?
            LogicalPlan::Shard(shard) => json!({
                "sharder": shard.sharder,
            }),
            LogicalPlan::Project(project) => json!({
                "projection": project.projection.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::ActorPoolProject(project) => json!({
                "projection": project.projection.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::Filter(filter) => json!({
                "predicate": vec![&filter.predicate.to_string()],
            }),
            LogicalPlan::Limit(limit) => json!({
                "limit": vec![&limit.limit.lit().to_string()],
            }),
            LogicalPlan::Explode(explode) => json!({
                "to_explode": explode.to_explode.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::Unpivot(unpivot) => json!({
                "ids": unpivot.ids.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "values": unpivot.values.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "variable_name": unpivot.variable_name,
                "value_name": unpivot.value_name,
            }),
            LogicalPlan::Sort(sort) => json!({
                "sort_by": sort.sort_by.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                "nulls_first": sort.nulls_first,
                "descending": sort.descending,

            }),
            LogicalPlan::Repartition(repartition) => json!({
                "repartition_spec": repartition.repartition_spec,
            }),
            LogicalPlan::Distinct(_) => json!({}),
            LogicalPlan::Aggregate(aggregate) => json!({
                "aggregations": aggregate.aggregations.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "groupby": aggregate.groupby.iter().map(|e| e.to_string()).collect::<Vec<_>>(),

            }),
            LogicalPlan::Pivot(pivot) => json!({
                "pivot_column": pivot.pivot_column.to_string(),
                "value_column": pivot.value_column.to_string(),
                "aggregation": pivot.aggregation,
                "group_by": pivot.group_by.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "names": pivot.names.iter().map(|e| resolved_col(e.clone()).to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::Concat(_) => json!({}),
            LogicalPlan::Intersect(intersect) => json!({
                "is_all": intersect.is_all,
            }),
            LogicalPlan::Union(union_) => json!({
                "quantifier": union_.quantifier,
                "strategy": union_.strategy,
            }),
            LogicalPlan::Join(join) => json!({
                "on": vec![&join.on.inner().map(|e| e.to_string())],
                "type": join.join_type,
                "strategy": join.join_strategy,

            }),
            LogicalPlan::Sink(_) => json!({}),
            LogicalPlan::Sample(sample) => json!({
                "fraction": sample.fraction,
                "with_replacement": sample.with_replacement,
                "seed": sample.seed,
            }),
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => json!({
                "column_name": vec![resolved_col(monotonically_increasing_id.column_name.clone()).to_string()]
            }),
            LogicalPlan::SubqueryAlias(subquery_alias) => json!({
                "name": subquery_alias.name,
            }),
            LogicalPlan::Window(window) => json!({
                "exprs": window.window_functions.iter().map(|e| e.to_string()).collect::<Vec<String>>(),
                "aliases": window.aliases,
                "window_spec": window.window_spec,
            }),
            LogicalPlan::TopN(top_n) => json!({
                "sort_by": top_n.sort_by.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                "nulls_first": top_n.nulls_first,
                "descending": top_n.descending,
                "limit": top_n.limit,
            }),
        }
    }
}

impl<W> JsonVisitor<'_, W> where W: Write {}

impl<W> TreeNodeVisitor for JsonVisitor<'_, W>
where
    W: Write,
{
    type Node = LogicalPlanRef;

    fn f_down(&mut self, node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        let id = self.next_id;
        self.next_id += 1;
        let mut object = Self::to_json_value(node.as_ref());
        // handle all common properties here
        object["type"] = json!(node.name());
        object["children"] = serde_json::Value::Array(vec![]);

        if self.with_schema {
            let schema = node.schema();
            object["schema"] = json!(schema.fields());
        }
        self.objects.insert(id, object);
        self.parent_ids.push(id);
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        let id = self.parent_ids.pop().unwrap();

        let current_node = self
            .objects
            .remove(&id)
            .ok_or_else(|| DaftError::ValueError("Missing current node!".to_string()))?;

        if let Some(parent_id) = self.parent_ids.last() {
            let parent_node = self
                .objects
                .get_mut(parent_id)
                .expect("Missing parent node!");

            let plans = parent_node
                .get_mut("children")
                .and_then(|p| p.as_array_mut())
                .expect("children should be an array");

            plans.push(current_node);
        } else {
            // This is the root node
            let plan = serde_json::json!(&current_node);
            write!(
                self.f,
                "{}",
                serde_json::to_string(&plan).map_err(DaftError::SerdeJsonError)?
            )?;
        }
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use common_treenode::TreeNode;
    use daft_core::join::JoinType;
    use daft_dsl::{lit, resolved_col};
    use daft_functions_utf8::{endswith, startswith};

    use crate::{
        display::test::{plan_1, plan_2},
        LogicalPlanBuilder,
    };

    #[test]
    fn test_repr_json() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::from(plan_1())
            .filter(resolved_col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::from(plan_2())
            .filter(
                startswith(resolved_col("last_name"), lit("S"))
                    .and(endswith(resolved_col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(Some("id2"), None)?
            .distinct(None)?
            .sort(vec![resolved_col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::from(subplan)
            .join(
                subplan2,
                None,
                vec!["id".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(resolved_col("first_name").eq(lit("hello")))?
            .select(vec![resolved_col("first_name")])?
            .limit(10, false)?
            .build();

        let mut output = String::new();

        let mut json_vis = super::JsonVisitor::new(&mut output);

        plan.visit(&mut json_vis).unwrap();

        let expected: serde_json::Value = serde_json::from_str(r#"{
          "children": [
            {
              "children": [
                {
                  "children": [
                    {
                      "children": [
                        {
                          "children": [
                            {
                              "children": [],
                              "type": "Source"
                            }
                          ],
                          "predicate": ["col(id) == lit(1)"],
                          "type": "Filter"
                        },
                        {
                          "children": [
                            {
                              "children": [
                                {
                                  "children": [
                                    {
                                      "children": [
                                        {
                                          "children": [
                                            {
                                              "children": [],
                                              "type": "Source"
                                            }
                                          ],
                                          "predicate": [
                                            "starts_with(col(last_name), lit(\"S\")) & ends_with(col(last_name), lit(\"n\"))"
                                          ],
                                          "type": "Filter"
                                        }
                                      ],
                                      "limit": ["lit(1000)"],
                                      "type": "Limit"
                                    }
                                  ],
                                  "column_name": ["col(id2)"],
                                  "type": "MonotonicallyIncreasingId"
                                }
                              ],
                              "type": "Distinct"
                            }
                          ],
                          "descending": [false],
                          "nulls_first": [false],
                          "sort_by": ["col(last_name)"],
                          "type": "Sort"
                        }
                      ],
                      "on": ["col(left.id#Int32) == col(right.id#Int32)"],
                      "strategy": null,
                      "type": "Join"
                    }
                  ],
                  "predicate": ["col(first_name) == lit(\"hello\")"],
                  "type": "Filter"
                }
              ],
              "projection": ["col(first_name)"],
              "type": "Project"
            }
          ],
          "limit": ["lit(10)"],
          "type": "Limit"
        }
        "#).unwrap();
        let actual: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(actual, expected);
        Ok(())
    }
    #[test]
    fn test_repr_json_with_schema() {
        let plan = plan_1();
        let mut output = String::new();

        let mut json_vis = super::JsonVisitor::new(&mut output);
        json_vis.with_schema(true);
        plan.visit(&mut json_vis).unwrap();
        let expected = r#"
            {
              "children": [],
              "schema": [
                {
                  "dtype": "Utf8",
                  "metadata": {},
                  "name": "text"
                },
                {
                  "dtype": "Int32",
                  "metadata": {},
                  "name": "id"
                }
              ],
              "type": "Source"
            }
         "#;

        let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
        let actual: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(actual, expected);
    }
}
