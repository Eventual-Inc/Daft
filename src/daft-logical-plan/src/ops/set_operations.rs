use std::sync::Arc;

use common_error::DaftError;
use daft_algebra::boolean::combine_conjunction;
use daft_core::{count_mode::CountMode, join::JoinType, utils::supertype::get_supertype};
use daft_dsl::{ExprRef, left_col, lit, null_lit, resolved_col, right_col};
use daft_functions_list::{explode, list_fill};
use daft_schema::{dtype::DataType, field::Field, schema::SchemaRef};
use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::{Aggregate, Concat, Distinct, Filter, Project, join::JoinPredicate};
use crate::{LogicalPlan, logical_plan, logical_plan::CreationSnafu};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetQuantifier {
    All,
    Distinct,
}

// todo: rename this to something else if we add support for by name for non-union set operations
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnionStrategy {
    Positional, // e.g. `select * from t1 union select * from t2`
    ByName,     // e.g. `select a1 from t1 union by name select a1 from t2`
}

fn build_union_all_internal(
    lhs: Arc<LogicalPlan>,
    rhs: Arc<LogicalPlan>,
    left_v_cols: Vec<ExprRef>,
    right_v_cols: Vec<ExprRef>,
) -> logical_plan::Result<LogicalPlan> {
    let left_with_v_col = Project::try_new(lhs, left_v_cols)?;
    let right_with_v_col = Project::try_new(rhs, right_v_cols)?;
    Union::try_new(
        left_with_v_col.into(),
        right_with_v_col.into(),
        SetQuantifier::All,
        UnionStrategy::Positional,
    )?
    .to_logical_plan()
}

fn intersect_or_except_plan(
    lhs: Arc<LogicalPlan>,
    rhs: Arc<LogicalPlan>,
    join_type: JoinType,
) -> logical_plan::Result<LogicalPlan> {
    let on_expr = combine_conjunction(
        lhs.schema()
            .into_iter()
            .zip(rhs.schema().fields())
            .map(|(l, r)| left_col(l.clone()).eq_null_safe(right_col(r.clone()))),
    );

    let on = JoinPredicate::try_new(on_expr)?;

    let join = logical_plan::Join::try_new(lhs, rhs, on, join_type, None);
    join.map(|j| Distinct::new(j.into(), None).into())
}

fn check_structurally_equal(
    lhs: SchemaRef,
    rhs: SchemaRef,
    operation: &str,
) -> logical_plan::Result<()> {
    if lhs.len() != rhs.len() {
        return Err(DaftError::SchemaMismatch(format!(
            "Both schemas must have the same num of fields to {}, \
                but got[lhs: {} v.s rhs: {}], lhs schema: {}, rhs schema: {}",
            operation,
            lhs.len(),
            rhs.len(),
            lhs,
            rhs
        )))
        .context(CreationSnafu);
    }
    // lhs and rhs should have the same type for each field
    // TODO: Support nested types recursively
    if lhs
        .into_iter()
        .zip(rhs.fields())
        .any(|(l, r)| l.dtype != r.dtype)
    {
        return Err(DaftError::SchemaMismatch(format!(
            "Both schemas should have the same type for each field to {}, \
                but got lhs schema: {}, rhs schema: {}",
            operation, lhs, rhs
        )))
        .context(CreationSnafu);
    }
    Ok(())
}

const V_COL_L: &str = "__v_col_l";
const V_L_CNT: &str = "__v_l_cnt";
const V_COL_R: &str = "__v_col_r";
const V_R_CNT: &str = "__v_r_cnt";
const V_MIN_COUNT: &str = "__min_count";

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Intersect {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream nodes.
    pub lhs: Arc<LogicalPlan>,
    pub rhs: Arc<LogicalPlan>,
    pub is_all: bool,
}

impl Intersect {
    pub(crate) fn try_new(
        lhs: Arc<LogicalPlan>,
        rhs: Arc<LogicalPlan>,
        is_all: bool,
    ) -> logical_plan::Result<Self> {
        let lhs_schema = lhs.schema();
        let rhs_schema = rhs.schema();
        check_structurally_equal(lhs_schema, rhs_schema, "intersect")?;
        Ok(Self {
            plan_id: None,
            node_id: None,
            lhs,
            rhs,
            is_all,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// intersect operations could be represented by other logical plans
    /// for intersect distinct, it could be represented as a semi join + distinct
    /// ```sql
    /// select a1, a2 from t1 intersect select b1, b2 from t2
    /// ```
    /// is the same as:
    /// ```sql
    /// select distinct a1, a2 from t1 left semi join t2
    ///   on t1.a1 <> t2.b1 and t1.a2 <> t2.b2
    /// ```
    ///
    /// for intersect all, it could be represented as group by + explode
    /// ```sql
    /// select a1 from t1 intersect all select a1 from t2
    /// ```
    /// is the same as:
    /// ```sql
    /// select a1
    /// from (
    ///   select explode(list_fill(min_count, a1)) as a1
    ///   from (
    ///      select a1, if_else(v_l_cnt > v_r_cnt, v_r_cnt, v_l_cnt) as min_count
    ///      from (
    ///        select count(v_col_l) as v_l_cnt, count(v_col_r) as v_r_cnt, a1
    ///        from (
    ///          select true as v_col_l, null as v_col_r, a1 from t1
    ///          union all
    ///          select null as v_col_l, true as v_col_r, a1 from t2
    ///        ) as union_all
    ///        group by a1
    ///      )
    ///      where v_l_cnt >= 1 and v_r_cnt >= 1
    ///   )
    /// )
    /// ```
    /// TODO: Move this logical to logical optimization rules
    pub(crate) fn to_logical_plan(&self) -> logical_plan::Result<LogicalPlan> {
        if self.is_all {
            let left_cols = self
                .lhs
                .schema()
                .field_names()
                .map(resolved_col)
                .collect::<Vec<ExprRef>>();
            // project the right cols to have the same name as the left cols
            let right_cols = self
                .rhs
                .schema()
                .field_names()
                .map(resolved_col)
                .zip(left_cols.iter())
                .map(|(r, l)| r.alias(l.name()))
                .collect::<Vec<ExprRef>>();
            let left_v_cols = vec![
                lit(true).alias(V_COL_L),
                null_lit().cast(&DataType::Boolean).alias(V_COL_R),
            ];
            let right_v_cols = vec![
                null_lit().cast(&DataType::Boolean).alias(V_COL_L),
                lit(true).alias(V_COL_R),
            ];
            let left_v_cols = [left_v_cols, left_cols.clone()].concat();
            let right_v_cols = [right_v_cols, right_cols].concat();
            let union_all = build_union_all_internal(
                self.lhs.clone(),
                self.rhs.clone(),
                left_v_cols,
                right_v_cols,
            )?;
            let one_lit = lit(1);
            let left_v_cnt = resolved_col(V_COL_L).count(CountMode::Valid).alias(V_L_CNT);
            let right_v_cnt = resolved_col(V_COL_R).count(CountMode::Valid).alias(V_R_CNT);
            let min_count = resolved_col(V_L_CNT)
                .gt(resolved_col(V_R_CNT))
                .if_else(resolved_col(V_R_CNT), resolved_col(V_L_CNT))
                .alias(V_MIN_COUNT);
            let aggregate_plan = Aggregate::try_new(
                union_all.into(),
                vec![left_v_cnt, right_v_cnt],
                left_cols.clone(),
            )?;
            let filter_plan = Filter::try_new(
                aggregate_plan.into(),
                resolved_col(V_L_CNT)
                    .gt_eq(one_lit.clone())
                    .and(resolved_col(V_R_CNT).gt_eq(one_lit)),
            )?;
            let min_count_plan = Project::try_new(
                filter_plan.into(),
                [vec![min_count], left_cols.clone()].concat(),
            )?;
            let fill_and_explodes = left_cols
                .iter()
                .map(|column| {
                    explode(list_fill(column.clone(), resolved_col(V_MIN_COUNT)))
                        .alias(column.name())
                })
                .collect::<Vec<_>>();
            let project_plan = Project::try_new(min_count_plan.into(), fill_and_explodes)?;
            Ok(project_plan.into())
        } else {
            intersect_or_except_plan(self.lhs.clone(), self.rhs.clone(), JoinType::Semi)
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if self.is_all {
            res.push("Intersect All:".to_string());
        } else {
            res.push("Intersect:".to_string());
        }
        res
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Union {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream nodes.
    pub lhs: Arc<LogicalPlan>,
    pub rhs: Arc<LogicalPlan>,
    pub quantifier: SetQuantifier,
    pub strategy: UnionStrategy,
}

impl Union {
    /// Union is more flexible than concat as it allows the two tables to have different schemas as long as they have a shared supertype
    /// ex:
    /// ```sql
    /// > create table t0 as select 'hello' as c0;
    /// > create table t1 as select 1 as c0;
    /// > select * from t0 union select * from t1;
    /// ```
    /// This is valid in Union, but not in Concat
    pub(crate) fn try_new(
        lhs: Arc<LogicalPlan>,
        rhs: Arc<LogicalPlan>,
        quantifier: SetQuantifier,
        strategy: UnionStrategy,
    ) -> logical_plan::Result<Self> {
        if matches!(strategy, UnionStrategy::Positional) && lhs.schema().len() != rhs.schema().len()
        {
            return Err(DaftError::SchemaMismatch(format!(
                "Both plans must have the same num of fields to union, \
                but got[lhs: {} v.s rhs: {}], lhs schema: {}, rhs schema: {}",
                lhs.schema().len(),
                rhs.schema().len(),
                lhs.schema(),
                rhs.schema()
            )))
            .context(CreationSnafu);
        }
        Ok(Self {
            plan_id: None,
            node_id: None,
            lhs,
            rhs,
            quantifier,
            strategy,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// union could be represented as a concat + distinct
    /// while union all could be represented as a concat
    pub(crate) fn to_logical_plan(&self) -> logical_plan::Result<LogicalPlan> {
        let lhs_schema = self.lhs.schema();
        let rhs_schema = self.rhs.schema();
        match self.strategy {
            UnionStrategy::Positional => {
                let (lhs, rhs) = if lhs_schema != rhs_schema {
                    // we need to try to do a type coercion
                    let coerced_fields = lhs_schema
                        .into_iter()
                        .zip(rhs_schema.fields())
                        .map(|(l, r)| {
                            let new_dtype = get_supertype(&l.dtype, &r.dtype).ok_or_else(|| {
                                logical_plan::Error::CreationError {
                                    source: DaftError::ComputeError(
                                        format!("
                                            unable to find a common supertype for union. {} and {} have no common supertype",
                                        l.dtype, r.dtype
                                        ),
                                    ),
                                }
                            })?;
                            Ok::<_, logical_plan::Error>(Field::new(l.name.clone(), new_dtype))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let projection_fields = coerced_fields
                        .into_iter()
                        .map(|f| resolved_col(f.name.clone()).cast(&f.dtype))
                        .collect::<Vec<_>>();
                    let lhs = Project::try_new(self.lhs.clone(), projection_fields.clone())?.into();
                    let rhs = Project::try_new(self.rhs.clone(), projection_fields)?.into();
                    (lhs, rhs)
                } else {
                    (self.lhs.clone(), self.rhs.clone())
                };
                let concat = LogicalPlan::Concat(Concat::try_new(lhs, rhs)?);
                match self.quantifier {
                    SetQuantifier::All => Ok(concat),
                    SetQuantifier::Distinct => Ok(Distinct::new(concat.arced(), None).into()),
                }
            }
            UnionStrategy::ByName => {
                let lhs_fields = lhs_schema.into_iter().cloned().collect::<IndexSet<_>>();
                let rhs_fields = rhs_schema.into_iter().cloned().collect::<IndexSet<_>>();
                let all_fields = lhs_fields
                    .union(&rhs_fields)
                    .cloned()
                    .collect::<IndexSet<_>>();
                let lhs_with_columns = all_fields
                    .iter()
                    .map(|f| {
                        if lhs_fields.contains(f) {
                            resolved_col(f.name.clone())
                        } else {
                            null_lit().cast(&f.dtype).alias(f.name.clone())
                        }
                    })
                    .collect::<Vec<_>>();
                let rhs_with_columns = all_fields
                    .iter()
                    .map(|f| {
                        if rhs_fields.contains(f) {
                            resolved_col(f.name.clone())
                        } else {
                            null_lit().cast(&f.dtype).alias(f.name.clone())
                        }
                    })
                    .collect::<Vec<_>>();
                let lhs = Project::try_new(self.lhs.clone(), lhs_with_columns)?;
                let rhs = Project::try_new(self.rhs.clone(), rhs_with_columns)?;
                let concat = LogicalPlan::Concat(Concat::try_new(lhs.into(), rhs.into())?);

                match self.quantifier {
                    SetQuantifier::All => Ok(concat),
                    SetQuantifier::Distinct => Ok(Distinct::new(concat.arced(), None).into()),
                }
            }
        }
    }
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        use SetQuantifier::{All, Distinct};
        use UnionStrategy::{ByName, Positional};
        match (self.quantifier, self.strategy) {
            (Distinct, Positional) => res.push("Union:".to_string()),
            (All, Positional) => res.push("Union All:".to_string()),
            (Distinct, ByName) => res.push("Union By Name:".to_string()),
            (All, ByName) => res.push("Union All By Name:".to_string()),
        }
        res
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Except {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream nodes.
    pub lhs: Arc<LogicalPlan>,
    pub rhs: Arc<LogicalPlan>,
    pub is_all: bool,
}
impl Except {
    pub(crate) fn try_new(
        lhs: Arc<LogicalPlan>,
        rhs: Arc<LogicalPlan>,
        is_all: bool,
    ) -> logical_plan::Result<Self> {
        let lhs_schema = lhs.schema();
        let rhs_schema = rhs.schema();
        check_structurally_equal(lhs_schema, rhs_schema, "except")?;
        Ok(Self {
            plan_id: None,
            node_id: None,
            lhs,
            rhs,
            is_all,
        })
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// except could be represented by other logical plans
    /// for except distinct, it could be represented as a anti join
    /// ```sql
    /// select a1, a2 from t1 except select b1, b2 from t2
    /// ```
    /// is the same as:
    /// ```sql
    /// select distinct a1, a2 from t1 left anti join t2
    ///   on t1.a1 <> t2.b1 and t1.a2 <> t2.b2
    /// ```
    ///
    /// for except all, it could be represented as group by + explode
    /// ```sql
    /// select a1 from t1 except all select a1 from t2
    /// ```
    /// is the same as:
    /// ```sql
    /// select a1
    /// from (
    ///  select explode(list_fill(sum, a1)) as a1
    ///  from (
    ///    select sum(v_col) as sum, a1
    ///    from (
    ///      select 1 as v_col, a1 from t1
    ///      union all
    ///      select -1 as v_col, a1 from t2
    ///    ) union_all
    ///    group by a1
    ///  )
    ///  where sum > 0
    /// )
    /// ```
    /// TODO: Move this logical to logical optimization rules
    pub(crate) fn to_logical_plan(&self) -> logical_plan::Result<LogicalPlan> {
        if self.is_all {
            let left_cols = self
                .lhs
                .schema()
                .field_names()
                .map(resolved_col)
                .collect::<Vec<ExprRef>>();
            // project the right cols to have the same name as the left cols
            let right_cols = self
                .rhs
                .schema()
                .field_names()
                .map(resolved_col)
                .zip(left_cols.iter())
                .map(|(r, l)| r.alias(l.name()))
                .collect::<Vec<ExprRef>>();
            let virtual_col = "__v_col";
            let virtual_sum = "__sum";
            let left_v_cols = vec![lit(1).alias(virtual_col)];
            let right_v_cols = vec![lit(-1).alias(virtual_col)];
            let left_v_cols = [left_v_cols, left_cols.clone()].concat();
            let right_v_cols = [right_v_cols, right_cols].concat();
            let union_all = build_union_all_internal(
                self.lhs.clone(),
                self.rhs.clone(),
                left_v_cols,
                right_v_cols,
            )?;
            let sum = resolved_col(virtual_col).sum().alias(virtual_sum);
            let aggregate_plan =
                Aggregate::try_new(union_all.into(), vec![sum], left_cols.clone())?;
            let filter_plan =
                Filter::try_new(aggregate_plan.into(), resolved_col(virtual_sum).gt(lit(0)))?;
            let fill_and_explodes = left_cols
                .iter()
                .map(|column| {
                    explode(list_fill(column.clone(), resolved_col(virtual_sum)))
                        .alias(column.name())
                })
                .collect::<Vec<_>>();
            let project_plan = Project::try_new(filter_plan.into(), fill_and_explodes)?;
            Ok(project_plan.into())
        } else {
            intersect_or_except_plan(self.lhs.clone(), self.rhs.clone(), JoinType::Anti)
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if self.is_all {
            res.push("Except All:".to_string());
        } else {
            res.push("Except:".to_string());
        }
        res
    }
}
