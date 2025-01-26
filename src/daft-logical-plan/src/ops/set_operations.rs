use std::sync::Arc;

use common_error::DaftError;
use daft_core::{count_mode::CountMode, join::JoinType, utils::supertype::get_supertype};
use daft_dsl::{col, lit, null_lit, ExprRef};
use daft_functions::list::{explode, list_fill};
use daft_schema::{dtype::DataType, field::Field, schema::SchemaRef};
use snafu::ResultExt;

use super::{Aggregate, Concat, Distinct, Filter, Project};
use crate::{logical_plan, logical_plan::CreationSnafu, LogicalPlan};

fn build_union_all_internal(
    lhs: Arc<LogicalPlan>,
    rhs: Arc<LogicalPlan>,
    left_v_cols: Vec<ExprRef>,
    right_v_cols: Vec<ExprRef>,
) -> logical_plan::Result<LogicalPlan> {
    let left_with_v_col = Project::try_new(lhs, left_v_cols)?;
    let right_with_v_col = Project::try_new(rhs, right_v_cols)?;
    Union::try_new(left_with_v_col.into(), right_with_v_col.into(), true)?.to_logical_plan()
}

fn intersect_or_except_plan(
    lhs: Arc<LogicalPlan>,
    rhs: Arc<LogicalPlan>,
    join_type: JoinType,
) -> logical_plan::Result<LogicalPlan> {
    let left_on = lhs
        .schema()
        .fields
        .keys()
        .map(|k| col(k.clone()))
        .collect::<Vec<ExprRef>>();
    let left_on_size = left_on.len();
    let right_on = rhs
        .schema()
        .fields
        .keys()
        .map(|k| col(k.clone()))
        .collect::<Vec<ExprRef>>();
    let join = logical_plan::Join::try_new(
        lhs,
        rhs,
        left_on,
        right_on,
        Some(vec![true; left_on_size]),
        join_type,
        None,
    );
    join.map(|j| Distinct::new(j.into()).into())
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
        .fields
        .values()
        .zip(rhs.fields.values())
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Intersect {
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
        Ok(Self { lhs, rhs, is_all })
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
                .fields
                .keys()
                .map(|k| col(k.clone()))
                .collect::<Vec<ExprRef>>();
            // project the right cols to have the same name as the left cols
            let right_cols = self
                .rhs
                .schema()
                .fields
                .keys()
                .map(|k| col(k.clone()))
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
            let left_v_cnt = col(V_COL_L).count(CountMode::Valid).alias(V_L_CNT);
            let right_v_cnt = col(V_COL_R).count(CountMode::Valid).alias(V_R_CNT);
            let min_count = col(V_L_CNT)
                .gt(col(V_R_CNT))
                .if_else(col(V_R_CNT), col(V_L_CNT))
                .alias(V_MIN_COUNT);
            let aggregate_plan = Aggregate::try_new(
                union_all.into(),
                vec![left_v_cnt, right_v_cnt],
                left_cols.clone(),
            )?;
            let filter_plan = Filter::try_new(
                aggregate_plan.into(),
                col(V_L_CNT)
                    .gt_eq(one_lit.clone())
                    .and(col(V_R_CNT).gt_eq(one_lit)),
            )?;
            let min_count_plan = Project::try_new(
                filter_plan.into(),
                [vec![min_count], left_cols.clone()].concat(),
            )?;
            let fill_and_explodes = left_cols
                .iter()
                .map(|column| {
                    explode(list_fill(col(V_MIN_COUNT), column.clone())).alias(column.name())
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Union {
    // Upstream nodes.
    pub lhs: Arc<LogicalPlan>,
    pub rhs: Arc<LogicalPlan>,
    pub is_all: bool,
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
        is_all: bool,
    ) -> logical_plan::Result<Self> {
        if lhs.schema().len() != rhs.schema().len() {
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
        Ok(Self { lhs, rhs, is_all })
    }

    /// union could be represented as a concat + distinct
    /// while union all could be represented as a concat
    pub(crate) fn to_logical_plan(&self) -> logical_plan::Result<LogicalPlan> {
        let lhs_schema = self.lhs.schema();
        let rhs_schema = self.rhs.schema();
        let (lhs, rhs) = if lhs_schema != rhs_schema {
            // we need to try to do a type coercion
            let coerced_fields = lhs_schema
                .fields
                .values()
                .zip(rhs_schema.fields.values())
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
                .map(|f| col(f.name.clone()).cast(&f.dtype))
                .collect::<Vec<_>>();
            let lhs = Project::try_new(self.lhs.clone(), projection_fields.clone())?.into();
            let rhs = Project::try_new(self.rhs.clone(), projection_fields)?.into();
            (lhs, rhs)
        } else {
            (self.lhs.clone(), self.rhs.clone())
        };
        let concat = LogicalPlan::Concat(Concat::try_new(lhs, rhs)?);
        if self.is_all {
            Ok(concat)
        } else {
            Ok(Distinct::new(concat.arced()).into())
        }
    }
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if self.is_all {
            res.push("Union All:".to_string());
        } else {
            res.push("Union:".to_string());
        }
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Except {
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
        Ok(Self { lhs, rhs, is_all })
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
                .fields
                .keys()
                .map(|k| col(k.clone()))
                .collect::<Vec<ExprRef>>();
            // project the right cols to have the same name as the left cols
            let right_cols = self
                .rhs
                .schema()
                .fields
                .keys()
                .map(|k| col(k.clone()))
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
            let sum = col(virtual_col).sum().alias(virtual_sum);
            let aggregate_plan =
                Aggregate::try_new(union_all.into(), vec![sum], left_cols.clone())?;
            let filter_plan = Filter::try_new(aggregate_plan.into(), col(virtual_sum).gt(lit(0)))?;
            let fill_and_explodes = left_cols
                .iter()
                .map(|column| {
                    explode(list_fill(col(virtual_sum), column.clone())).alias(column.name())
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
