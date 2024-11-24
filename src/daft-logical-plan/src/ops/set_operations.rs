use std::sync::Arc;

use common_error::DaftError;
use daft_core::{join::JoinType, utils::supertype::get_supertype};
use daft_dsl::col;
use daft_schema::field::Field;
use snafu::ResultExt;

use super::{Concat, Distinct, Project};
use crate::{logical_plan, logical_plan::CreationSnafu, LogicalPlan};

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
        if lhs_schema.len() != rhs_schema.len() {
            return Err(DaftError::SchemaMismatch(format!(
                "Both plans must have the same num of fields to intersect, \
                but got[lhs: {} v.s rhs: {}], lhs schema: {}, rhs schema: {}",
                lhs_schema.len(),
                rhs_schema.len(),
                lhs_schema,
                rhs_schema
            )))
            .context(CreationSnafu);
        }
        // lhs and rhs should have the same type for each field to intersect
        if lhs_schema
            .fields
            .values()
            .zip(rhs_schema.fields.values())
            .any(|(l, r)| l.dtype != r.dtype)
        {
            return Err(DaftError::SchemaMismatch(format!(
                "Both plans' schemas should have the same type for each field to intersect, \
                but got lhs schema: {}, rhs schema: {}",
                lhs_schema, rhs_schema
            )))
            .context(CreationSnafu);
        }
        Ok(Self { lhs, rhs, is_all })
    }

    /// intersect distinct could be represented as a semi join + distinct
    /// the following intersect operator:
    /// ```sql
    /// select a1, a2 from t1 intersect select b1, b2 from t2
    /// ```
    /// is the same as:
    /// ```sql
    /// select distinct a1, a2 from t1 left semi join t2
    ///   on t1.a1 <> t2.b1 and t1.a2 <> t2.b2
    /// ```
    /// TODO: Move this logical to logical optimization rules
    pub(crate) fn to_optimized_join(&self) -> logical_plan::Result<LogicalPlan> {
        if self.is_all {
            Err(logical_plan::Error::CreationError {
                source: DaftError::InternalError("intersect all is not supported yet".to_string()),
            })
        } else {
            let left_on = self
                .lhs
                .schema()
                .fields
                .keys()
                .map(|k| col(k.clone()))
                .collect();
            let right_on = self
                .rhs
                .schema()
                .fields
                .keys()
                .map(|k| col(k.clone()))
                .collect();
            let join = logical_plan::Join::try_new(
                self.lhs.clone(),
                self.rhs.clone(),
                left_on,
                right_on,
                Some(vec![true; self.lhs.schema().fields.len()]),
                JoinType::Semi,
                None,
                None,
                None,
                false,
            );
            join.map(|j| logical_plan::Distinct::new(j.into()).into())
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
        // we don't want to use `try_new` as we have already checked the schema
        let concat = LogicalPlan::Concat(Concat {
            input: lhs,
            other: rhs,
        });
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
