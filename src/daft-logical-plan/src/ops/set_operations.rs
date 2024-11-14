use std::sync::Arc;

use common_error::DaftError;
use daft_core::join::JoinType;
use daft_dsl::col;
use snafu::ResultExt;

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
