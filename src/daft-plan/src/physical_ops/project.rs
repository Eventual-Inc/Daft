use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::Expr;
use indexmap::IndexMap;

use crate::{physical_plan::PhysicalPlan, PartitionScheme, PartitionSpec, ResourceRequest};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
    pub partition_spec: Arc<PartitionSpec>,
}

impl Project {
    pub(crate) fn try_new(
        input: Arc<PhysicalPlan>,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
        partition_spec: Arc<PartitionSpec>,
    ) -> DaftResult<Self> {
        let partition_spec = Self::translate_partition_spec(partition_spec, &projection);
        Ok(Self {
            input,
            projection,
            resource_request,
            partition_spec,
        })
    }

    fn translate_partition_spec(
        input_pspec: Arc<PartitionSpec>,
        projection: &Vec<Expr>,
    ) -> Arc<PartitionSpec> {
        // Given an input partition spec, and a new projection,
        // produce the new partition spec.

        use crate::PartitionScheme::*;
        match input_pspec.scheme {
            // If the scheme is vacuous, the result partiiton spec is the same.
            Random | Unknown => input_pspec.clone(),
            // Otherwise, need to reevaluate the partition scheme for each expression.
            Range | Hash => {
                // See what columns the projection directly translates into new columns.
                let mut old_colname_to_new_colname = IndexMap::new();
                for expr in projection {
                    if let Some(oldname) = expr.input_mapping() {
                        let newname = expr.name().unwrap().to_string();
                        // Add the oldname -> newname mapping,
                        // but don't overwrite any existing identity mappings (e.g. "a" -> "a").
                        if old_colname_to_new_colname.get(&oldname) != Some(&oldname) {
                            old_colname_to_new_colname.insert(oldname, newname);
                        }
                    }
                }

                // Then, see if we can fully translate the partition spec.
                let maybe_new_pspec = input_pspec
                    .by
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|e| Self::translate_partition_spec_expr(e, &old_colname_to_new_colname))
                    .collect::<std::result::Result<Vec<_>, _>>();
                maybe_new_pspec.map_or_else(
                    |()| {
                        PartitionSpec::new_internal(
                            PartitionScheme::Unknown,
                            input_pspec.num_partitions,
                            None,
                        )
                        .into()
                    },
                    |new_pspec: Vec<Expr>| {
                        PartitionSpec::new_internal(
                            input_pspec.scheme.clone(),
                            input_pspec.num_partitions,
                            Some(new_pspec),
                        )
                        .into()
                    },
                )
            }
        }
    }

    fn translate_partition_spec_expr(
        pspec_expr: &Expr,
        old_colname_to_new_colname: &IndexMap<String, String>,
    ) -> std::result::Result<Expr, ()> {
        // Given a single expression of an input partition spec,
        // translate it to a new expression in the given projection.
        // Returns:
        //  - Ok(expr) with expr being the translation, or
        //  - Err(()) if no translation is possible in the new projection.

        match pspec_expr {
            Expr::Column(name) => match old_colname_to_new_colname.get(name.as_ref()) {
                Some(newname) => Ok(Expr::Column(newname.as_str().into())),
                None => Err(()),
            },
            Expr::Literal(_) => Ok(pspec_expr.clone()),
            Expr::Alias(child, name) => {
                let newchild = Self::translate_partition_spec_expr(
                    child.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::Alias(newchild.into(), name.clone()))
            }
            Expr::BinaryOp { op, left, right } => {
                let newleft =
                    Self::translate_partition_spec_expr(left.as_ref(), old_colname_to_new_colname)?;
                let newright = Self::translate_partition_spec_expr(
                    right.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::BinaryOp {
                    op: *op,
                    left: newleft.into(),
                    right: newright.into(),
                })
            }
            Expr::Cast(child, dtype) => {
                let newchild = Self::translate_partition_spec_expr(
                    child.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::Cast(newchild.into(), dtype.clone()))
            }
            Expr::Function { func, inputs } => {
                let new_inputs = inputs
                    .iter()
                    .map(|e| Self::translate_partition_spec_expr(e, old_colname_to_new_colname))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expr::Function {
                    func: func.clone(),
                    inputs: new_inputs,
                })
            }
            Expr::Not(child) => {
                let newchild = Self::translate_partition_spec_expr(
                    child.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::Not(newchild.into()))
            }
            Expr::IsNull(child) => {
                let newchild = Self::translate_partition_spec_expr(
                    child.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::IsNull(newchild.into()))
            }
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let newtrue = Self::translate_partition_spec_expr(
                    if_true.as_ref(),
                    old_colname_to_new_colname,
                )?;
                let newfalse = Self::translate_partition_spec_expr(
                    if_false.as_ref(),
                    old_colname_to_new_colname,
                )?;
                let newpred = Self::translate_partition_spec_expr(
                    predicate.as_ref(),
                    old_colname_to_new_colname,
                )?;
                Ok(Expr::IfElse {
                    if_true: newtrue.into(),
                    if_false: newfalse.into(),
                    predicate: newpred.into(),
                })
            }
            // Cannot have agg exprs in partition specs.
            Expr::Agg(_) => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit, Expr};
    use rstest::rstest;

    use crate::{planner::plan, test::dummy_scan_node, PartitionScheme, PartitionSpec};

    /// Test that projections preserving column inputs, even through aliasing,
    /// do not destroy the partition spec.
    #[test]
    fn test_partition_spec_preserving() -> DaftResult<()> {
        let expressions = vec![
            (col("a") % lit(2)), // this is now "a"
            col("b"),
            col("a").alias("aa"),
        ];
        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            Some(3),
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .project(expressions, Default::default())?
        .build();

        let physical_plan = plan(&logical_plan)?;

        let expected_pspec =
            PartitionSpec::new_internal(PartitionScheme::Hash, 3, Some(vec![col("aa"), col("b")]));

        assert_eq!(
            expected_pspec,
            physical_plan.partition_spec().as_ref().clone()
        );

        Ok(())
    }

    /// Test that projections destroying even a single column input from the partition spec
    /// destroys the entire partition spec.
    #[rstest]
    fn test_partition_spec_destroying(
        #[values(
            vec![col("a"), col("c").alias("b")], // original "b" is gone even though "b" is present
            vec![col("b")],                      // original "a" dropped
            vec![col("a") % lit(2), col("b")],   // original "a" gone
            vec![col("c")],                      // everything gone
        )]
        projection: Vec<Expr>,
    ) -> DaftResult<()> {
        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            Some(3),
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .project(projection, Default::default())?
        .build();

        let physical_plan = plan(&logical_plan)?;

        let expected_pspec = PartitionSpec::new_internal(PartitionScheme::Unknown, 3, None);
        assert_eq!(
            expected_pspec,
            physical_plan.partition_spec().as_ref().clone()
        );

        Ok(())
    }

    /// Test that new partition specs favor existing instead of new names.
    /// i.e. ("a", "a" as "b") remains partitioned by "a", not "b"
    #[test]
    fn test_partition_spec_prefer_existing_names() -> DaftResult<()> {
        let expressions = vec![col("a").alias("y"), col("a"), col("a").alias("z"), col("b")];

        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            Some(3),
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .project(expressions, Default::default())?
        .build();

        let physical_plan = plan(&logical_plan)?;

        let expected_pspec =
            PartitionSpec::new_internal(PartitionScheme::Hash, 3, Some(vec![col("a"), col("b")]));

        assert_eq!(
            expected_pspec,
            physical_plan.partition_spec().as_ref().clone()
        );

        Ok(())
    }
}
