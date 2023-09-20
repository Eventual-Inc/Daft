use std::sync::Arc;

use daft_core::datatypes::FieldID;
use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::{optimization, AggExpr, Expr, ExprRef};
use indexmap::{IndexMap, IndexSet};
use snafu::ResultExt;

use crate::logical_plan::{CreationSnafu, Result};
use crate::optimization::Transformed;
use crate::{LogicalPlan, PartitionScheme, PartitionSpec, ResourceRequest};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Project {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub projection: Vec<Expr>,
    pub resource_request: ResourceRequest,
    pub projected_schema: SchemaRef,
    pub partition_spec: Arc<PartitionSpec>,
}

impl Project {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        projection: Vec<Expr>,
        resource_request: ResourceRequest,
    ) -> Result<Self> {
        // Factor the projection and see if there are any substitutions to factor out.
        let (factored_input, factored_projection) =
            Self::try_factor_subexpressions(input, projection, &resource_request)?;

        let upstream_schema = factored_input.schema();
        let projected_schema = {
            let fields = factored_projection
                .iter()
                .map(|e| e.to_field(&upstream_schema))
                .collect::<common_error::DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        let partition_spec =
            Self::translate_partition_spec(factored_input.partition_spec(), &factored_projection);
        Ok(Self {
            input: factored_input,
            projection: factored_projection,
            resource_request,
            projected_schema,
            partition_spec,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "Project: {}",
                self.projection
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            format!("Partition spec = {:?}", self.partition_spec),
        ]
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

    fn try_factor_subexpressions(
        input: Arc<LogicalPlan>,
        projection: Vec<Expr>,
        resource_request: &ResourceRequest,
    ) -> Result<(Arc<LogicalPlan>, Vec<Expr>)> {
        // Given construction parameters for a projection,
        // see if we can factor out common subexpressions.
        // Returns a new set of projection parameters
        // (a maybe new input node, and a maybe new list of projection expressions).
        let upstream_schema = input.schema();
        let (projection, substitutions) = Self::factor_expressions(projection, &upstream_schema);
        // If there are substitutions to factor out,
        // create a child projection node to do the factoring.
        let input = if substitutions.is_empty() {
            input
        } else {
            let child_projection = projection
                .iter()
                .flat_map(optimization::get_required_columns)
                .collect::<IndexSet<_>>()
                .into_iter()
                .map(|colname| {
                    let expr = &substitutions[&colname];
                    Expr::Alias(expr.clone().into(), colname.clone().into())
                })
                .collect::<Vec<_>>();

            let plan: LogicalPlan =
                Self::try_new(input, child_projection, resource_request.clone())?.into();
            plan.into()
        };
        Ok((input, projection))
    }

    fn factor_expressions(
        exprs: Vec<Expr>,
        schema: &Schema,
    ) -> (Vec<Expr>, IndexMap<String, Expr>) {
        // Returns
        //  1. original expressions with substitutions
        //  2. substitution definitions
        //
        // E.g. factor_expressions([a+b as e, (a+b)+c as f]) ->
        //  1. [some_id as e, some_id + c as f]
        //  2. {some_id: a+b}

        // Find all top-level repeated subexpressions;
        // these are the expressions we will cache.
        let mut seen_subexpressions = IndexSet::new();
        let mut subexpressions_to_cache = IndexMap::new();

        // While walking, also find all column name references.
        // If we do substitutions and create new semantic ID names,
        // all existing names must also be converted to semantic IDs.
        let mut column_name_substitutions = IndexMap::new();

        let mut exprs_to_walk: Vec<Arc<Expr>> = exprs.iter().map(|e| e.clone().into()).collect();
        while !exprs_to_walk.is_empty() {
            exprs_to_walk = exprs_to_walk
                .iter()
                .flat_map(|expr| {
                    // If expr is an alias, ignore and continue recursing
                    // (otherwise the unaliased child will be double counted)
                    if matches!(expr.as_ref(), Expr::Alias(..)) {
                        expr.children()
                    } else {
                        let expr_id = expr.semantic_id(schema);
                        if let Expr::Column(..) = expr.as_ref() {
                            column_name_substitutions.insert(expr_id.clone(), expr.clone());
                        }
                        // Mark expr as seen
                        let newly_seen = seen_subexpressions.insert(expr_id.clone());
                        if newly_seen {
                            // If not previously seen, continue recursing down children
                            expr.children()
                        } else {
                            // If previously seen, cache the expression (if it involves computation)
                            if optimization::requires_computation(expr) {
                                subexpressions_to_cache.insert(expr_id.clone(), expr.clone());
                            }
                            // Stop recursing if previously seen;
                            // we only want top-level repeated subexpressions
                            vec![]
                        }
                    }
                })
                .collect();
        }

        if subexpressions_to_cache.is_empty() {
            (exprs.to_vec(), IndexMap::new())
        } else {
            // Then, substitute all the cached subexpressions in the original expressions.
            let subexprs_to_replace = subexpressions_to_cache
                .keys()
                .chain(column_name_substitutions.keys())
                .cloned()
                .collect::<IndexSet<_>>();
            let substituted_expressions = exprs
                .iter()
                .map(|e| {
                    let new_expr = replace_column_with_semantic_id(
                        e.clone().into(),
                        &subexprs_to_replace,
                        schema,
                    )
                    .unwrap()
                    .as_ref()
                    .clone();
                    // The substitution can unintentionally change the expression's name
                    // (since the name depends on the first column referenced, which can be substituted away)
                    // so re-alias the original name here if it has changed.
                    let old_name = e.name().unwrap();
                    if new_expr.name().unwrap() != old_name {
                        new_expr.alias(old_name)
                    } else {
                        new_expr
                    }
                })
                .collect::<Vec<_>>();

            let substitutions = subexpressions_to_cache
                .iter()
                .chain(column_name_substitutions.iter())
                .map(|(k, v)| (k.id.as_ref().to_string(), v.as_ref().clone()))
                .collect::<IndexMap<_, _>>();

            (substituted_expressions, substitutions)
        }
    }
}

fn replace_column_with_semantic_id(
    e: ExprRef,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<ExprRef> {
    // Constructs a new copy of this expression
    // with all occurences of subexprs_to_replace replaced with a column selection.
    // e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
    //  -> Col("(a + b)") + c

    let sem_id = e.semantic_id(schema);
    if subexprs_to_replace.contains(&sem_id) {
        let new_expr = Expr::Column(sem_id.id.clone());
        let new_expr = match e.as_ref() {
            Expr::Alias(_, name) => Expr::Alias(new_expr.into(), name.clone()),
            _ => new_expr,
        };
        Transformed::Yes(new_expr.into())
    } else {
        match e.as_ref() {
            Expr::Column(_) | Expr::Literal(_) => Transformed::No(e),
            Expr::Agg(agg_expr) => replace_column_with_semantic_id_aggexpr(
                agg_expr.clone(),
                subexprs_to_replace,
                schema,
            )
            .map_yes_no(
                |transformed_child| Expr::Agg(transformed_child).into(),
                |_| e,
            ),
            Expr::Alias(child, name) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Alias(transformed_child, name.clone()).into(),
                        |_| e.clone(),
                    )
            }
            Expr::Cast(child, datatype) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Cast(transformed_child, datatype.clone()).into(),
                        |_| e.clone(),
                    )
            }
            Expr::Not(child) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::Not(transformed_child).into(),
                        |_| e,
                    )
            }
            Expr::IsNull(child) => {
                replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                    .map_yes_no(
                        |transformed_child| Expr::IsNull(transformed_child).into(),
                        |_| e,
                    )
            }
            Expr::BinaryOp { op, left, right } => {
                let left =
                    replace_column_with_semantic_id(left.clone(), subexprs_to_replace, schema);
                let right =
                    replace_column_with_semantic_id(right.clone(), subexprs_to_replace, schema);
                if left.is_no() && right.is_no() {
                    Transformed::No(e)
                } else {
                    Transformed::Yes(
                        Expr::BinaryOp {
                            op: *op,
                            left: left.unwrap().clone(),
                            right: right.unwrap().clone(),
                        }
                        .into(),
                    )
                }
            }
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let predicate =
                    replace_column_with_semantic_id(predicate.clone(), subexprs_to_replace, schema);
                let if_true =
                    replace_column_with_semantic_id(if_true.clone(), subexprs_to_replace, schema);
                let if_false =
                    replace_column_with_semantic_id(if_false.clone(), subexprs_to_replace, schema);
                if predicate.is_no() && if_true.is_no() && if_false.is_no() {
                    Transformed::No(e)
                } else {
                    Transformed::Yes(
                        Expr::IfElse {
                            predicate: predicate.unwrap().clone(),
                            if_true: if_true.unwrap().clone(),
                            if_false: if_false.unwrap().clone(),
                        }
                        .into(),
                    )
                }
            }
            Expr::Function { func, inputs } => {
                let transforms = inputs
                    .iter()
                    .map(|e| {
                        replace_column_with_semantic_id(
                            e.clone().into(),
                            subexprs_to_replace,
                            schema,
                        )
                    })
                    .collect::<Vec<_>>();
                if transforms.iter().all(|e| e.is_no()) {
                    Transformed::No(e)
                } else {
                    Transformed::Yes(
                        Expr::Function {
                            func: func.clone(),
                            inputs: transforms
                                .iter()
                                .map(|t| t.unwrap().as_ref())
                                .cloned()
                                .collect(),
                        }
                        .into(),
                    )
                }
            }
        }
    }
}

fn replace_column_with_semantic_id_aggexpr(
    e: AggExpr,
    subexprs_to_replace: &IndexSet<FieldID>,
    schema: &Schema,
) -> Transformed<AggExpr> {
    // Constructs a new copy of this expression
    // with all occurences of subexprs_to_replace replaced with a column selection.
    // e.g. e := (a+b)+c, subexprs := {FieldID("(a + b)")}
    //  -> Col("(a + b)") + c

    match e {
        AggExpr::Count(ref child, mode) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema).map_yes_no(
                |transformed_child| AggExpr::Count(transformed_child, mode),
                |_| e.clone(),
            )
        }
        AggExpr::Sum(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Sum, |_| e.clone())
        }
        AggExpr::Mean(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Mean, |_| e.clone())
        }
        AggExpr::Min(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Min, |_| e.clone())
        }
        AggExpr::Max(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Max, |_| e.clone())
        }
        AggExpr::List(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::List, |_| e.clone())
        }
        AggExpr::Concat(ref child) => {
            replace_column_with_semantic_id(child.clone(), subexprs_to_replace, schema)
                .map_yes_no(AggExpr::Concat, |_| e.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{binary_op, col, lit, Expr, Operator};

    use crate::{
        logical_ops::Project, test::dummy_scan_node, LogicalPlan, PartitionScheme, PartitionSpec,
    };

    /// Test that nested common subexpressions are correctly split
    /// into multiple levels of projections.
    /// e.g.
    /// ((a+a)+(a+a))+((a+a)+(a+a)) as x
    /// ->
    /// 1. aaaa+aaaa as x
    /// 2. aa+aa as aaaa
    /// 3: a+a as aa
    #[test]
    fn test_nested_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .build();
        let a2 = binary_op(Operator::Plus, &col("a"), &col("a"));
        let a4 = binary_op(Operator::Plus, &a2, &a2);
        let a8 = binary_op(Operator::Plus, &a4, &a4);
        let expressions = vec![a8.alias("x")];
        let result_projection = Project::try_new(source.clone(), expressions, Default::default())?;

        let a4_colname = a4.semantic_id(&source.schema()).id;
        let a4_col = col(a4_colname.clone());
        let expected_result_projection =
            vec![binary_op(Operator::Plus, &a4_col, &a4_col).alias("x")];
        assert_eq!(result_projection.projection, expected_result_projection);

        let a2_colname = a2.semantic_id(&source.schema()).id;
        let a2_col = col(a2_colname.clone());
        let expected_subprojection =
            vec![binary_op(Operator::Plus, &a2_col, &a2_col).alias(a4_colname.clone())];
        let LogicalPlan::Project(subprojection) = result_projection.input.as_ref() else {
            panic!()
        };
        assert_eq!(subprojection.projection, expected_subprojection);

        let expected_third_projection = vec![a2.alias(a2_colname.clone())];
        let LogicalPlan::Project(third_projection) = subprojection.input.as_ref() else {
            panic!()
        };
        assert_eq!(third_projection.projection, expected_third_projection);

        Ok(())
    }

    /// Test that common subexpressions are correctly identified
    /// across separate expressions.
    /// e.g.
    /// (a+a) as x, (a+a)+a as y
    /// ->
    /// 1. aa as x, aa+a as y
    /// 2. a+a as aa, a
    #[test]
    fn test_shared_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .build();
        let a2 = binary_op(Operator::Plus, &col("a"), &col("a"));
        let expressions = vec![
            a2.alias("x"),
            binary_op(Operator::Plus, &a2, &col("a")).alias("y"),
        ];
        let result_projection = Project::try_new(source.clone(), expressions, Default::default())?;

        let a2_colname = a2.semantic_id(&source.schema()).id;
        let a2_col = col(a2_colname.clone());
        let expected_result_projection = vec![
            a2_col.alias("x"),
            binary_op(Operator::Plus, &a2_col, &col("a")).alias("y"),
        ];
        assert_eq!(result_projection.projection, expected_result_projection);

        let expected_subprojection = vec![a2.alias(a2_colname.clone()), col("a").alias("a")];
        let LogicalPlan::Project(subprojection) = result_projection.input.as_ref() else {
            panic!()
        };
        assert_eq!(subprojection.projection, expected_subprojection);

        Ok(())
    }

    /// Test that common leaf expressions are not factored out
    /// (since this would not save computation and only introduces another materialization)
    /// e.g.
    /// 3 as x, 3 as y, a as w, a as z
    /// ->
    /// (unchanged)
    #[test]
    fn test_vacuous_subexpression() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .build();
        let expressions = vec![
            lit(3).alias("x"),
            lit(3).alias("y"),
            col("a").alias("w"),
            col("a").alias("z"),
        ];
        let result_projection = Project::try_new(source, expressions.clone(), Default::default())?;

        assert_eq!(result_projection.projection, expressions);

        Ok(())
    }

    /// Test that projections preserving column inputs, even through aliasing,
    /// do not destroy the partition spec.
    #[test]
    fn test_partition_spec_preserving() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            3,
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .build();

        let expressions = vec![
            (col("a") % lit(2)), // this is now "a"
            col("b"),
            col("a").alias("aa"),
        ];

        let result_projection = Project::try_new(source, expressions, Default::default())?;

        let expected_pspec =
            PartitionSpec::new_internal(PartitionScheme::Hash, 3, Some(vec![col("aa"), col("b")]));

        assert_eq!(
            expected_pspec,
            result_projection.partition_spec.as_ref().clone()
        );

        Ok(())
    }

    /// Test that projections destroying even a single column input from the partition spec
    /// destroys the entire partition spec.
    #[test]
    fn test_partition_spec_destroying() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            3,
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .build();

        let expected_pspec = PartitionSpec::new_internal(PartitionScheme::Unknown, 3, None);

        let test_cases = vec![
            vec![col("a"), col("c").alias("b")], // original "b" is gone even though "b" is present
            vec![col("b")],                      // original "a" dropped
            vec![col("a") % lit(2), col("b")],   // original "a" gone
            vec![col("c")],                      // everything gone
        ];

        for projection in test_cases {
            let result_projection =
                Project::try_new(source.clone(), projection, Default::default())?;
            assert_eq!(
                expected_pspec,
                result_projection.partition_spec.as_ref().clone()
            );
        }

        Ok(())
    }

    /// Test that new partition specs favor existing instead of new names.
    /// i.e. ("a", "a" as "b") remains partitioned by "a", not "b"
    #[test]
    fn test_partition_spec_prefer_existing_names() -> DaftResult<()> {
        let source = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            3,
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionScheme::Hash,
        )?
        .build();

        let expressions = vec![col("a").alias("y"), col("a"), col("a").alias("z"), col("b")];

        let result_projection = Project::try_new(source, expressions, Default::default())?;

        let expected_pspec =
            PartitionSpec::new_internal(PartitionScheme::Hash, 3, Some(vec![col("a"), col("b")]));

        assert_eq!(
            expected_pspec,
            result_projection.partition_spec.as_ref().clone()
        );

        Ok(())
    }
}
