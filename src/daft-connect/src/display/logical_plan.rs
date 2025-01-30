use std::fmt;

use common_error::DaftResult;
use daft_dsl::common_treenode::TreeNodeVisitor;
use daft_logical_plan::LogicalPlanRef;

struct SparkDisplayVisitor<'a, W: fmt::Write + 'a> {
    output: &'a mut W,
    /// The current depth of the tree
    depth: usize,
}

fn write_indent<'a, W: fmt::Write + 'a>(level: usize, output: &mut W) -> fmt::Result {
    if level == 0 {
        Ok(())
    } else if level == 1 {
        write!(output, "+- ")
    } else {
        write!(output, "{:indent$}+- ", "", indent = 2 * (level - 1))
    }
}

fn write_node<'a, W: fmt::Write + 'a>(node: &LogicalPlanRef, output: &mut W) -> fmt::Result {
    match node.as_ref() {
        daft_logical_plan::LogicalPlan::Source(_) => todo!(),
        daft_logical_plan::LogicalPlan::Project(_) => todo!(),
        daft_logical_plan::LogicalPlan::ActorPoolProject(_) => todo!(),
        daft_logical_plan::LogicalPlan::Filter(_) => todo!(),
        daft_logical_plan::LogicalPlan::Limit(_) => todo!(),
        daft_logical_plan::LogicalPlan::Explode(_) => todo!(),
        daft_logical_plan::LogicalPlan::Unpivot(_) => todo!(),
        daft_logical_plan::LogicalPlan::Sort(sort) => {
            write!(output, "Sort [")?;

            let mut is_first = true;
            for ((sort, desc), nulls_first) in sort
                .sort_by
                .iter()
                .zip(sort.descending.iter())
                .zip(sort.nulls_first.iter())
            {
                if !is_first {
                    write!(output, ", ")?;
                } else {
                    is_first = false;
                }

                write!(
                    output,
                    "{name} {asc} {nulls_first}",
                    name = sort.name(),
                    asc = if *desc { "DESC" } else { "ASC" },
                    nulls_first = if *nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    }
                )?;
            }
            write!(output, "]")?;

            Ok(())
        }
        daft_logical_plan::LogicalPlan::Repartition(_) => todo!(),
        daft_logical_plan::LogicalPlan::Distinct(_) => todo!(),
        daft_logical_plan::LogicalPlan::Aggregate(_) => todo!(),
        daft_logical_plan::LogicalPlan::Pivot(_) => todo!(),
        daft_logical_plan::LogicalPlan::Concat(_) => todo!(),
        daft_logical_plan::LogicalPlan::Intersect(_) => todo!(),
        daft_logical_plan::LogicalPlan::Union(_) => todo!(),
        daft_logical_plan::LogicalPlan::Join(_) => todo!(),
        daft_logical_plan::LogicalPlan::Sink(_) => todo!(),
        daft_logical_plan::LogicalPlan::Sample(_) => todo!(),
        daft_logical_plan::LogicalPlan::MonotonicallyIncreasingId(_) => todo!(),
    }
}

impl<'a, W: fmt::Write + 'a> TreeNodeVisitor for SparkDisplayVisitor<'a, W> {
    type Node = LogicalPlanRef;

    fn f_down(
        &mut self,
        _node: &Self::Node,
    ) -> DaftResult<daft_dsl::common_treenode::TreeNodeRecursion> {
        self.depth += 1;
        write_indent(self.depth, self.output)?;

        Ok(daft_dsl::common_treenode::TreeNodeRecursion::Continue)
    }

    fn f_up(
        &mut self,
        _node: &Self::Node,
    ) -> DaftResult<daft_dsl::common_treenode::TreeNodeRecursion> {
        todo!()
    }
}

fn fmt_logical_plan<'a, W: fmt::Write + 'a>(
    logical_plan: LogicalPlanRef,
    s: &'a mut W,
) -> fmt::Result {
    let mut indent = 0;

    todo!()
}
