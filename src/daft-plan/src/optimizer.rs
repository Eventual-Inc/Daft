use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_treenode::Transformed;

pub trait OptimizerRule<T> {
    fn apply(&self, plan: Arc<T>) -> DaftResult<Transformed<Arc<T>>>;
}

pub struct RuleBatch<T> {
    pub name: String,
    pub rules: Vec<Box<dyn OptimizerRule<T>>>,
    pub strategy: RuleBatchStrategy,
}

impl<T> RuleBatch<T> {
    pub fn new(
        name: impl ToString,
        rules: Vec<Box<dyn OptimizerRule<T>>>,
        strategy: RuleBatchStrategy,
    ) -> Self {
        Self {
            name: name.to_string(),
            rules,
            strategy,
        }
    }

    pub fn apply_rules(&self, plan: Arc<T>) -> DaftResult<Transformed<Arc<T>>> {
        self.rules.iter().try_fold(Transformed::no(plan), |p, r| {
            p.transform_data(|data| r.apply(data))
        })
    }
}

#[derive(PartialEq, Eq)]
pub enum RuleBatchStrategy {
    Once,
    FixedPoint {
        max_passes: usize,
        error_on_exceed: bool,
    },
}

impl Default for RuleBatchStrategy {
    fn default() -> Self {
        RuleBatchStrategy::FixedPoint {
            max_passes: 5,
            error_on_exceed: false,
        }
    }
}

pub trait Optimizer<T> {
    fn batches(&self) -> impl IntoIterator<Item = RuleBatch<T>>;

    fn execute(&self, plan: Arc<T>) -> DaftResult<Arc<T>> {
        self.batches().into_iter().try_fold(plan, |plan, batch| {
            match batch.strategy {
                RuleBatchStrategy::Once => {
                    let new_plan = batch.apply_rules(plan)?;

                    if new_plan.transformed {
                        // check idempotence of batches that run once
                        let retried_plan = batch.apply_rules(new_plan.data)?;

                        if retried_plan.transformed {
                            Err(DaftError::InternalError(format!(
                                "Optimizer rule batch \"{}\" did not converge after one iteration",
                                batch.name
                            )))
                        } else {
                            Ok(retried_plan.data)
                        }
                    } else {
                        // no need to check idempotence if not transformed
                        Ok(new_plan.data)
                    }
                }
                RuleBatchStrategy::FixedPoint {
                    max_passes,
                    error_on_exceed,
                } => {
                    let mut curr_plan = plan;
                    let mut iterations = 0;

                    loop {
                        let new_plan = batch.apply_rules(curr_plan)?;

                        curr_plan = new_plan.data;

                        // exit loop early if stable
                        if !new_plan.transformed {
                            break;
                        }

                        iterations += 1;

                        if iterations >= max_passes {
                            if error_on_exceed {
                                return Err(DaftError::InternalError(format!(
                                    "Optimizer rule batch \"{}\" exceeded max iterations",
                                    batch.name
                                )));
                            } else {
                                break;
                            }
                        }
                    }

                    Ok(curr_plan)
                }
            }
        })
    }
}
