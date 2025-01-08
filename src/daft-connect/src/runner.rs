use daft_logical_plan::LogicalPlanBuilder;
use daft_ray_execution::RayRunnerShim;

pub enum Runner {
    Ray(RayRunnerShim),
    Native,
}


impl Runner {
    // pub fn run(lp: LogicalPlanBuilder) -> DaftResult<
}