use common_scan_info::ScanTaskLikeRef;
use daft_local_plan::LocalPhysicalPlanRef;

struct DistributedPhysicalPlan {
    local_physical_plan: LocalPhysicalPlanRef,
    inputs: Vec<DistributedPlanInput>,
}

enum DistributedPlanInput {
    ScanTask(ScanTaskLikeRef),
}
