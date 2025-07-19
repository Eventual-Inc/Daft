use std::{fmt::Debug, sync::Arc};

use crate::{wrap_scan_operator::PushdownEnabledScanOperator, Pushdowns, ScanOperator}; // 添加ScanOperator导入

pub trait ScanBuilder {
    fn build(&self, scan_operator: Arc<dyn ScanOperator>) -> Arc<dyn ScanOperator> {
        if scan_operator.can_absorb_filter() {
            Arc::new(PushdownEnabledScanOperator::new(scan_operator))
        } else {
            scan_operator
        }
    }

    fn pushdowns(&self) -> Pushdowns;
}

#[derive(Debug, Clone)]
pub struct ScanBuilderImpl {
    pushdowns: Pushdowns,
}

impl ScanBuilder for ScanBuilderImpl {
    fn pushdowns(&self) -> Pushdowns {
        self.pushdowns.clone()
    }
}

impl ScanBuilderImpl {
    pub fn new(pushdowns: Pushdowns) -> Self {
        Self { pushdowns }
    }
}
