use daft_plan::ResourceRequest;

#[derive(Debug)]
pub struct ResourceManager {
    current_capacity: ExecutionResources,
    current_usage: ExecutionResources,
}

impl ResourceManager {
    pub fn new(resource_capacity: ExecutionResources) -> Self {
        Self {
            current_capacity: resource_capacity,
            current_usage: Default::default(),
        }
    }

    pub fn can_admit(&self, resource_request: &ResourceRequest) -> bool {
        let mut usage_with_request: ExecutionResources = resource_request.into();
        usage_with_request.add(&self.current_usage);
        self.current_capacity.can_hold(&usage_with_request)
    }

    pub fn admit(&mut self, resource_request: &ResourceRequest) {
        assert!(self.can_admit(resource_request));
        let exec_resource_request = resource_request.into();
        self.current_usage.add(&exec_resource_request)
    }

    pub fn release(&mut self, resource_request: &ResourceRequest) {
        let exec_resource_request = resource_request.into();
        self.current_usage.subtract(&exec_resource_request)
    }

    pub fn update_capacity(&mut self, new_capacity: ExecutionResources) {
        self.current_capacity = new_capacity;
    }

    pub fn current_capacity(&self) -> &ExecutionResources {
        &self.current_capacity
    }

    pub fn current_utilization(&self) -> &ExecutionResources {
        &self.current_usage
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExecutionResources {
    num_cpus: f64,
    num_gpus: f64,
    heap_memory_bytes: usize,
}

impl ExecutionResources {
    pub fn new(num_cpus: f64, num_gpus: f64, heap_memory_bytes: usize) -> Self {
        Self {
            num_cpus,
            num_gpus,
            heap_memory_bytes,
        }
    }
    pub fn can_hold(&self, request: &ExecutionResources) -> bool {
        request.num_cpus <= self.num_cpus
            && request.num_gpus <= self.num_gpus
            && request.heap_memory_bytes <= self.heap_memory_bytes
    }

    pub fn subtract(&mut self, request: &ExecutionResources) {
        self.num_cpus -= request.num_cpus;
        self.num_gpus -= request.num_gpus;
        self.heap_memory_bytes -= request.heap_memory_bytes;
    }

    pub fn add(&mut self, request: &ExecutionResources) {
        self.num_cpus += request.num_cpus;
        self.num_gpus += request.num_gpus;
        self.heap_memory_bytes += request.heap_memory_bytes;
    }
}

impl Eq for ExecutionResources {}

impl From<&ResourceRequest> for ExecutionResources {
    fn from(value: &ResourceRequest) -> Self {
        Self::new(
            value.num_cpus.unwrap_or(0.0),
            value.num_gpus.unwrap_or(0.0),
            value.memory_bytes.unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_plan::ResourceRequest;
    use rstest::rstest;

    use super::{ExecutionResources, ResourceManager};

    /// Tests resource request fitting within resource capacity.
    #[rstest]
    #[case((0.0, 0.0, 0), (0.0, 0.0, 0), true)]
    #[case((0.0, 0.0, 0), (1.0, 1.0, 1), false)]
    #[case((1.0, 1.0, 1024), (0.5, 0.5, 512), true)]
    #[case((1.0, 1.0, 1024), (1.0, 1.0, 1024), true)]
    #[case((1.0, 1.0, 1024), (1.1, 1.0, 1024), false)]
    #[case((1.0, 1.0, 1024), (1.0, 1.1, 1024), false)]
    #[case((1.0, 1.0, 1024), (1.0, 1.0, 2048), false)]
    fn can_hold(
        #[case] capacity: (f64, f64, usize),
        #[case] request: (f64, f64, usize),
        #[case] should_hold: bool,
    ) -> DaftResult<()> {
        let (num_cpus, num_gpus, memory_bytes) = capacity;
        let capacity = ExecutionResources::new(num_cpus, num_gpus, memory_bytes);
        let (num_cpus, num_gpus, memory_bytes) = request;
        let request = ExecutionResources::new(num_cpus, num_gpus, memory_bytes);
        assert_eq!(capacity.can_hold(&request), should_hold);
        Ok(())
    }

    /// Tests adding and subtracting execution resources from capacity.
    #[test]
    fn add_sub() -> DaftResult<()> {
        let request = ExecutionResources::new(1.0, 1.0, 1024);
        let mut capacity = ExecutionResources::new(0.0, 0.0, 0);
        assert!(!capacity.can_hold(&request));
        capacity.add(&ExecutionResources::new(1.0, 1.0, 1024));
        assert!(capacity.can_hold(&request));
        assert_eq!(capacity, request);
        let half = ExecutionResources::new(0.5, 0.5, 512);
        capacity.subtract(&half);
        assert!(!capacity.can_hold(&request));
        assert_eq!(capacity, half);
        Ok(())
    }

    /// Tests resource request admission by resource manager.
    #[rstest]
    #[case((0.0, 0.0, 0), (0.0, 0.0, 0), (0.0, 0.0, 0), true)]
    #[case((0.0, 0.0, 0), (0.0, 0.0, 0), (1.0, 1.0, 1024), false)]
    #[case((1.0, 1.0, 1024), (0.0, 0.0, 0), (1.0, 1.0, 1024), true)]
    #[case((1.0, 1.0, 1024), (1.0, 1.0, 1024), (1.0, 1.0, 1024), false)]
    #[case((2.0, 2.0, 2048), (1.0, 1.0, 1024), (1.0, 1.0, 1024), true)]
    fn can_admit(
        #[case] capacity: (f64, f64, usize),
        #[case] usage: (f64, f64, usize),
        #[case] request: (f64, f64, usize),
        #[case] should_hold: bool,
    ) -> DaftResult<()> {
        let (num_cpus, num_gpus, memory_bytes) = capacity;
        let capacity = ExecutionResources::new(num_cpus, num_gpus, memory_bytes);
        let (num_cpus, num_gpus, memory_bytes) = usage;
        let usage =
            ResourceRequest::new_internal(Some(num_cpus), Some(num_gpus), Some(memory_bytes));
        let usage_ex_res: ExecutionResources = (&usage).into();
        // This should be guaranteed by test case definitions.
        assert!(capacity.can_hold(&usage_ex_res));
        let mut resource_manager = ResourceManager::new(capacity);
        // Set usage.
        resource_manager.admit(&usage);
        let (num_cpus, num_gpus, memory_bytes) = request;
        let request =
            ResourceRequest::new_internal(Some(num_cpus), Some(num_gpus), Some(memory_bytes));
        if should_hold {
            // Resource request should fit.
            // Confirm that admission of resource request is allowed.
            assert!(resource_manager.can_admit(&request));
            // Simulate admitting and releasing resource request.
            resource_manager.admit(&request);
            resource_manager.release(&request);
            // Check that resource request can be admitted after admitting and releasing.
            assert!(resource_manager.can_admit(&request));
        } else {
            // Resource request shouldn't fit.
            // Confirm that resource request is not allowed.
            assert!(!resource_manager.can_admit(&request));
        }
        Ok(())
    }
}
