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
        let mut slack = self.current_capacity.clone();
        slack.subtract(&self.current_usage);
        let exec_resource_request = resource_request.into();
        slack.can_hold(&exec_resource_request)
    }

    pub fn admit(&mut self, resource_request: &ResourceRequest) {
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

#[derive(Clone, Debug, Default)]
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

impl From<&ResourceRequest> for ExecutionResources {
    fn from(value: &ResourceRequest) -> Self {
        Self::new(
            value.num_cpus.unwrap_or(0.0),
            value.num_gpus.unwrap_or(0.0),
            value.memory_bytes.unwrap_or(0),
        )
    }
}
