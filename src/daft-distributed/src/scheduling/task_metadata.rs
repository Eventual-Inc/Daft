use daft_local_plan::Input;

use super::task::{
    InMemoryScanSource, PhysicalScanSource, SwordfishTask, TaskMetadata, TaskSource,
};

impl SwordfishTask {
    pub(crate) fn build_metadata(&self) -> TaskMetadata {
        let sources: Vec<TaskSource> = self.task_sources();
        TaskMetadata { sources }
    }

    fn task_sources(&self) -> Vec<TaskSource> {
        let inputs = self.inputs();
        let mut sources: Vec<TaskSource> = Vec::with_capacity(inputs.len() + self.psets().len());
        for (source_id, input) in inputs {
            match input {
                Input::ScanTasks(scan_tasks) => {
                    let mut paths: Vec<String> = Vec::new();
                    let mut storage_bytes: Option<usize> = None;
                    let mut estimated_memory_bytes: Option<usize> = None;
                    for scan_task in scan_tasks {
                        paths.extend(scan_task.get_file_paths().iter().cloned());
                        if let Some(size) = scan_task.size_bytes_on_disk() {
                            storage_bytes = Some(storage_bytes.unwrap_or(0) + size);
                        }
                        if let Some(memory_bytes) =
                            scan_task.estimate_in_memory_size_bytes(Some(self.config()))
                        {
                            estimated_memory_bytes =
                                Some(estimated_memory_bytes.unwrap_or(0) + memory_bytes);
                        }
                    }

                    sources.push(TaskSource::PhysicalScan(PhysicalScanSource {
                        source_id: *source_id,
                        scan_tasks: scan_tasks.len() as u32,
                        paths,
                        storage_bytes,
                        estimated_memory_bytes,
                    }));
                }
                // TODO: support glob and flight shuffle inputs
                Input::GlobPaths(_paths) => {}
                Input::FlightShuffle(_inputs) => {}
                Input::InMemory(_) => {
                    debug_assert!(
                        false,
                        "Input::InMemory should never appear in SwordfishTask.inputs; in-memory data lives in psets"
                    );
                }
            }
        }

        // Get in memory sources
        for (source_id, pset) in self.psets() {
            let partitions = pset.len();
            let total_bytes = pset.iter().map(|p| p.size_bytes()).sum();
            sources.push(TaskSource::InMemoryScan(InMemoryScanSource {
                source_id: *source_id,
                partitions,
                total_bytes: Some(total_bytes),
            }));
        }

        sources
    }
}
