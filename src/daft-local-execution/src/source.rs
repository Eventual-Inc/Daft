use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;
use tokio::sync::mpsc;

pub enum Source {
    Data(Vec<Arc<MicroPartition>>),
    ScanTask(Vec<Arc<ScanTask>>),
    Receiver(mpsc::Receiver<Vec<Arc<MicroPartition>>>),
}

pub enum Morsel {
    Data(Vec<Arc<MicroPartition>>),
    ScanTask(Arc<ScanTask>),
}
