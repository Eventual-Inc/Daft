use std::{collections::BTreeMap, sync::Arc};

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use tokio::sync::{mpsc, Mutex};

use crate::{
    intermediate_ops::intermediate_op::IntermediateOperator,
    sinks::sink::{Sink, SinkResultType},
    source::{Morsel, Source},
};

struct Producer {
    id: usize,
    input: Source,
    final_tx: mpsc::Sender<(usize, usize, Vec<Arc<MicroPartition>>)>,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
}

impl Producer {
    async fn run(self) {
        let mut handles = vec![];
        match self.input {
            Source::Data(data) => {
                for value in data {
                    let final_tx = self.final_tx.clone();
                    let part_idx = handles.len();

                    // Spawn a consumer for each morsel
                    let handle = tokio::spawn(
                        Consumer::new(
                            self.id,
                            Morsel::Data(vec![value]),
                            final_tx,
                            part_idx,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);
                }
            }
            Source::Receiver(mut rx) => {
                while let Some(value) = rx.recv().await {
                    let final_tx = self.final_tx.clone();
                    let id = handles.len();

                    // Spawn a consumer for each morsel
                    let handle = tokio::spawn(
                        Consumer::new(
                            self.id,
                            Morsel::Data(value),
                            final_tx,
                            id,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);
                }
            }
            Source::ScanTask(scan_tasks) => {
                for scan_task in scan_tasks {
                    let final_tx = self.final_tx.clone();
                    let part_idx = handles.len();

                    // Spawn a consumer for each morsel
                    let handle = tokio::spawn(
                        Consumer::new(
                            self.id,
                            Morsel::ScanTask(scan_task.clone()),
                            final_tx,
                            part_idx,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);
                }
            }
        }

        // Wait for all consumers to finish
        for handle in handles {
            handle.await.unwrap();
        }
    }
}

struct Consumer {
    id: usize,
    input: Morsel,
    tx: mpsc::Sender<(usize, usize, Vec<Arc<MicroPartition>>)>,
    part_idx: usize,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
}

impl Consumer {
    fn new(
        id: usize,
        input: Morsel,
        tx: mpsc::Sender<(usize, usize, Vec<Arc<MicroPartition>>)>,
        part_idx: usize,
        intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    ) -> Self {
        Self {
            id,
            input,
            tx,
            part_idx,
            intermediate_operators,
        }
    }

    async fn run(self) {
        match self.input {
            Morsel::Data(value) => {
                // Apply intermediate operators
                let mut results = value;
                for intermediate_operator in &self.intermediate_operators {
                    results = intermediate_operator.execute(&results).unwrap();
                }
                // Send the value to the final consumer
                let _ = self.tx.send((self.id, self.part_idx, results)).await;
            }
            Morsel::ScanTask(scan_task) => {
                let (send, recv) = tokio::sync::oneshot::channel();
                // TODO: need to put the scan task in a separate thread because it tries to create another tokio runtime,
                // which is not allowed because it is already in a tokio runtime
                // rayon is just a patch, need to think about a better solution
                rayon::spawn(move || {
                    let io_stats = IOStatsContext::new(format!(
                        "MicroPartition::from_scan_task for {:?}",
                        scan_task.sources
                    ));
                    let part =
                        Arc::new(MicroPartition::from_scan_task(scan_task, io_stats).unwrap());
                    let _ = send.send(part);
                });
                let value = recv.await.unwrap();

                // Apply intermediate operators
                let mut results = vec![value];
                for intermediate_operator in &self.intermediate_operators {
                    results = intermediate_operator.execute(&results).unwrap();
                }
                // Send the value to the final consumer
                self.tx
                    .send((self.id, self.part_idx, results))
                    .await
                    .unwrap();
            }
        }
    }
}

struct FinalConsumer {
    rx: mpsc::Receiver<(usize, usize, Vec<Arc<MicroPartition>>)>,
    next_pipeline_tx: Option<mpsc::Sender<Vec<Arc<MicroPartition>>>>,
    sink: Option<Arc<Mutex<dyn Sink>>>,
}

impl FinalConsumer {
    fn new(
        rx: mpsc::Receiver<(usize, usize, Vec<Arc<MicroPartition>>)>,
        next_pipeline_tx: Option<mpsc::Sender<Vec<Arc<MicroPartition>>>>,
        sink: Option<Arc<Mutex<dyn Sink>>>,
    ) -> Self {
        Self {
            rx,
            next_pipeline_tx,
            sink,
        }
    }

    async fn run(mut self) {
        // TODO: in order vs out of order has a lot of dupes, factor out
        let in_order = match self.sink {
            Some(ref sink) => sink.lock().await.in_order(),
            None => false,
        };
        let mut done = false;

        if in_order {
            // Buffer values until they can be processed in order
            let mut buffer = BTreeMap::new();
            let mut next_index = 0;

            while let Some((id, part_idx, value)) = self.rx.recv().await {
                buffer.insert(part_idx, (id, value));

                // Process all consecutive values starting from `next_index`
                while let Some((id, val)) = buffer.remove(&next_index) {
                    next_index += 1;
                    if let Some(sink) = self.sink.as_mut() {
                        // Sink the value
                        let sink_result = sink.lock().await.sink(&val, id);
                        match sink_result {
                            SinkResultType::Finished(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(vec![part]).await.unwrap();
                                    }
                                }
                                done = true;
                                break;
                            }
                            SinkResultType::NeedMoreInput(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(vec![part]).await.unwrap();
                                    }
                                }
                            }
                        }
                    } else {
                        // If there is no sink, just forward the value
                        if let Some(ref tx) = self.next_pipeline_tx {
                            tx.send(val.clone()).await.unwrap();
                        }
                    }
                }
            }

            // Process remaining values
            if !done {
                while let Some((id, val)) = buffer.remove(&next_index) {
                    if let Some(sink) = self.sink.as_mut() {
                        // Sink the value
                        let sink_result = sink.lock().await.sink(&val, id);
                        match sink_result {
                            SinkResultType::Finished(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(vec![part]).await.unwrap();
                                    }
                                }
                                done = true;
                                break;
                            }
                            SinkResultType::NeedMoreInput(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(vec![part]).await.unwrap();
                                    }
                                }
                            }
                        }
                    } else {
                        // If there is no sink, just forward the value
                        if let Some(ref tx) = self.next_pipeline_tx {
                            tx.send(val.clone()).await.unwrap();
                        }
                    }
                    next_index += 1;
                }
            }
        } else {
            while let Some((id, _, val)) = self.rx.recv().await {
                if let Some(sink) = self.sink.as_mut() {
                    // Sink the value
                    let sink_result = sink.lock().await.sink(&val, id);
                    match sink_result {
                        SinkResultType::Finished(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(vec![part]).await.unwrap();
                                }
                            }
                            done = true;
                            break;
                        }
                        SinkResultType::NeedMoreInput(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(vec![part]).await.unwrap();
                                }
                            }
                        }
                    }
                } else {
                    // If there is no sink, just forward the value
                    if let Some(ref tx) = self.next_pipeline_tx {
                        tx.send(val.clone()).await.unwrap();
                    }
                }
            }
        }

        if !done {
            if let Some(sink) = self.sink {
                if let Some(leftover) = sink.lock().await.finalize() {
                    for part in leftover {
                        if let Some(ref tx) = self.next_pipeline_tx {
                            tx.send(vec![part]).await.unwrap();
                        }
                    }
                }
            }
        }
    }
}

pub struct StreamingPipeline {
    id: usize,
    input: Source,
    next_pipeline_tx: Option<mpsc::Sender<Vec<Arc<MicroPartition>>>>,
    intermediate_operators: Vec<Box<dyn IntermediateOperator>>,
    sink: Option<Arc<Mutex<dyn Sink>>>,
}

impl StreamingPipeline {
    pub fn new(
        id: usize,
        input: Source,
        next_pipeline_tx: Option<mpsc::Sender<Vec<Arc<MicroPartition>>>>,
    ) -> Self {
        Self {
            id,
            input,
            next_pipeline_tx,
            intermediate_operators: vec![],
            sink: None,
        }
    }

    pub fn add_operator(&mut self, operator: Box<dyn IntermediateOperator>) {
        self.intermediate_operators.push(operator);
    }

    pub fn set_sink(&mut self, sink: Arc<Mutex<dyn Sink>>) {
        self.sink = Some(sink);
    }

    pub fn set_next_pipeline_tx(&mut self, tx: mpsc::Sender<Vec<Arc<MicroPartition>>>) {
        self.next_pipeline_tx = Some(tx);
    }

    pub async fn run(self) {
        // Create channels
        let channel_size = match self.sink {
            Some(ref sink) => sink.lock().await.queue_size(),
            None => 32,
        };
        let (final_tx, final_rx) = mpsc::channel(channel_size);

        // Spawn the producer
        let producer = Producer {
            id: self.id,
            input: self.input,
            final_tx,
            intermediate_operators: self.intermediate_operators,
        };
        tokio::spawn(producer.run());

        // Spawn the final consumer
        let final_consumer = FinalConsumer::new(final_rx, self.next_pipeline_tx, self.sink);
        let final_handle = tokio::spawn(final_consumer.run());

        // Await the final consumer
        final_handle.await.unwrap();
    }
}
