use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{
        Aggregate, Coalesce, Filter, HashJoin, InMemoryScan, Limit, Project, TabularScan,
    },
    PhysicalPlan,
};
use daft_scan::ScanTask;
use tokio::sync::{mpsc, Mutex};

use crate::{
    intermediate_op::IntermediateOperatorType,
    sink::{CoalesceSink, HashJoinSink, LimitSink, Sink, SinkResultType},
};

enum Source {
    Data(Vec<Arc<MicroPartition>>),
    ScanTask(Vec<Arc<ScanTask>>),
    Receiver(mpsc::Receiver<Arc<MicroPartition>>),
}

enum Morsel {
    Data(Arc<MicroPartition>),
    ScanTask(Arc<ScanTask>),
}

struct Producer {
    id: usize,
    input: Source,
    final_tx: mpsc::Sender<(usize, usize, Arc<MicroPartition>)>,
    intermediate_operators: Vec<IntermediateOperatorType>,
}

impl Producer {
    async fn run(self) {
        let mut handles = vec![];
        // TODO: Factor out all the dupes
        match self.input {
            Source::Data(data) => {
                for value in data {
                    let final_tx = self.final_tx.clone();
                    let part_idx = handles.len();

                    // Spawn a consumer for each piece of data
                    let handle = tokio::spawn(
                        Consumer::new(
                            self.id,
                            Morsel::Data(value),
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

                    // Spawn a consumer for each piece of data
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

                    // Spawn a consumer for each piece of data
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
    tx: mpsc::Sender<(usize, usize, Arc<MicroPartition>)>,
    part_idx: usize,
    intermediate_operators: Vec<IntermediateOperatorType>,
}

impl Consumer {
    fn new(
        id: usize,
        input: Morsel,
        tx: mpsc::Sender<(usize, usize, Arc<MicroPartition>)>,
        part_idx: usize,
        intermediate_operators: Vec<IntermediateOperatorType>,
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
            Morsel::Data(mut value) => {
                // Apply intermediate operators
                for intermediate_operator in &self.intermediate_operators {
                    value = intermediate_operator.execute(&value).unwrap();
                }
                // Send the value to the final consumer
                let _ = self.tx.send((self.id, self.part_idx, value)).await;
            }
            Morsel::ScanTask(scan_task) => {
                let (send, recv) = tokio::sync::oneshot::channel();
                rayon::spawn(move || {
                    let io_stats = IOStatsContext::new(format!(
                        "MicroPartition::from_scan_task for {:?}",
                        scan_task.sources
                    ));
                    let part =
                        Arc::new(MicroPartition::from_scan_task(scan_task, io_stats).unwrap());
                    let _ = send.send(part);
                });
                let mut value = recv.await.unwrap();
                // Apply intermediate operators
                for intermediate_operator in &self.intermediate_operators {
                    value = intermediate_operator.execute(&value).unwrap();
                }
                // Send the value to the final consumer
                self.tx.send((self.id, self.part_idx, value)).await.unwrap();
            }
        }
    }
}

struct FinalConsumer {
    rx: mpsc::Receiver<(usize, usize, Arc<MicroPartition>)>,
    next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    sink: Option<Arc<Mutex<dyn Sink>>>,
}

impl FinalConsumer {
    fn new(
        rx: mpsc::Receiver<(usize, usize, Arc<MicroPartition>)>,
        next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
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
        if in_order {
            // Buffer values until they can be processed in order
            let mut buffer = BTreeMap::new();
            let mut next_index = 0;

            while let Some((id, part_idx, value)) = self.rx.recv().await {
                buffer.insert(part_idx, (id, value));

                // Process all consecutive values starting from `next_index`
                while let Some((id, val)) = buffer.get(&next_index).as_ref() {
                    if let Some(sink) = self.sink.as_mut() {
                        // Sink the value
                        let sink_result = sink.lock().await.sink(val, *id);
                        match sink_result {
                            SinkResultType::Finished(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(part).await.unwrap();
                                    }
                                }
                                break;
                            }
                            SinkResultType::NeedMoreInput(part) => {
                                if let Some(part) = part {
                                    if let Some(ref tx) = self.next_pipeline_tx {
                                        tx.send(part).await.unwrap();
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
                    buffer.remove(&next_index);
                    next_index += 1;
                }
            }

            // Process remaining values
            while let Some((id, val)) = buffer.remove(&next_index) {
                if let Some(sink) = self.sink.as_mut() {
                    // Sink the value
                    let sink_result = sink.lock().await.sink(&val, id);
                    match sink_result {
                        SinkResultType::Finished(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(part).await.unwrap();
                                }
                            }
                        }
                        SinkResultType::NeedMoreInput(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(part).await.unwrap();
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
        } else {
            while let Some((id, _, val)) = self.rx.recv().await {
                if let Some(sink) = self.sink.as_mut() {
                    // Sink the value
                    let sink_result = sink.lock().await.sink(&val, id);
                    match sink_result {
                        SinkResultType::Finished(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(part).await.unwrap();
                                }
                            }
                        }
                        SinkResultType::NeedMoreInput(part) => {
                            if let Some(part) = part {
                                if let Some(ref tx) = self.next_pipeline_tx {
                                    tx.send(part).await.unwrap();
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

        // TODO: Probs need to flush the sink here
        if let Some(sink) = self.sink {
            if let Some(leftover) = sink.lock().await.finalize() {
                for part in leftover {
                    if let Some(ref tx) = self.next_pipeline_tx {
                        tx.send(part).await.unwrap();
                    }
                }
            }
        }
    }
}

pub struct StreamingPipeline {
    id: usize,
    input: Source,
    next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    intermediate_operators: Vec<IntermediateOperatorType>,
    sink: Option<Arc<Mutex<dyn Sink>>>,
}

impl StreamingPipeline {
    fn new(
        id: usize,
        input: Source,
        next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    ) -> Self {
        Self {
            id,
            input,
            next_pipeline_tx,
            intermediate_operators: vec![],
            sink: None,
        }
    }

    fn add_operator(&mut self, operator: IntermediateOperatorType) {
        self.intermediate_operators.push(operator);
    }

    fn set_sink(&mut self, sink: Arc<Mutex<dyn Sink>>) {
        self.sink = Some(sink);
    }

    pub fn set_next_pipeline_tx(&mut self, tx: mpsc::Sender<Arc<MicroPartition>>) {
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

pub fn physical_plan_to_streaming_pipeline(
    physical_plan: &Arc<PhysicalPlan>,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> Vec<StreamingPipeline> {
    let mut pipelines = vec![];
    fn recursively_create_pipelines<'a>(
        physical_plan: &Arc<PhysicalPlan>,
        psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
        pipelines: &'a mut Vec<StreamingPipeline>,
    ) -> &'a mut StreamingPipeline {
        match physical_plan.as_ref() {
            PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                let partitions = psets.get(&in_memory_info.cache_key).unwrap();
                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Data(partitions.clone()), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                let new_pipeline = StreamingPipeline::new(
                    pipelines.len(),
                    Source::ScanTask(scan_tasks.clone()),
                    None,
                );
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let agg_op = IntermediateOperatorType::Aggregate {
                    aggregations: aggregations.clone(),
                    groupby: groupby.clone(),
                };
                current_pipeline.add_operator(agg_op);
                current_pipeline
            }
            PhysicalPlan::Project(Project {
                input, projection, ..
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let proj_op = IntermediateOperatorType::Project {
                    projection: projection.clone(),
                };
                current_pipeline.add_operator(proj_op);
                current_pipeline
            }
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let filter_op = IntermediateOperatorType::Filter {
                    predicate: predicate.clone(),
                };
                current_pipeline.add_operator(filter_op);
                current_pipeline
            }
            PhysicalPlan::Limit(Limit { limit, input, .. }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let sink = LimitSink::new(*limit as usize);
                current_pipeline.set_sink(Arc::new(Mutex::new(sink)));

                let (tx, rx) = tokio::sync::mpsc::channel::<Arc<MicroPartition>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);

                let q = num_from / num_to;
                let r = num_from % num_to;

                let mut distribution = vec![q; *num_to];
                for bucket in distribution.iter_mut().take(r) {
                    *bucket += 1;
                }
                let sink = CoalesceSink::new(distribution);

                current_pipeline.set_sink(Arc::new(Mutex::new(sink)));

                let (tx, rx) = tokio::sync::mpsc::channel::<Arc<MicroPartition>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::HashJoin(HashJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
            }) => {
                {
                    recursively_create_pipelines(left, psets, pipelines);
                };
                let left_idx = pipelines.len() - 1;
                {
                    recursively_create_pipelines(right, psets, pipelines);
                };
                let right_idx = pipelines.len() - 1;

                let hash_join_sink = HashJoinSink::new(
                    left_on.clone(),
                    right_on.clone(),
                    *join_type,
                    left_idx,
                    right_idx,
                );
                let ptr = Arc::new(Mutex::new(hash_join_sink));
                let (tx, rx) = tokio::sync::mpsc::channel::<Arc<MicroPartition>>(32);

                pipelines[left_idx].set_sink(ptr.clone());
                pipelines[right_idx].set_sink(ptr);
                pipelines[left_idx].set_next_pipeline_tx(tx.clone());
                pipelines[right_idx].set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            _ => {
                todo!()
            }
        }
    }

    recursively_create_pipelines(physical_plan, psets, &mut pipelines);
    pipelines
}
