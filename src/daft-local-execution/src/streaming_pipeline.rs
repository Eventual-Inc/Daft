use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Aggregate, Coalesce, Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};
use daft_scan::ScanTask;
use tokio::sync::mpsc;

use crate::{
    intermediate_op::IntermediateOperatorType,
    sink::{SinkResultType, StreamingSink},
};

enum Source {
    Data(Vec<Arc<MicroPartition>>),
    ScanTask(Vec<Arc<ScanTask>>),
    Receiver(mpsc::Receiver<Arc<MicroPartition>>),
}

struct Producer {
    input: Source,
    final_tx: mpsc::Sender<(usize, Arc<MicroPartition>)>,
    intermediate_operators: Vec<IntermediateOperatorType>,
}

impl Producer {
    async fn run(self) {
        let mut handles = vec![];
        // TODO: Factor out all the dupes
        match self.input {
            Source::Data(data) => {
                for value in data {
                    let (intermediate_tx, intermediate_rx) = mpsc::channel(32);
                    let final_tx = self.final_tx.clone();
                    let id = handles.len();

                    // Spawn a consumer for each piece of data
                    let handle = tokio::spawn(
                        Consumer::new(
                            intermediate_rx,
                            final_tx,
                            id,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);

                    // Send data to the intermediate consumer
                    intermediate_tx.send(value).await.unwrap();
                }
            }
            Source::Receiver(mut rx) => {
                while let Some(value) = rx.recv().await {
                    let (intermediate_tx, intermediate_rx) = mpsc::channel(32);
                    let final_tx = self.final_tx.clone();
                    let id = handles.len();

                    // Spawn a consumer for each piece of data
                    let handle = tokio::spawn(
                        Consumer::new(
                            intermediate_rx,
                            final_tx,
                            id,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);

                    // Send data to the intermediate consumer
                    intermediate_tx.send(value).await.unwrap();
                }
            }
            Source::ScanTask(scan_tasks) => {
                for scan_task in scan_tasks {
                    let (intermediate_tx, intermediate_rx) = mpsc::channel(32);
                    let final_tx = self.final_tx.clone();
                    let id = handles.len();

                    // Spawn a consumer for each piece of data
                    let handle = tokio::spawn(
                        Consumer::new(
                            intermediate_rx,
                            final_tx,
                            id,
                            self.intermediate_operators.clone(),
                        )
                        .run(),
                    );
                    handles.push(handle);

                    let io_stats = IOStatsContext::new(format!(
                        "MicroPartition::from_scan_task for {:?}",
                        scan_task.sources
                    ));
                    let part =
                        Arc::new(MicroPartition::from_scan_task(scan_task, io_stats).unwrap());
                    // TODO: handle error handling dont unwrap

                    // Send data to the intermediate consumer
                    intermediate_tx.send(part).await.unwrap();
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
    rx: mpsc::Receiver<Arc<MicroPartition>>,
    tx: mpsc::Sender<(usize, Arc<MicroPartition>)>,
    id: usize,
    intermediate_operators: Vec<IntermediateOperatorType>,
}

impl Consumer {
    fn new(
        rx: mpsc::Receiver<Arc<MicroPartition>>,
        tx: mpsc::Sender<(usize, Arc<MicroPartition>)>,
        id: usize,
        intermediate_operators: Vec<IntermediateOperatorType>,
    ) -> Self {
        Self {
            rx,
            tx,
            id,
            intermediate_operators,
        }
    }

    async fn run(mut self) {
        while let Some(mut value) = self.rx.recv().await {
            // Apply intermediate operators
            for intermediate_operator in &self.intermediate_operators {
                value = intermediate_operator.execute(&value).unwrap();
            }
            // Send the value to the final consumer
            self.tx.send((self.id, value)).await.unwrap();
        }
    }
}

struct FinalConsumer {
    rx: mpsc::Receiver<(usize, Arc<MicroPartition>)>,
    in_order: bool,
    next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    sink: Option<StreamingSink>,
}

impl FinalConsumer {
    fn new(
        rx: mpsc::Receiver<(usize, Arc<MicroPartition>)>,
        in_order: bool, // TODO: the sink should decide this
        next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
        sink: Option<StreamingSink>,
    ) -> Self {
        Self {
            rx,
            in_order,
            next_pipeline_tx,
            sink,
        }
    }

    async fn run(mut self) {
        // TODO: in order vs out of order has a lot of dupes, factor out

        if self.in_order {
            // Buffer values until they can be processed in order
            let mut buffer = BTreeMap::new();
            let mut next_index = 0;

            while let Some((index, value)) = self.rx.recv().await {
                buffer.insert(index, value);

                // Process all consecutive values starting from `next_index`
                while let Some(&val) = buffer.get(&next_index).as_ref() {
                    if let Some(sink) = self.sink.as_mut() {
                        // Sink the value
                        let sink_result = sink.sink(val);
                        // Stream out the value
                        if let Some(part) = sink.stream_out_one() {
                            if let Some(ref tx) = self.next_pipeline_tx {
                                tx.send(part).await.unwrap();
                            }
                        }
                        match sink_result {
                            SinkResultType::Finished => break,
                            SinkResultType::NeedMoreInput => {}
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
        } else {
            while let Some((_, val)) = self.rx.recv().await {
                if let Some(sink) = self.sink.as_mut() {
                    // Sink the value
                    let sink_result = sink.sink(&val);
                    // Stream out the value
                    if let Some(part) = sink.stream_out_one() {
                        if let Some(ref tx) = self.next_pipeline_tx {
                            tx.send(part).await.unwrap();
                        }
                    }
                    match sink_result {
                        SinkResultType::Finished => break,
                        SinkResultType::NeedMoreInput => {}
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
    }
}

pub struct StreamingPipeline {
    input: Source,
    in_order: bool,
    next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    intermediate_operators: Vec<IntermediateOperatorType>,
    sink: Option<StreamingSink>,
}

impl StreamingPipeline {
    fn new(
        input: Source,
        in_order: bool,
        next_pipeline_tx: Option<mpsc::Sender<Arc<MicroPartition>>>,
    ) -> Self {
        Self {
            input,
            in_order,
            next_pipeline_tx,
            intermediate_operators: vec![],
            sink: None,
        }
    }

    fn add_operator(&mut self, operator: IntermediateOperatorType) {
        self.intermediate_operators.push(operator);
    }

    fn set_sink(&mut self, sink: StreamingSink) {
        self.sink = Some(sink);
    }

    pub fn set_next_pipeline_tx(&mut self, tx: mpsc::Sender<Arc<MicroPartition>>) {
        self.next_pipeline_tx = Some(tx);
    }

    pub async fn run(self) {
        // Create channels
        let (final_tx, final_rx) = mpsc::channel(32);

        // Spawn the producer
        let producer = Producer {
            input: self.input,
            final_tx,
            intermediate_operators: self.intermediate_operators,
        };
        tokio::spawn(producer.run());

        // Spawn the final consumer
        let final_consumer =
            FinalConsumer::new(final_rx, self.in_order, self.next_pipeline_tx, self.sink);
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
                    StreamingPipeline::new(Source::Data(partitions.clone()), true, None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                let new_pipeline =
                    StreamingPipeline::new(Source::ScanTask(scan_tasks.clone()), true, None);
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
                let sink = StreamingSink::Limit {
                    limit: *limit as usize,
                    partitions: VecDeque::new(),
                    num_rows_taken: 0,
                };
                current_pipeline.set_sink(sink);
                current_pipeline
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
                let sink = StreamingSink::Coalesce {
                    partitions: VecDeque::new(),
                    chunk_sizes: distribution,
                    current_chunk: 0,
                    holding_area: vec![],
                };

                current_pipeline.set_sink(sink);

                let (tx, rx) = tokio::sync::mpsc::channel::<Arc<MicroPartition>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                // TODO: Instead of creating new pipelines after sinks, we should create them when adding intermediate operators
                // This way we can avoid cases where there's an empty pipeline

                let new_pipeline = StreamingPipeline::new(Source::Receiver(rx), true, None);
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
