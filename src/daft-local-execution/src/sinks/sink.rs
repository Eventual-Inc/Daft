use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::{info_span, instrument};

use crate::{
    channel::{create_channel, MultiReceiver, MultiSender},
    NUM_CPUS,
};

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub fn run_sink(sink: Box<dyn Sink>, send_to: MultiSender) -> Vec<MultiSender> {
    let inputs = sink.num_inputs();
    let mut senders = Vec::with_capacity(inputs);
    let mut receivers = Vec::with_capacity(inputs);
    let in_order = sink.in_order();
    for _ in 0..inputs {
        let (sender, receiver) = create_channel(*NUM_CPUS, in_order);
        senders.push(sender);
        receivers.push(receiver);
    }

    let actor = SinkActor::new(sink, receivers, send_to);
    tokio::spawn(async move {
        let _ = actor.run().await;
    });
    senders
}

// pub trait Sink: Send + Sync {
//     fn sink(&mut self, index: usize, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
//     fn num_inputs(&self) -> usize;
//     fn in_order(&self) -> bool;
//     fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>>;
// }

// pub struct SinkActor {
//     sink: Box<dyn Sink>,
//     receivers: Vec<MultiReceiver>,
//     sender: MultiSender,
// }

// impl SinkActor {
//     pub fn new(sink: Box<dyn Sink>, receivers: Vec<MultiReceiver>, sender: MultiSender) -> Self {
//         Self {
//             sink,
//             receivers,
//             sender,
//         }
//     }
//     #[instrument(level = "info", skip(self), name = "SinkActor::run")]
//     pub async fn run(mut self) -> DaftResult<()> {
//         for (i, mut receiver) in self.receivers.into_iter().enumerate() {
//             // maybe this should be concurrent?
//             while let Some(val) = receiver.recv().await {
//                 let _sink_span = info_span!("Sink::sink").entered();
//                 let sink_result = self.sink.sink(i, &val?)?;
//                 match sink_result {
//                     SinkResultType::NeedMoreInput => {
//                         continue;
//                     }
//                     SinkResultType::Finished => {
//                         break;
//                     }
//                 }
//             }
//         }

//         let final_span = info_span!("Sink::finalize");

//         let finalized_values = final_span.in_scope(|| self.sink.finalize())?;
//         for val in finalized_values {
//             let _ = self.sender.get_next_sender().send(Ok(val)).await;
//         }
//         Ok(())
//     }
// }
