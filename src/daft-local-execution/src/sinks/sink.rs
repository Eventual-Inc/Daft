use std::{borrow::BorrowMut, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{stream::FuturesUnordered, StreamExt};

use crate::{Receiver, Sender};

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub trait Sink: dyn_clone::DynClone + Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}

dyn_clone::clone_trait_object!(Sink);

pub async fn launch_sink(
    mut receivers: Vec<Receiver>,
    mut sink: Option<Box<dyn Sink>>,
    send_to_next_source: Sender,
) -> DaftResult<()> {
    let in_order = match sink {
        Some(ref sink) => sink.in_order(),
        None => false,
    };
    log::debug!("Launching sink with in_order: {}", in_order);

    if in_order {
        let mut finished_receiver_idxs = std::collections::HashSet::new();
        let mut curr_idx = 0;
        while finished_receiver_idxs.len() != receivers.len() {
            if finished_receiver_idxs.contains(&curr_idx) {
                curr_idx = (curr_idx + 1) % receivers.len();
                continue;
            }
            let receiver = receivers.get_mut(curr_idx).expect("Receiver not found");
            if let Some(val) = receiver.recv().await {
                if let Some(sink) = sink.borrow_mut() {
                    let sink_result = sink.sink(&val?)?;
                    match sink_result {
                        SinkResultType::Finished => {
                            break;
                        }
                        SinkResultType::NeedMoreInput => {}
                    }
                } else {
                    log::debug!("No sink, sending value to next source");
                    let _ = send_to_next_source.send(val).await;
                }
            } else {
                finished_receiver_idxs.insert(curr_idx);
            }
            curr_idx = (curr_idx + 1) % receivers.len();
        }
    } else {
        let mut unordered_receivers = FuturesUnordered::new();
        for receiver in receivers.iter_mut() {
            unordered_receivers.push(receiver.recv());
        }
        while let Some(val) = unordered_receivers.next().await {
            if let Some(val) = val {
                if let Some(sink) = sink.borrow_mut() {
                    let sink_result = sink.sink(&val?)?;
                    match sink_result {
                        SinkResultType::Finished => {
                            break;
                        }
                        SinkResultType::NeedMoreInput => {}
                    }
                } else {
                    log::debug!("No sink, sending value to next source");
                    let _ = send_to_next_source.send(val).await;
                }
            }
        }
    }

    if let Some(sink) = sink.borrow_mut() {
        let final_values = sink.finalize()?;
        for value in final_values {
            log::debug!("Sending finalized value to next source");
            let _ = send_to_next_source.send(Ok(value)).await;
        }
    }
    Ok(())
}
