use std::{collections::VecDeque, time::Duration};

pub trait DynamicBatching: Send + Sync {
    type State: Send + Sync;
    fn adjust_batch_size(&self, state: &mut Self::State, duration: Duration) -> usize;
    fn current_batch_size(&self, state: &Self::State) -> usize;
}

pub struct AimdDynamicBatching {
    additive_increase: usize,
    multiplicative_decrease: f64,
    latency_threshold: Duration,
    min_batch_size: usize,
    max_batch_size: usize,
}

#[allow(dead_code)]
impl AimdDynamicBatching {
    pub fn new(
        additive_increase: usize,
        multiplicative_decrease: f64,
        latency_threshold: Duration,
        min_batch_size: usize,
        max_batch_size: usize,
    ) -> Self {
        Self {
            additive_increase,
            multiplicative_decrease,
            latency_threshold,
            min_batch_size,
            max_batch_size,
        }
    }
}
impl Default for AimdDynamicBatching {
    fn default() -> Self {
        Self {
            additive_increase: 10,
            multiplicative_decrease: 0.5,
            latency_threshold: Duration::from_secs(5),
            min_batch_size: 1,
            max_batch_size: 128 * 1024,
        }
    }
}

pub struct AimdState {
    history: VecDeque<(usize, Duration)>,
    current_batch_size: usize,
}

impl AimdState {
    pub fn new(initial_batch_size: usize) -> Self {
        Self {
            history: VecDeque::new(),
            current_batch_size: initial_batch_size,
        }
    }
}

impl DynamicBatching for AimdDynamicBatching {
    type State = AimdState;

    fn adjust_batch_size(&self, state: &mut Self::State, duration: Duration) -> usize {
        let num_rows = self.current_batch_size(state);
        // Record this execution
        state.history.push_back((num_rows, duration));
        if state.history.len() > 10 {
            state.history.pop_front();
        }

        let congestion_detected = duration > self.latency_threshold;

        state.current_batch_size = if congestion_detected {
            // Multiplicative decrease
            ((state.current_batch_size as f64 * self.multiplicative_decrease) as usize)
                .max(self.min_batch_size)
        } else {
            // Additive increase
            (state.current_batch_size + self.additive_increase).min(self.max_batch_size)
        };

        state.current_batch_size
    }

    fn current_batch_size(&self, state: &Self::State) -> usize {
        state.current_batch_size
    }
}
