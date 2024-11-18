use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::sync::Semaphore;

pub(crate) struct DynamicParquetReadingSemaphore {
    semaphore: Arc<Semaphore>,
    timings: Mutex<RunningTimings>,
    max_permits: usize,
    current_permits: AtomicUsize,
}

struct RunningTimings {
    io: Option<RunningAverage>,
    compute: Option<RunningAverage>,
    waiting: Option<RunningAverage>,
}

struct RunningAverage {
    count: u32,
    average: Duration,
}

impl RunningAverage {
    fn new(initial: Duration) -> Self {
        Self {
            count: 1,
            average: initial,
        }
    }

    fn update(&mut self, new_value: Duration) {
        // Exponential moving average with alpha = 0.2
        const ALPHA: f64 = 0.2;
        let new_millis = (self.average.as_millis() as f64)
            .mul_add(1.0 - ALPHA, new_value.as_millis() as f64 * ALPHA);
        self.average = Duration::from_millis(new_millis as u64);
        self.count += 1;
    }
}

impl DynamicParquetReadingSemaphore {
    const COMPUTE_THRESHOLD: f64 = 1.2;
    const WAIT_THRESHOLD: f64 = 0.5;

    pub(crate) fn new(max_permits: usize) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(1)), // Start with 1 permit
            timings: Mutex::new(RunningTimings {
                io: None,
                compute: None,
                waiting: None,
            }),
            max_permits,
            current_permits: AtomicUsize::new(1),
        })
    }

    pub(crate) async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed")
    }

    pub(crate) fn record_io_time(&self, duration: Duration) {
        let mut timings = self.timings.lock().unwrap();
        match &mut timings.io {
            Some(avg) => avg.update(duration),
            None => timings.io = Some(RunningAverage::new(duration)),
        }
    }

    pub(crate) fn record_compute_times(
        &self,
        compute_duration: Duration,
        waiting_duration: Duration,
    ) {
        if self.current_permits.load(Ordering::Relaxed) == self.max_permits {
            // No need to record times if we are already at max permits
            return;
        }

        let mut timings = self.timings.lock().unwrap();

        match &mut timings.compute {
            Some(avg) => avg.update(compute_duration),
            None => timings.compute = Some(RunningAverage::new(compute_duration)),
        }

        match &mut timings.waiting {
            Some(avg) => avg.update(waiting_duration),
            None => timings.waiting = Some(RunningAverage::new(waiting_duration)),
        }

        if let (Some(io_avg), Some(compute_avg), Some(wait_avg)) =
            (&timings.io, &timings.compute, &timings.waiting)
        {
            let compute_ratio =
                compute_avg.average.as_millis() as f64 / io_avg.average.as_millis() as f64;

            let wait_ratio =
                wait_avg.average.as_millis() as f64 / compute_avg.average.as_millis() as f64;

            if compute_ratio > Self::COMPUTE_THRESHOLD && wait_ratio < Self::WAIT_THRESHOLD {
                let current_permits = self.current_permits.load(Ordering::Relaxed);
                let optimal_permits = (compute_ratio.ceil() as usize).min(self.max_permits);
                if current_permits < optimal_permits {
                    self.semaphore.add_permits(1);
                    self.current_permits.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
