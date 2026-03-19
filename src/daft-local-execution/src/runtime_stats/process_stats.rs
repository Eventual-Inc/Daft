use std::sync::Arc;

use common_metrics::{
    Gauge, PROCESS_CPU_PERCENT_KEY, PROCESS_JEMALLOC_ALLOCATED_KEY, PROCESS_JEMALLOC_RESIDENT_KEY,
    PROCESS_RSS_KEY, Stat, Stats,
};
use opentelemetry::{KeyValue, metrics::Meter};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

pub struct ProcessStatsCollector {
    // OTEL gauges
    #[cfg(not(target_env = "msvc"))]
    jemalloc_allocated: Gauge,
    #[cfg(not(target_env = "msvc"))]
    jemalloc_resident: Gauge,
    rss: Gauge,
    cpu_percent: Gauge,
    // sysinfo state
    system: System,
    pid: sysinfo::Pid,
    // OTEL labels
    labels: Vec<KeyValue>,
}

impl ProcessStatsCollector {
    pub fn new(meter: &Meter) -> Option<Self> {
        let pid = match get_current_pid() {
            Ok(pid) => pid,
            Err(e) => {
                log::warn!("Failed to get current PID for process monitor: {e}");
                return None;
            }
        };

        let role = if std::env::var("DAFT_FLOTILLA_WORKER").is_ok() {
            "worker"
        } else {
            "driver"
        };
        let labels = vec![KeyValue::new("daft.process.role", role)];

        Some(Self {
            #[cfg(not(target_env = "msvc"))]
            jemalloc_allocated: Gauge::new(
                meter,
                PROCESS_JEMALLOC_ALLOCATED_KEY,
                Some("Bytes currently allocated by the application via jemalloc".into()),
            ),
            #[cfg(not(target_env = "msvc"))]
            jemalloc_resident: Gauge::new(
                meter,
                PROCESS_JEMALLOC_RESIDENT_KEY,
                Some("Resident bytes from jemalloc's perspective".into()),
            ),
            rss: Gauge::new(
                meter,
                PROCESS_RSS_KEY,
                Some("OS-level resident set size of the process".into()),
            ),
            cpu_percent: Gauge::new(
                meter,
                PROCESS_CPU_PERCENT_KEY,
                Some("Process CPU utilization percentage".into()),
            ),
            system: System::new(),
            pid,
            labels,
        })
    }

    /// Called on each tick from the RuntimeStatsManager event loop.
    /// Returns a Stats snapshot for inclusion in the subscriber pipeline.
    pub fn sample(&mut self) -> Stats {
        // Collect sysinfo stats (RSS, CPU)
        let (rss_bytes, cpu_pct) = self.sample_sysinfo();

        // Update OTEL gauges
        self.rss.update(rss_bytes as f64, &self.labels);
        self.cpu_percent.update(cpu_pct as f64, &self.labels);

        // Build stats entries
        let rss_key: Arc<str> = PROCESS_RSS_KEY.into();
        let cpu_key: Arc<str> = PROCESS_CPU_PERCENT_KEY.into();

        // Collect jemalloc stats and update OTEL gauges
        #[cfg(not(target_env = "msvc"))]
        let (jemalloc_allocated_bytes, jemalloc_resident_bytes) = {
            let (a, r) = self.sample_jemalloc();
            self.jemalloc_allocated.update(a as f64, &self.labels);
            self.jemalloc_resident.update(r as f64, &self.labels);
            (a, r)
        };

        #[cfg(not(target_env = "msvc"))]
        let entries = smallvec::smallvec![
            (rss_key, Stat::Bytes(rss_bytes)),
            (cpu_key, Stat::Percent(cpu_pct as f64)),
            (
                Arc::from(PROCESS_JEMALLOC_ALLOCATED_KEY),
                Stat::Bytes(jemalloc_allocated_bytes)
            ),
            (
                Arc::from(PROCESS_JEMALLOC_RESIDENT_KEY),
                Stat::Bytes(jemalloc_resident_bytes)
            ),
        ];

        #[cfg(target_env = "msvc")]
        let entries = smallvec::smallvec![
            (rss_key, Stat::Bytes(rss_bytes)),
            (cpu_key, Stat::Percent(cpu_pct as f64)),
        ];

        Stats(entries)
    }

    #[cfg(not(target_env = "msvc"))]
    fn sample_jemalloc(&self) -> (u64, u64) {
        use tikv_jemalloc_ctl::{epoch, stats};

        // Advance the epoch to refresh jemalloc's internal stats cache.
        if let Err(e) = epoch::advance() {
            log::warn!("Failed to advance jemalloc epoch: {e}");
            return (0, 0);
        }

        let allocated = stats::allocated::read().unwrap_or(0) as u64;
        let resident = stats::resident::read().unwrap_or(0) as u64;
        (allocated, resident)
    }

    fn sample_sysinfo(&mut self) -> (u64, f32) {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            false,
            ProcessRefreshKind::nothing().with_memory().with_cpu(),
        );

        match self.system.process(self.pid) {
            Some(process) => (process.memory(), process.cpu_usage()),
            None => {
                log::warn!("Failed to find current process in sysinfo");
                (0, 0.0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::global;

    use super::*;

    #[test]
    fn test_process_stats_collector_creation() {
        let meter = global::meter("test_process_stats");
        let collector = ProcessStatsCollector::new(&meter);
        assert!(
            collector.is_some(),
            "ProcessStatsCollector should be created successfully"
        );
    }

    #[test]
    fn test_process_stats_sample_returns_stats() {
        let meter = global::meter("test_process_stats_sample");
        let mut collector = ProcessStatsCollector::new(&meter).unwrap();

        let stats = collector.sample();
        // Should have at least RSS and CPU entries
        assert!(stats.0.len() >= 2);

        // RSS should be the first entry and non-zero
        let (name, value) = &stats.0[0];
        assert_eq!(name.as_ref(), PROCESS_RSS_KEY);
        match value {
            Stat::Bytes(b) => assert!(*b > 0, "RSS should be non-zero"),
            _ => panic!("Expected Stat::Bytes for RSS"),
        }
    }

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn test_jemalloc_stats_nonzero() {
        let meter = global::meter("test_jemalloc_stats");
        let collector = ProcessStatsCollector::new(&meter).unwrap();

        let (allocated, resident) = collector.sample_jemalloc();
        assert!(allocated > 0, "jemalloc allocated should be non-zero");
        assert!(resident > 0, "jemalloc resident should be non-zero");
        assert!(resident >= allocated, "resident should be >= allocated");
    }
}
