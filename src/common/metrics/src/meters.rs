use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
};

use atomic_float::AtomicF64;
use opentelemetry::{KeyValue, metrics::Meter};

pub fn normalize_name(name: impl Into<Cow<'static, str>>) -> String {
    let name = name.into();
    if name.starts_with("daft.") {
        name.to_string()
    } else {
        format!("daft.{}", name.replace(' ', "_").to_lowercase())
    }
}

pub struct Counter {
    value: AtomicU64,
    otel: opentelemetry::metrics::Counter<u64>,
}

impl Counter {
    pub fn new(
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: Option<Cow<'static, str>>,
        unit: Option<Cow<'static, str>>,
    ) -> Self {
        let normalized_name = normalize_name(name);
        let builder = meter.u64_counter(normalized_name);
        let builder = if let Some(description) = description {
            builder.with_description(description)
        } else {
            builder
        };
        let builder = if let Some(unit) = unit {
            builder.with_unit(unit)
        } else {
            builder
        };
        Self {
            value: AtomicU64::new(0),
            otel: builder.build(),
        }
    }

    pub fn add(&self, value: u64, key_values: &[KeyValue]) -> u64 {
        let prev = self.value.fetch_add(value, Ordering::Relaxed);
        self.otel.add(value, key_values);
        prev + value
    }

    pub fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }
}

pub struct Gauge {
    value: AtomicF64,
    otel: opentelemetry::metrics::Gauge<f64>,
}

impl Gauge {
    pub fn new(
        meter: &Meter,
        name: impl Into<Cow<'static, str>>,
        description: Option<Cow<'static, str>>,
    ) -> Self {
        let normalized_name = normalize_name(name);
        let builder = meter.f64_gauge(normalized_name);
        let builder = if let Some(description) = description {
            builder.with_description(description)
        } else {
            builder
        };
        Self {
            value: AtomicF64::new(f64::NAN),
            otel: builder.build(),
        }
    }

    pub fn update(&self, value: f64, key_values: &[KeyValue]) {
        self.value.store(value, Ordering::Relaxed);
        self.otel.record(value, key_values);
    }

    pub fn load(&self, ordering: Ordering) -> f64 {
        self.value.load(ordering)
    }
}
