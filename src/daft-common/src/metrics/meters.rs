use std::{
    borrow::Cow,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
};

use atomic_float::AtomicF64;
use opentelemetry::{InstrumentationScope, KeyValue, global};

use super::{
    ATTR_QUERY_ID, BYTES_IN_KEY, BYTES_OUT_KEY, DURATION_KEY, NUM_TASKS_KEY, QueryID, ROWS_IN_KEY,
    ROWS_OUT_KEY, UNIT_BYTES, UNIT_MICROSECONDS, UNIT_ROWS, UNIT_TASKS,
};

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
    fn new(
        meter: &opentelemetry::metrics::Meter,
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
    fn new(
        meter: &opentelemetry::metrics::Meter,
        name: impl Into<Cow<'static, str>>,
        description: Option<Cow<'static, str>>,
        unit: Option<Cow<'static, str>>,
    ) -> Self {
        let normalized_name = normalize_name(name);
        let builder = meter.f64_gauge(normalized_name);
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

pub struct UpDownCounter {
    value: AtomicI64,
    otel: opentelemetry::metrics::UpDownCounter<i64>,
}

impl UpDownCounter {
    fn new(meter: &opentelemetry::metrics::Meter, name: impl Into<Cow<'static, str>>) -> Self {
        let normalized_name = normalize_name(name);
        let builder = meter.i64_up_down_counter(normalized_name);
        Self {
            value: AtomicI64::new(0),
            otel: builder.build(),
        }
    }

    pub fn add(&self, value: i64, key_values: &[KeyValue]) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.otel.add(value, key_values);
    }

    pub fn load(&self, ordering: Ordering) -> i64 {
        self.value.load(ordering)
    }
}

#[derive(Clone)]
pub struct Meter {
    otel: opentelemetry::metrics::Meter,
}

impl Meter {
    pub fn query_scope(query_id: QueryID, name: impl Into<Cow<'static, str>>) -> Self {
        let scope = InstrumentationScope::builder(name)
            .with_attributes(vec![KeyValue::new(ATTR_QUERY_ID, query_id)])
            .build();

        let otel = global::meter_with_scope(scope);
        Self { otel }
    }

    pub fn global_scope(name: &'static str) -> Self {
        let otel = global::meter(name);
        Self { otel }
    }

    pub fn test_scope(name: &'static str) -> Self {
        Self::global_scope(name)
    }

    pub fn u64_counter(&self, name: impl Into<Cow<'static, str>>) -> Counter {
        Counter::new(&self.otel, name, None, None)
    }

    pub fn u64_counter_with_desc_and_unit(
        &self,
        name: impl Into<Cow<'static, str>>,
        description: Option<Cow<'static, str>>,
        unit: Option<Cow<'static, str>>,
    ) -> Counter {
        Counter::new(&self.otel, name, description, unit)
    }

    pub fn f64_gauge(&self, name: impl Into<Cow<'static, str>>) -> Gauge {
        Gauge::new(&self.otel, name, None, None)
    }

    pub fn f64_gauge_with_desc_and_unit(
        &self,
        name: impl Into<Cow<'static, str>>,
        description: Option<Cow<'static, str>>,
        unit: Option<Cow<'static, str>>,
    ) -> Gauge {
        Gauge::new(&self.otel, name, description, unit)
    }

    pub fn i64_up_down_counter(&self, name: impl Into<Cow<'static, str>>) -> UpDownCounter {
        UpDownCounter::new(&self.otel, name)
    }

    pub fn duration_us_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            DURATION_KEY,
            None,
            Some(Cow::Borrowed(UNIT_MICROSECONDS)),
        )
    }

    pub fn rows_in_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            ROWS_IN_KEY,
            None,
            Some(Cow::Borrowed(UNIT_ROWS)),
        )
    }

    pub fn rows_out_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            ROWS_OUT_KEY,
            None,
            Some(Cow::Borrowed(UNIT_ROWS)),
        )
    }

    pub fn bytes_in_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            BYTES_IN_KEY,
            None,
            Some(Cow::Borrowed(UNIT_BYTES)),
        )
    }

    pub fn bytes_out_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            BYTES_OUT_KEY,
            None,
            Some(Cow::Borrowed(UNIT_BYTES)),
        )
    }

    pub fn num_tasks_metric(&self) -> Counter {
        Counter::new(
            &self.otel,
            NUM_TASKS_KEY,
            None,
            Some(Cow::Borrowed(UNIT_TASKS)),
        )
    }
}
