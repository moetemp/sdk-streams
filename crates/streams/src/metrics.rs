//! The crate's metrics, on the meter lang gives, or on a no-op meter.

use std::time::Duration;
use temporalio_common::telemetry::metrics::{
    Counter, HistogramDuration, MetricAttributes, MetricParameters, TemporalMeter, UpDownCounter,
};

/// Every metric of the Stream runtime.
#[derive(Clone)]
pub(crate) struct Metrics {
    attributes: MetricAttributes,
    hook_latency: HistogramDuration,
    flush_failures: Counter,
    fenced: Counter,
    superseded: Counter,
    buffer_full: Counter,
    close_retries: Counter,
    records_dropped: Counter,
    blocking_reads_waiting: UpDownCounter,
}

fn parameters(
    name: &'static str,
    description: &'static str,
    unit: &'static str,
) -> MetricParameters {
    MetricParameters {
        name: name.into(),
        description: description.into(),
        unit: unit.into(),
    }
}

impl Metrics {
    pub(crate) fn new(meter: &TemporalMeter) -> Self {
        Self {
            attributes: meter.get_default_attributes().clone(),
            hook_latency: meter.histogram_duration(parameters(
                "stream_hook_latency",
                "The time of before_complete for one run",
                "duration",
            )),
            flush_failures: meter.counter(parameters(
                "stream_flush_failures",
                "Workflow flushes that failed the Workflow Task",
                "",
            )),
            fenced: meter.counter(parameters(
                "stream_fenced",
                "Batches that the Store refused for a lower epoch or a withdrawn run",
                "",
            )),
            superseded: meter.counter(parameters(
                "stream_superseded",
                "Superseded flags that the Store set",
                "",
            )),
            buffer_full: meter.counter(parameters(
                "stream_buffer_full",
                "Publishes refused because a writer's buffer was full",
                "",
            )),
            close_retries: meter.counter(parameters(
                "stream_close_retries",
                "Close retries after a Workflow Task completion",
                "",
            )),
            records_dropped: meter.counter(parameters(
                "stream_records_dropped",
                "Records dropped for a closed or expired Stream, or for their size",
                "",
            )),
            blocking_reads_waiting: meter.up_down_counter(parameters(
                "stream_blocking_reads_waiting",
                "Reads that wait on the Store",
                "",
            )),
        }
    }

    pub(crate) fn hook_latency(&self, latency: Duration) {
        self.hook_latency.record(latency, &self.attributes);
    }

    pub(crate) fn flush_failed(&self) {
        self.flush_failures.add(1, &self.attributes);
    }

    pub(crate) fn fenced(&self) {
        self.fenced.add(1, &self.attributes);
    }

    pub(crate) fn superseded(&self) {
        self.superseded.add(1, &self.attributes);
    }

    pub(crate) fn buffer_full(&self) {
        self.buffer_full.add(1, &self.attributes);
    }

    pub(crate) fn close_retried(&self) {
        self.close_retries.add(1, &self.attributes);
    }

    pub(crate) fn records_dropped(&self, count: usize) {
        self.records_dropped.add(count as u64, &self.attributes);
    }

    pub(crate) fn read_waits(&self, waiting: bool) {
        self.blocking_reads_waiting
            .add(if waiting { 1 } else { -1 }, &self.attributes);
    }
}
