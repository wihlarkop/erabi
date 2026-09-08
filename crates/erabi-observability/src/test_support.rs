use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
};

use tracing::{Event, Id, Subscriber, field::Visit, span::Attributes};
use tracing_subscriber::{
    layer::{Context, Layer},
    prelude::*,
    registry::LookupSpan,
};

use crate::TelemetryLevel;

/// A semantic field captured from a span or event for focused assertions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedField {
    pub name: String,
    pub value: String,
}

/// A closed span captured without timestamps, thread IDs, or formatted lines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedSpan {
    pub name: String,
    pub level: TelemetryLevel,
    pub fields: Vec<CapturedField>,
}

/// An event captured without depending on renderer output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedEvent {
    pub name: String,
    pub level: TelemetryLevel,
    pub fields: Vec<CapturedField>,
}

/// A semantic record emitted by an isolated test dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapturedRecord {
    Span(CapturedSpan),
    Event(CapturedEvent),
}

/// Isolated structured capture for tests; it never installs a global subscriber.
#[derive(Clone, Default)]
pub struct Capture {
    records: Arc<Mutex<Vec<CapturedRecord>>>,
}

impl Capture {
    /// Creates an empty isolated capture sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs a future under an isolated subscriber and dispatcher.
    pub async fn run<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let subscriber = tracing_subscriber::registry().with(CaptureLayer {
            records: Arc::clone(&self.records),
        });
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);
        future.await
    }

    /// Returns a stable semantic snapshot of captured records.
    #[must_use]
    pub fn records(&self) -> Vec<CapturedRecord> {
        self.records
            .lock()
            .map_or_else(|_| Vec::new(), |records| records.clone())
    }
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<CapturedRecord>>>,
}

struct CapturedSpanState {
    name: String,
    level: TelemetryLevel,
    fields: Vec<CapturedField>,
}

impl CaptureLayer {
    fn push(&self, record: CapturedRecord) {
        if let Ok(mut records) = self.records.lock() {
            records.push(record);
        }
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let metadata = span.metadata();
        let mut fields = Vec::new();
        attrs.record(&mut FieldRecorder {
            fields: &mut fields,
        });
        span.extensions_mut().insert(CapturedSpanState {
            name: metadata.name().to_owned(),
            level: TelemetryLevel::from_tracing(*metadata.level()),
            fields,
        });
    }

    fn on_record(&self, id: &Id, record: &tracing::span::Record<'_>, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        if let Some(state) = span.extensions_mut().get_mut::<CapturedSpanState>() {
            record.record(&mut FieldRecorder {
                fields: &mut state.fields,
            });
        }
    }

    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        let state = span.extensions_mut().remove::<CapturedSpanState>();
        if let Some(state) = state {
            self.push(CapturedRecord::Span(CapturedSpan {
                name: state.name,
                level: state.level,
                fields: state.fields,
            }));
        }
    }

    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        let mut fields = Vec::new();
        event.record(&mut FieldRecorder {
            fields: &mut fields,
        });
        self.push(CapturedRecord::Event(CapturedEvent {
            name: metadata.name().to_owned(),
            level: TelemetryLevel::from_tracing(*metadata.level()),
            fields,
        }));
    }
}

struct FieldRecorder<'a> {
    fields: &'a mut Vec<CapturedField>,
}

impl FieldRecorder<'_> {
    fn set(&mut self, name: &tracing::field::Field, value: String) {
        if let Some(field) = self
            .fields
            .iter_mut()
            .find(|field| field.name == name.name())
        {
            field.value = value;
        } else {
            self.fields.push(CapturedField {
                name: name.name().to_owned(),
                value,
            });
        }
    }
}

impl Visit for FieldRecorder<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.set(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.set(field, value.to_owned());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.set(field, value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.set(field, value.to_string());
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.set(field, value.to_string());
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.set(field, value.to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.set(field, value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.set(field, value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;

    #[test]
    fn capture_records_event_name_level_and_fields() {
        let capture = Capture::new();
        block_on(capture.run(async {
            tracing::warn!(
                target: crate::fields::ERABI_TELEMETRY_TARGET,
                event_name = "telemetry.test",
                code = "SAFE_CODE"
            );
        }));

        let records = capture.records();
        let Some(event) = records.iter().find_map(|record| match record {
            CapturedRecord::Event(event) => Some(event),
            CapturedRecord::Span(_) => None,
        }) else {
            panic!("event was not captured");
        };
        assert_eq!(event.level, TelemetryLevel::Warn);
        assert!(
            event
                .fields
                .iter()
                .any(|field| field.name == "event_name" && field.value == "telemetry.test")
        );
        assert!(
            event
                .fields
                .iter()
                .any(|field| field.name == "code" && field.value == "SAFE_CODE")
        );
    }

    fn block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                std::task::Poll::Ready(output) => return output,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
