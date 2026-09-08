use std::{future::Future, time::Duration};

use tracing::{Instrument, Span, field};

use crate::{
    HttpMethod, RequestTraceId, RouteTemplate,
    fields::{
        FIELD_DURATION_MS, FIELD_HTTP_METHOD, FIELD_ROUTE_TEMPLATE, FIELD_STATUS_CODE,
        FIELD_TRACE_ID, HTTP_REQUEST_SPAN_NAME,
    },
};

/// Narrow async-safe instrumentation for an HTTP request.
pub struct HttpRequestSpan {
    span: Span,
}

impl HttpRequestSpan {
    /// Creates the fixed `http.request` span and its reserved outcome fields.
    #[must_use]
    pub fn new(
        request_trace_id: &RequestTraceId,
        method: HttpMethod,
        route_template: &RouteTemplate,
    ) -> Self {
        let trace_id = request_trace_id.as_str();
        let http_method = method.as_str();
        let route_template = route_template.as_str();
        Self {
            span: tracing::info_span!(
                target: crate::fields::ERABI_TELEMETRY_TARGET,
                HTTP_REQUEST_SPAN_NAME,
                trace_id = %trace_id,
                http_method = %http_method,
                route_template = %route_template,
                status_code = field::Empty,
                duration_ms = field::Empty,
            ),
        }
    }

    /// Runs a future under this span without exposing `tracing` to callers.
    pub async fn run<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        future.instrument(self.span.clone()).await
    }

    /// Records only the fixed numeric HTTP outcome fields.
    pub fn record_outcome(&self, status_code: u16, duration: Duration) {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.span.record(FIELD_STATUS_CODE, status_code);
        self.span.record(FIELD_DURATION_MS, duration_ms);
    }
}

#[allow(dead_code)]
const _: (&str, &str, &str, &str, &str, &str) = (
    FIELD_TRACE_ID,
    FIELD_HTTP_METHOD,
    FIELD_ROUTE_TEMPLATE,
    FIELD_STATUS_CODE,
    FIELD_DURATION_MS,
    HTTP_REQUEST_SPAN_NAME,
);

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use std::future::Future;

    use super::*;

    #[test]
    fn request_span_records_numeric_outcome_fields() {
        let capture = crate::test_support::Capture::new();
        block_on(capture.run(async {
            let request_trace_id = RequestTraceId::from_incoming(Some("trace-id-0001"));
            let span = HttpRequestSpan::new(
                &request_trace_id,
                HttpMethod::Get,
                &RouteTemplate::ApiV1Health,
            );
            span.run(async {}).await;
            span.record_outcome(204, Duration::from_millis(12));
        }));
        let records = capture.records();
        let request_span = records.iter().find_map(|record| match record {
            crate::test_support::CapturedRecord::Span(span)
                if span.name == HTTP_REQUEST_SPAN_NAME =>
            {
                Some(span)
            }
            _ => None,
        });
        let Some(request_span) = request_span else {
            panic!("request span was not captured");
        };
        assert!(
            request_span
                .fields
                .iter()
                .any(|field| field.name == FIELD_STATUS_CODE && field.value == "204")
        );
        assert!(
            request_span
                .fields
                .iter()
                .any(|field| field.name == FIELD_DURATION_MS && field.value == "12")
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
