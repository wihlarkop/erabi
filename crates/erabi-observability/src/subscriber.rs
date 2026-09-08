use std::io::IsTerminal;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{filter::filter_fn, layer::SubscriberExt, prelude::*};

use crate::fields::{
    ERABI_TELEMETRY_TARGET, FIELD_CODE, FIELD_EVENT_NAME, TELEMETRY_CONFIGURATION_EVENT_NAME,
};

/// Output renderer selected by the CLI composition root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryFormat {
    Human,
    Json,
}

/// The fixed five-level telemetry filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelemetryLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl TelemetryLevel {
    pub(crate) const fn as_filter(self) -> tracing_subscriber::filter::LevelFilter {
        match self {
            Self::Trace => tracing_subscriber::filter::LevelFilter::TRACE,
            Self::Debug => tracing_subscriber::filter::LevelFilter::DEBUG,
            Self::Info => tracing_subscriber::filter::LevelFilter::INFO,
            Self::Warn => tracing_subscriber::filter::LevelFilter::WARN,
            Self::Error => tracing_subscriber::filter::LevelFilter::ERROR,
        }
    }

    #[cfg(feature = "test-support")]
    pub(crate) const fn from_tracing(level: tracing::Level) -> Self {
        match level {
            tracing::Level::TRACE => Self::Trace,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::INFO => Self::Info,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::ERROR => Self::Error,
        }
    }
}

/// Process-wide subscriber configuration.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TelemetryConfig {
    format: TelemetryFormat,
    level: TelemetryLevel,
}

impl TelemetryConfig {
    /// Creates a static-format, static-level configuration.
    #[must_use]
    pub const fn new(format: TelemetryFormat, level: TelemetryLevel) -> Self {
        Self { format, level }
    }
}

/// Result of attempting to install the process subscriber.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    Installed,
    AlreadyInstalled,
}

/// Bounded startup configuration warning codes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ConfigurationWarning {
    InvalidLogFormat,
    InvalidLogLevel,
}

impl ConfigurationWarning {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidLogFormat => "INVALID_LOG_FORMAT",
            Self::InvalidLogLevel => "INVALID_LOG_LEVEL",
        }
    }
}

/// Installs Erabi's subscriber without overriding an existing host subscriber.
#[must_use]
pub fn install(config: TelemetryConfig) -> InstallOutcome {
    let dispatch = make_dispatch(config);
    match tracing::dispatcher::set_global_default(dispatch) {
        Ok(()) => InstallOutcome::Installed,
        Err(_) => InstallOutcome::AlreadyInstalled,
    }
}

/// Emits one of the fixed, sanitized CLI configuration warnings.
pub fn emit_startup_configuration_warning(warning: ConfigurationWarning) {
    tracing::warn!(
        target: ERABI_TELEMETRY_TARGET,
        event_name = TELEMETRY_CONFIGURATION_EVENT_NAME,
        code = warning.as_str(),
    );
}

fn make_dispatch(config: TelemetryConfig) -> tracing::Dispatch {
    make_dispatch_with_writer(config, std::io::stderr)
}

fn make_dispatch_with_writer<W>(config: TelemetryConfig, writer: W) -> tracing::Dispatch
where
    W: for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + Send + Sync + 'static,
{
    match config.format {
        TelemetryFormat::Human => tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_target(false)
                    .with_ansi(std::io::stderr().is_terminal())
                    .with_span_events(FmtSpan::CLOSE)
                    .with_writer(writer)
                    .with_filter(target_filter(config.level)),
            ),
        ),
        TelemetryFormat::Json => tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(false)
                    .with_ansi(false)
                    .with_span_events(FmtSpan::CLOSE)
                    .with_writer(writer)
                    .with_filter(target_filter(config.level)),
            ),
        ),
    }
}

fn target_filter(
    level: TelemetryLevel,
) -> impl tracing_subscriber::layer::Filter<tracing_subscriber::registry::Registry> {
    let level = level.as_filter();
    filter_fn(move |metadata| {
        metadata.target() == ERABI_TELEMETRY_TARGET && level >= *metadata.level()
    })
}

#[allow(dead_code)]
const _: (&str, &str) = (FIELD_EVENT_NAME, FIELD_CODE);

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        io::Write,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;
    use crate::{HttpMethod, HttpRequestSpan, RequestTraceId, RouteTemplate};

    #[derive(Clone, Default)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl TestWriter {
        fn output(&self) -> String {
            self.0.lock().map_or_else(
                |_| String::new(),
                |bytes| String::from_utf8_lossy(&bytes).into_owned(),
            )
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for TestWriter {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    impl Write for TestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("test writer lock poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn human_subscriber_can_be_constructed() {
        let _dispatch = make_dispatch(TelemetryConfig::new(
            TelemetryFormat::Human,
            TelemetryLevel::Info,
        ));
    }

    #[test]
    fn json_subscriber_can_be_constructed() {
        let _dispatch = make_dispatch(TelemetryConfig::new(
            TelemetryFormat::Json,
            TelemetryLevel::Info,
        ));
    }

    #[test]
    fn all_five_levels_can_be_configured() {
        for level in [
            TelemetryLevel::Trace,
            TelemetryLevel::Debug,
            TelemetryLevel::Info,
            TelemetryLevel::Warn,
            TelemetryLevel::Error,
        ] {
            let _dispatch = make_dispatch(TelemetryConfig::new(TelemetryFormat::Json, level));
        }
    }

    #[test]
    fn repeated_global_installation_is_controlled() {
        let _first = install(TelemetryConfig::new(
            TelemetryFormat::Json,
            TelemetryLevel::Error,
        ));
        assert_eq!(
            install(TelemetryConfig::new(
                TelemetryFormat::Human,
                TelemetryLevel::Info,
            )),
            InstallOutcome::AlreadyInstalled
        );
    }

    #[test]
    fn target_allowlist_is_shared_by_human_and_json_renderers() {
        for format in [TelemetryFormat::Human, TelemetryFormat::Json] {
            let info = render_filtered_output(format, TelemetryLevel::Info);
            assert!(info.contains("SAFE_INFO"));
            assert!(info.contains("SAFE_WARN"));
            assert!(info.contains("SAFE_ERROR"));
            assert!(info.contains("http.request"));
            assert!(info.contains("INVALID_LOG_LEVEL"));
            assert!(!info.contains("SAFE_DEBUG"));
            assert!(!info.contains("SAFE_TRACE"));
            assert!(!info.contains("EXTERNAL_SECRET_SENTINEL_92841"));

            let trace = render_filtered_output(format, TelemetryLevel::Trace);
            assert!(trace.contains("SAFE_TRACE"));
            assert!(trace.contains("http.request"));
            assert!(trace.contains("INVALID_LOG_LEVEL"));
            assert!(!trace.contains("EXTERNAL_SECRET_SENTINEL_92841"));
            assert!(!trace.contains("EXTERNAL_SAFE_TARGET"));
            assert!(!trace.contains("NEAR_PREFIX_SECRET_SENTINEL_61537"));
        }
    }

    fn render_filtered_output(format: TelemetryFormat, level: TelemetryLevel) -> String {
        let writer = TestWriter::default();
        let dispatch =
            make_dispatch_with_writer(TelemetryConfig::new(format, level), writer.clone());
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: ERABI_TELEMETRY_TARGET, event_name = "safe.info", code = "SAFE_INFO");
            tracing::warn!(target: ERABI_TELEMETRY_TARGET, event_name = "safe.warn", code = "SAFE_WARN");
            tracing::error!(target: ERABI_TELEMETRY_TARGET, event_name = "safe.error", code = "SAFE_ERROR");
            tracing::debug!(target: ERABI_TELEMETRY_TARGET, event_name = "safe.debug", code = "SAFE_DEBUG");
            tracing::trace!(target: ERABI_TELEMETRY_TARGET, event_name = "safe.trace", code = "SAFE_TRACE");
            tracing::trace!(target: "hostile.external.test", body = "EXTERNAL_SECRET_SENTINEL_92841");
            tracing::info!(target: "hostile.external.test", body = "EXTERNAL_SAFE_TARGET");
            tracing::trace!(target: "erabi.telemetry.untrusted", body = "NEAR_PREFIX_SECRET_SENTINEL_61537");

            let request_trace_id = RequestTraceId::from_incoming(Some("trace-id-0001"));
            let span = HttpRequestSpan::new(
                &request_trace_id,
                HttpMethod::Get,
                &RouteTemplate::ApiV1Health,
            );
            block_on(span.run(async {}));
            span.record_outcome(200, Duration::from_millis(1));
            emit_startup_configuration_warning(ConfigurationWarning::InvalidLogLevel);
        });
        writer.output()
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
