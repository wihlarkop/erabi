use std::io::IsTerminal;

use erabi_observability::{
    ConfigurationWarning, InstallOutcome, TelemetryConfig, TelemetryFormat, TelemetryLevel,
    emit_startup_configuration_warning, install,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogFormatSetting {
    Auto,
    Human,
    Json,
}

impl LogFormatSetting {
    const fn resolve(self, stderr_is_terminal: bool) -> TelemetryFormat {
        match self {
            Self::Auto if stderr_is_terminal => TelemetryFormat::Human,
            Self::Auto | Self::Json => TelemetryFormat::Json,
            Self::Human => TelemetryFormat::Human,
        }
    }
}

struct ParsedTelemetrySettings {
    format: LogFormatSetting,
    level: TelemetryLevel,
    invalid_format: bool,
    invalid_level: bool,
}

impl ParsedTelemetrySettings {
    fn from_values(format: Option<&str>, level: Option<&str>) -> Self {
        let (format, invalid_format) = match format {
            None | Some("auto") => (LogFormatSetting::Auto, false),
            Some("human") => (LogFormatSetting::Human, false),
            Some("json") => (LogFormatSetting::Json, false),
            Some(_) => (LogFormatSetting::Auto, true),
        };
        let (level, invalid_level) = match level {
            None | Some("info") => (TelemetryLevel::Info, false),
            Some("trace") => (TelemetryLevel::Trace, false),
            Some("debug") => (TelemetryLevel::Debug, false),
            Some("warn") => (TelemetryLevel::Warn, false),
            Some("error") => (TelemetryLevel::Error, false),
            Some(_) => (TelemetryLevel::Info, true),
        };
        Self {
            format,
            level,
            invalid_format,
            invalid_level,
        }
    }
}

/// Parses process telemetry settings and installs the process subscriber.
///
/// Invalid values intentionally fall back to safe defaults and produce only
/// bounded warning codes after installation. No supplied environment value is
/// retained or rendered.
#[must_use]
pub fn install_from_process_environment() -> InstallOutcome {
    let format = environment_value("ERABI_LOG_FORMAT");
    let level = environment_value("ERABI_LOG_LEVEL");
    let settings = ParsedTelemetrySettings::from_values(format.as_deref(), level.as_deref());
    let resolved_format = settings.format.resolve(std::io::stderr().is_terminal());
    let outcome = install(TelemetryConfig::new(resolved_format, settings.level));

    if settings.invalid_format {
        emit_startup_configuration_warning(ConfigurationWarning::InvalidLogFormat);
    }
    if settings.invalid_level {
        emit_startup_configuration_warning(ConfigurationWarning::InvalidLogLevel);
    }
    outcome
}

fn environment_value(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => Some(String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_auto_format_and_info_level() {
        let settings = ParsedTelemetrySettings::from_values(None, None);
        assert_eq!(settings.format, LogFormatSetting::Auto);
        assert_eq!(settings.level, TelemetryLevel::Info);
        assert!(!settings.invalid_format);
        assert!(!settings.invalid_level);
    }

    #[test]
    fn invalid_values_fall_back_without_retaining_input() {
        let settings = ParsedTelemetrySettings::from_values(
            Some("DO_NOT_LOG_FORMAT_SECRET"),
            Some("DO_NOT_LOG_LEVEL_SECRET"),
        );
        assert_eq!(settings.format, LogFormatSetting::Auto);
        assert_eq!(settings.level, TelemetryLevel::Info);
        assert!(settings.invalid_format);
        assert!(settings.invalid_level);
    }

    #[test]
    fn all_supported_levels_and_formats_parse() {
        for value in ["trace", "debug", "info", "warn", "error"] {
            let settings = ParsedTelemetrySettings::from_values(Some("json"), Some(value));
            assert!(!settings.invalid_level);
        }
        for value in ["human", "json"] {
            let settings = ParsedTelemetrySettings::from_values(Some(value), Some("info"));
            assert!(!settings.invalid_format);
        }
    }

    #[test]
    fn auto_format_uses_terminal_detection() {
        let settings = ParsedTelemetrySettings::from_values(Some("auto"), None);
        assert_eq!(settings.format.resolve(true), TelemetryFormat::Human);
        assert_eq!(settings.format.resolve(false), TelemetryFormat::Json);
    }
}
