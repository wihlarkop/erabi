//! Erabi command-line entry point.

fn main() {
    if let Err(error) = erabi::BootstrapConfig::from_process_environment() {
        // `BootstrapConfig` errors intentionally name only the configuration
        // field that failed; they never include supplied values or secrets.
        eprintln!("erabi bootstrap configuration error: {error}");
        std::process::exit(2);
    }
}
