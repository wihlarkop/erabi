//! Erabi process entry point.

#[tokio::main]
async fn main() {
    let command = std::env::args().nth(1);
    if command.as_deref().is_some_and(|command| command != "serve") {
        eprintln!("usage: erabi [serve]");
        std::process::exit(2);
    }

    let runtime = match erabi::RunningRuntime::start_from_bootstrap(
        erabi::BootstrapConfig::from_process_environment(),
    )
    .await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("erabi startup error: {error}");
            std::process::exit(2);
        }
    };
    if let Err(error) = runtime.serve_until_signal().await {
        eprintln!("erabi runtime error: {error}");
        std::process::exit(1);
    }
}
