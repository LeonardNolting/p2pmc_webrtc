pub use tracing::subscriber::SetGlobalDefaultError;
use tracing_subscriber::EnvFilter;

pub fn start_logger() -> Result<(), SetGlobalDefaultError> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_thread_names(true)
        // Don't display the event's target (module path)
        .with_target(false)
        .with_env_filter("info,mainline=error")
        // Build the subscriber
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}
