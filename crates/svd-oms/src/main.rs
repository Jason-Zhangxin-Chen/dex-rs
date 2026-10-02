//! SVD_OMS entry point: loads the config, installs the signal handlers
//! (SIGHUP reloads the config, SIGTERM / SIGINT stop the engine) and
//! launches the engine in the configured mode.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use svd_oms::config::OmsConfig;
use svd_oms::engine::Runtime;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let path = std::env::args().nth(1).expect("usage: svd-oms <config.toml>");
    let config = OmsConfig::load(&path)?;
    let runtime = Arc::new(Runtime::new(config));
    info!("the OMS engine is starting");

    // The signal thread: a SIGHUP reloads the config (a mode flip promotes
    // a slave), the termination signals stop the engine.
    let mut signals = Signals::new([SIGHUP, SIGTERM, SIGINT])?;
    let handle = signals.handle();
    let signal_runtime = Arc::clone(&runtime);
    let signal_path = path.clone();
    std::thread::Builder::new().name("oms-signals".to_string()).spawn(move || {
        for signal in signals.forever() {
            match signal {
                SIGHUP => match OmsConfig::load(&signal_path) {
                    Ok(config) => {
                        info!("configuration update event received, applying the reload");
                        signal_runtime.reload(config);
                    }
                    Err(err) => {
                        error!(error = %err, "cannot reload the config, keeping the current one");
                    }
                },
                _ => {
                    info!("termination signal received, stopping the engine");
                    signal_runtime.shutdown().store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
    })?;

    // Blocks until the core thread exits.
    runtime.launch()?;
    handle.close();
    info!("the OMS engine stopped");
    Ok(())
}
