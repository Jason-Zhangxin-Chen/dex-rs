//! The OMS engine runtime: the shared state of the process (config, mode,
//! shutdown), the master and the slave loops, and the mode switch.
//!
//! The core thread is the single owner of the book and of all the service
//! handles; it runs the master spin loop or the slave consume loop. The
//! signal handlers only write the shared flags: a SIGHUP reload flips the
//! mode flag (and notifies the watch channel the slave awaits on), SIGTERM
//! and SIGINT set the shutdown flag. The loops poll the flags, so the rewire
//! of a promotion happens on the core thread itself and needs no locks
//! around the hot structures.

pub mod master;
pub mod slave;

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use async_nats::jetstream;
use primitives::base::Symbol;
use tracing::error;

use crate::config::{Mode, OmsConfig};

/// Value of the shared mode flag in master mode.
pub const MODE_MASTER: u8 = 0;
/// Value of the shared mode flag in slave mode.
pub const MODE_SLAVE: u8 = 1;

/// Converts a config mode into the shared flag value.
fn mode_value(mode: Mode) -> u8 {
    match mode {
        Mode::Master => MODE_MASTER,
        Mode::Slave => MODE_SLAVE,
    }
}

/// Errors of the OMS engine.
#[derive(Debug)]
pub enum EngineError {
    /// A file or queue operation failed.
    Io(std::io::Error),
    /// The config could not be loaded.
    Config(crate::config::ConfigError),
    /// A journal operation failed.
    Journal(crate::journal::JournalError),
    /// A storage (Redis) operation failed.
    Storage(storage::StorageError),
    /// A NATS client or server error, already formatted.
    Nats(String),
    /// A MessagePack encode / decode error, already formatted.
    Codec(String),
    /// A runtime wiring error, already formatted.
    Runtime(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Io(err) => write!(f, "engine io: {err}"),
            EngineError::Config(err) => write!(f, "engine config: {err}"),
            EngineError::Journal(err) => write!(f, "engine journal: {err}"),
            EngineError::Storage(err) => write!(f, "engine storage: {err}"),
            EngineError::Nats(what) => write!(f, "engine nats: {what}"),
            EngineError::Codec(what) => write!(f, "engine codec: {what}"),
            EngineError::Runtime(what) => write!(f, "engine runtime: {what}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Io(err) => Some(err),
            EngineError::Config(err) => Some(err),
            EngineError::Journal(err) => Some(err),
            EngineError::Storage(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        EngineError::Io(err)
    }
}

impl From<crate::config::ConfigError> for EngineError {
    fn from(err: crate::config::ConfigError) -> Self {
        EngineError::Config(err)
    }
}

impl From<crate::journal::JournalError> for EngineError {
    fn from(err: crate::journal::JournalError) -> Self {
        EngineError::Journal(err)
    }
}

impl From<storage::StorageError> for EngineError {
    fn from(err: storage::StorageError) -> Self {
        EngineError::Storage(err)
    }
}

/// Connects to the first reachable NATS URL and ensures the symbol's
/// JetStream stream exists. Runs in a blocking context (startup only).
pub(crate) async fn connect_jetstream(
    config: &crate::config::NatsConfig,
    symbol: Symbol,
) -> Result<jetstream::Context, EngineError> {
    let mut last_error: Option<String> = None;
    for url in &config.urls {
        match async_nats::connect(url.as_str()).await {
            Ok(client) => {
                let context = jetstream::new(client);
                context
                    .get_or_create_stream(jetstream::stream::Config {
                        name: config.stream(symbol),
                        subjects: vec![config.subject(symbol)],
                        ..Default::default()
                    })
                    .await
                    .map_err(|err| EngineError::Nats(err.to_string()))?;
                return Ok(context);
            }
            Err(err) => last_error = Some(err.to_string()),
        }
    }
    Err(EngineError::Nats(format!(
        "cannot connect to any NATS url {:?}: {}",
        config.urls,
        last_error.unwrap_or_else(|| "no urls configured".to_string())
    )))
}

/// Pins the current thread to the configured core.
pub(crate) fn pin_current_thread(core_id: Option<usize>) {
    let Some(core_id) = core_id else { return };
    let Some(core_ids) = core_affinity::get_core_ids() else {
        error!("no core ids available, the core thread stays unpinned");
        return;
    };
    match core_ids.iter().find(|core| core.id == core_id) {
        Some(core) => {
            core_affinity::set_for_current(*core);
        }
        None => error!(
            core_id = core_id,
            "the configured core id is not available, the core thread stays unpinned"
        ),
    }
}

/// Shared state of the OMS engine. The signal handlers write into it, the
/// core thread reads it.
pub struct Runtime {
    /// The active configuration, swapped by a SIGHUP reload.
    config: Arc<RwLock<OmsConfig>>,
    /// The mode flag the core loops observe.
    mode: Arc<AtomicU8>,
    /// Watch channel announcing mode flips, awaited by the slave loop.
    mode_changed: tokio::sync::watch::Sender<u8>,
    /// The shutdown flag, set by SIGTERM / SIGINT.
    shutdown: Arc<AtomicBool>,
}

impl Runtime {
    /// Creates the runtime around the initial configuration.
    pub fn new(config: OmsConfig) -> Self {
        let mode = Arc::new(AtomicU8::new(mode_value(config.mode)));
        let (mode_changed, _) = tokio::sync::watch::channel(mode.load(Ordering::Relaxed));
        Self {
            config: Arc::new(RwLock::new(config)),
            mode,
            mode_changed,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The shared config, for the signal handler.
    pub fn config(&self) -> &Arc<RwLock<OmsConfig>> {
        &self.config
    }

    /// The shutdown flag, for the signal handler.
    pub fn shutdown(&self) -> &Arc<AtomicBool> {
        &self.shutdown
    }

    /// Applies a reloaded config. A mode flip is stored in the flag and
    /// announced on the watch channel, so the slave loop wakes up and
    /// performs the promotion.
    pub fn reload(&self, config: OmsConfig) {
        let old_mode = self.mode.load(Ordering::Relaxed);
        let new_mode = mode_value(config.mode);
        *self.config.write().expect("config lock") = config;
        if old_mode != new_mode {
            self.mode.store(new_mode, Ordering::Relaxed);
            let _ = self.mode_changed.send(new_mode);
        }
    }

    /// Launches the engine: spawns and pins the core thread, and blocks
    /// until it exits (shutdown or a fatal error).
    pub fn launch(&self) -> Result<(), EngineError> {
        let config = Arc::clone(&self.config);
        let (initial_mode, core_id) = {
            let config = config.read().expect("config lock");
            (config.mode, config.core_id)
        };
        let mode = Arc::clone(&self.mode);
        let shutdown = Arc::clone(&self.shutdown);
        let mode_rx = self.mode_changed.subscribe();

        let thread = std::thread::Builder::new()
            .name("oms-core".to_string())
            .spawn(move || {
                pin_current_thread(core_id);
                let result = match initial_mode {
                    Mode::Master => master::run(config, mode, shutdown),
                    Mode::Slave => slave::run(config, mode, shutdown, mode_rx),
                };
                if let Err(err) = result {
                    error!(error = %err, "the OMS engine stopped with an error");
                }
            })
            .map_err(|err| EngineError::Runtime(format!("cannot spawn the core thread: {err}")))?;
        thread.join().map_err(|_| EngineError::Runtime("the core thread panicked".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(mode: Mode) -> OmsConfig {
        OmsConfig::from_toml(&format!(
            r#"
mode = "{}"
symbol = "TEST"
[ingress]
path = "/tmp/q"
capacity = 1024
[settlement]
path = "/tmp/s"
capacity = 1024
"#,
            match mode {
                Mode::Master => "master",
                Mode::Slave => "slave",
            }
        ))
        .unwrap()
    }

    #[test]
    fn test_reload_flips_mode_and_announces() {
        let runtime = Runtime::new(config(Mode::Slave));
        let mode_rx = runtime.mode_changed.subscribe();
        assert_eq!(*mode_rx.borrow(), MODE_SLAVE);

        runtime.reload(config(Mode::Master));
        assert_eq!(runtime.mode.load(Ordering::Relaxed), MODE_MASTER);
        assert_eq!(*mode_rx.borrow(), MODE_MASTER);
        assert_eq!(runtime.config.read().expect("config lock").mode, Mode::Master);
    }

    #[test]
    fn test_reload_without_mode_flip_keeps_watch_silent() {
        let runtime = Runtime::new(config(Mode::Slave));
        let mode_rx = runtime.mode_changed.subscribe();
        assert_eq!(*mode_rx.borrow(), MODE_SLAVE);
        // A same-mode reload (e.g. a snapshot interval change) must not
        // wake the slave's consume loop: the watch stays unchanged.
        assert!(!mode_rx.has_changed().expect("the watch sender is alive"));
        runtime.reload(config(Mode::Slave));
        assert!(!mode_rx.has_changed().expect("the watch sender is alive"));
    }

    #[test]
    fn test_mode_values() {
        assert_eq!(mode_value(Mode::Master), MODE_MASTER);
        assert_eq!(mode_value(Mode::Slave), MODE_SLAVE);
    }
}
