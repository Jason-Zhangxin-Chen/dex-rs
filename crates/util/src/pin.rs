use core_affinity;
use tracing::error;

/// Pins the current thread to the configured core.
pub fn pin_current_thread(core_id: Option<usize>) {
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
