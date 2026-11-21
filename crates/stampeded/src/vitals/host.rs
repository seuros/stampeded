//! Readings from the host this worker runs on. Blocking: run on a blocking thread.

use super::{DiskReading, Readings, ThermalReading, VitalsConfig};
use std::path::Path;
use vitals_machines::probe::{
    StorageRole, StorageSnapshot, StorageTarget, ThermalState, inspect_machine, inspect_storage,
};

pub(super) fn read(config: &VitalsConfig) -> Readings {
    let disks = if config.disks.is_empty() {
        Vec::new()
    } else {
        let targets: Vec<StorageTarget> = config
            .disks
            .iter()
            .map(|d| StorageTarget {
                role: StorageRole::State,
                path: d.path.clone(),
            })
            .collect();
        let snapshot = inspect_storage(&targets);
        config
            .disks
            .iter()
            .map(|d| disk_reading(&snapshot, &d.path))
            .collect()
    };
    // Only the thermal part is used; the rest of the snapshot costs a few cheap reads.
    let thermal = config
        .thermal_pressure
        .then(|| thermal_reading(inspect_machine().thermal.state));
    Readings { disks, thermal }
}

/// The filesystem `path` resolved to, or why it could not be read.
pub(super) fn disk_reading(snapshot: &StorageSnapshot, path: &Path) -> DiskReading {
    if let Some(fs) = snapshot
        .filesystems
        .iter()
        .find(|fs| fs.targets.iter().any(|t| t.target.path == path))
    {
        return DiskReading::Space {
            available: fs.available_bytes,
            total: fs.total_bytes,
        };
    }
    snapshot
        .unavailable
        .iter()
        .find(|f| f.target.path == path)
        .map_or_else(
            || DiskReading::Unreadable("not reported by the probe".into()),
            |f| DiskReading::Unreadable(f.reason.clone()),
        )
}

pub(super) const fn thermal_reading(state: ThermalState) -> ThermalReading {
    match state {
        ThermalState::Normal => ThermalReading::Normal,
        ThermalState::Warning => ThermalReading::Pressure,
        ThermalState::Critical => ThermalReading::Critical,
        ThermalState::Unknown => ThermalReading::Unknown,
    }
}
