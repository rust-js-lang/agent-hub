//! Preserve the hub IPC namespace while sharing filesystem safety primitives.
use std::path::{Path, PathBuf};
const RUNTIME: process_supervisor::runtime::Runtime = process_supervisor::runtime::Runtime {
    home_directory: ".cross-agent-message",
    temporary_prefix: "cam",
};
pub fn socket(db: &Path, name: &str) -> PathBuf {
    RUNTIME.socket(db, name)
}
pub fn prepare(db: &Path) -> anyhow::Result<()> {
    RUNTIME.prepare(db)
}
pub use process_supervisor::runtime::legacy_alias;
