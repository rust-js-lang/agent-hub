#![cfg_attr(not(test), deny(clippy::unwrap_used))]
//! Application-independent cooperative process supervision for Unix services.
//!
//! The engine owns process and listener lifetimes. Applications supply identity,
//! candidate validation, readiness probes, worker invocation and notifications.
pub mod addresses;
pub mod handoff;
pub mod installation;
pub mod listener;
pub mod runtime;
pub mod supervisor;
pub mod worker;

use anyhow::Result;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{path::Path, process::Command, time::Duration};
pub use supervisor::{Config, Deferred, Swap, Swapped};

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Serializable application settings, flattened into the worker configuration.
/// Hooks execute in the supervisor and must be bounded. A successful preflight
/// must not mutate live application state. Readiness must exercise the real
/// service and its state through every protected listener.
/// All durable writes must remain readable by the predecessor during rollback
/// and post-activation crash recovery; breaking migrations need a multi-release
/// application rollout. Hooks must not change the engine's lifecycle budgets.
pub trait Application: Clone + std::fmt::Debug + Serialize + DeserializeOwned {
    fn name(&self) -> &'static str;
    fn runtime(&self) -> runtime::Runtime;
    fn inspect(&self, binary: &Path) -> Result<BuildInfo>;
    fn preflight(&self, binary: &Path, config: &Config<Self>) -> Result<()>;
    fn probe(
        &self,
        config: &Config<Self>,
        build: &str,
        timeout: Duration,
    ) -> Vec<addresses::ListenAddress>;
    /// Configure arguments/environment only; descriptor inheritance is owned by
    /// the engine. The lifecycle channel will occupy fd 3 + listener_count.
    fn worker_command(
        &self,
        binary: &Path,
        config: &Config<Self>,
        listener_count: usize,
    ) -> Command;
    fn announce(&self, config: &Config<Self>, message: &str) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    pub version: String,
    pub build: String,
    pub target: String,
    pub protocol: u32,
    #[serde(default)]
    pub handoff_protocol: u32,
    #[serde(default)]
    pub handoff_startup: bool,
}

impl BuildInfo {
    /// Advertise this linked engine's lifecycle capability, independently of
    /// the application's own protocol and version.
    pub fn new(version: String, build: String, target: String, protocol: u32) -> Self {
        Self {
            version,
            build,
            target,
            protocol,
            handoff_protocol: handoff::VERSION,
            handoff_startup: true,
        }
    }
}

#[cfg(test)]
mod test_support {
    use super::*;
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct TestApplication {
        #[serde(flatten)]
        pub settings: std::collections::BTreeMap<String, serde_json::Value>,
    }
    impl Application for TestApplication {
        fn name(&self) -> &'static str {
            "fixture-service"
        }
        fn runtime(&self) -> runtime::Runtime {
            runtime::Runtime {
                home_directory: ".process-supervisor-tests",
                temporary_prefix: "ps-test",
            }
        }
        fn inspect(&self, binary: &Path) -> Result<BuildInfo> {
            std::fs::metadata(binary)?;
            Ok(BuildInfo {
                version: "fixture".into(),
                build: "fixture".into(),
                target: "fixture".into(),
                protocol: 1,
                handoff_protocol: handoff::VERSION,
                handoff_startup: true,
            })
        }
        fn preflight(&self, _: &Path, _: &Config<Self>) -> Result<()> {
            Ok(())
        }
        fn probe(&self, _: &Config<Self>, _: &str, _: Duration) -> Vec<addresses::ListenAddress> {
            Vec::new()
        }
        fn worker_command(&self, binary: &Path, _: &Config<Self>, _: usize) -> Command {
            Command::new(binary)
        }
        fn announce(&self, _: &Config<Self>, _: &str) -> Result<()> {
            Ok(())
        }
    }
}
