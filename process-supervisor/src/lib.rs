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
    fn runtime() -> runtime::Runtime;
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
    fn announce(&self, config: &Config<Self>, event: &SupervisorEvent) -> Result<()>;
}

/// A durable lifecycle outcome for the application to word or deliver.
#[derive(Debug, Clone)]
pub enum SupervisorEvent {
    AutomaticRollback { build: String, crashes: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Lifecycle capability comes from the linked engine or deserialized candidate
/// identity, rather than being writable by an application adapter.
///
/// ```compile_fail
/// let mut info = process_supervisor::BuildInfo::new("1".into(), "build".into(), "target".into(), 1);
/// info.handoff_protocol = 99;
/// ```
pub struct BuildInfo {
    pub version: String,
    pub build: String,
    pub target: String,
    pub protocol: u32,
    #[serde(default)]
    handoff_protocol: u32,
    #[serde(default)]
    handoff_startup: bool,
}

impl BuildInfo {
    pub fn handoff_protocol(&self) -> u32 {
        self.handoff_protocol
    }
    pub fn handoff_startup(&self) -> bool {
        self.handoff_startup
    }
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
        fn runtime() -> runtime::Runtime {
            runtime::Runtime {
                home_directory: ".process-supervisor-tests",
                temporary_prefix: "ps-test",
            }
        }
        fn inspect(&self, binary: &Path) -> Result<BuildInfo> {
            if self
                .settings
                .get("inspect_error")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                anyhow::bail!("fixture inspection hook failed");
            }
            std::fs::metadata(binary)?;
            Ok(BuildInfo::new(
                "fixture".into(),
                "fixture".into(),
                "fixture".into(),
                1,
            ))
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
        fn announce(&self, _: &Config<Self>, _: &SupervisorEvent) -> Result<()> {
            Ok(())
        }
    }
}
