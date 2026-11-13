//! Hub adapter for the reusable cooperative process supervisor.
use anyhow::Result;
use process_supervisor::{Application, BuildInfo};
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Command, time::Duration};

pub use process_supervisor::supervisor::{
    ArcSignal, Deferred, Swap, Swapped, atomic_json, bind_listener, request, snapshot,
};
pub type Config = process_supervisor::Config<HubApplication>;

/// Hub policy stays here; the process engine never sees agent or message state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubApplication {
    pub away_after: u64,
    pub forget_after: u64,
    pub reconnect_grace: u64,
}
impl Application for HubApplication {
    fn name(&self) -> &'static str {
        "cross-agent-message"
    }
    fn runtime(&self) -> process_supervisor::runtime::Runtime {
        process_supervisor::runtime::Runtime {
            home_directory: ".cross-agent-message",
            temporary_prefix: "cam",
        }
    }
    fn inspect(&self, binary: &Path) -> Result<BuildInfo> {
        crate::updates::inspect(binary)
    }
    fn preflight(&self, binary: &Path, config: &Config) -> Result<()> {
        crate::upgrade::preflight(binary, config)
    }
    fn probe(
        &self,
        config: &Config,
        build: &str,
        timeout: Duration,
    ) -> Vec<process_supervisor::addresses::ListenAddress> {
        crate::addresses::probe_for(config, build, timeout)
            .into_iter()
            .map(crate::addresses::process_address)
            .collect()
    }
    fn worker_command(&self, binary: &Path, config: &Config, listener_count: usize) -> Command {
        let mut command = Command::new(binary);
        command
            .arg("worker")
            .arg("--config")
            .arg(config.config_path())
            .env("CROSS_AGENT_LISTEN_COUNT", listener_count.to_string())
            .env("CROSS_AGENT_READY_FD", (3 + listener_count).to_string())
            .env("CROSS_AGENT_CONTROL_SOCK", config.control());
        command
    }
    fn announce(&self, config: &Config, message: &str) -> Result<()> {
        crate::upgrade::announce(config, message)
    }
}

pub fn run(config: Config) -> Result<()> {
    process_supervisor::supervisor::run(config)
}
pub fn load_config(db: &Path) -> Result<Config> {
    process_supervisor::supervisor::load_config(db)
}
pub fn stop(db: &Path) -> Result<()> {
    process_supervisor::supervisor::stop_in(
        db,
        HubApplication {
            away_after: 0,
            forget_after: 0,
            reconnect_grace: 0,
        }
        .runtime(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattened_hub_settings_preserve_existing_configuration_and_ignore_future_fields() {
        let legacy = serde_json::json!({
            "addresses": ["127.0.0.1:7878"], "db": "/tmp/hub.db",
            "away_after": 60000, "forget_after": 120000, "reconnect_grace": 30000,
            "activate_current": false, "worker_path": "/tmp/cross-agent-message",
            "requested_addresses": [], "required_ready": [],
            "handoff_protocol": 1, "handoff_startup": true
        });
        let mut future = legacy.clone();
        future["future_engine_setting"] = serde_json::json!("ignored");
        future["future_hub_setting"] = serde_json::json!(17);
        let config: Config = serde_json::from_value(future).unwrap();
        config.validate().unwrap();
        assert_eq!(config.away_after, 60000);
        assert_eq!(config.forget_after, 120000);
        assert_eq!(config.reconnect_grace, 30000);
        assert_eq!(serde_json::to_value(&config).unwrap(), legacy);
        // Old supervisors had none of the cooperative lifecycle fields.
        let mut oldest = legacy;
        for key in [
            "handoff_protocol",
            "handoff_startup",
            "required_ready",
            "requested_addresses",
            "worker_path",
            "activate_current",
        ] {
            oldest.as_object_mut().unwrap().remove(key);
        }
        let config: Config = serde_json::from_value(oldest).unwrap();
        assert_eq!(config.handoff_protocol, 0);
        assert!(!config.handoff_startup);
        assert!(config.worker_path.is_none());
    }

    #[test]
    fn readiness_wire_matches_the_public_hub_address_schema() {
        for state in [
            protocol::AddressState::Ready,
            protocol::AddressState::Pending,
            protocol::AddressState::Blocked,
            protocol::AddressState::Unknown,
        ] {
            let address = protocol::ListenAddress {
                address: "127.0.0.1:7878".into(),
                state,
                reason: Some("fixture".into()),
                remote_seen: true,
            };
            let process = crate::addresses::process_address(address.clone());
            assert_eq!(
                serde_json::to_value(process).unwrap(),
                serde_json::to_value(address).unwrap()
            );
        }
    }
}
