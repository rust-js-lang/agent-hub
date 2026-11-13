use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AddressState {
    Ready,
    Pending,
    Blocked,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListenAddress {
    pub address: String,
    pub state: AddressState,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub remote_seen: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ready {
    pub build: String,
    pub addresses: Vec<ListenAddress>,
    #[serde(default)]
    pub handoff_protocol: u32,
    #[serde(default)]
    pub request: u64,
    #[serde(default)]
    pub accepted: Vec<String>,
    #[serde(default)]
    pub first_accepted_ns: std::collections::BTreeMap<String, u64>,
}

#[derive(Serialize, Deserialize)]
pub struct Observation {
    pub pid: u32,
    pub addresses: Vec<ListenAddress>,
}

pub fn ready(addresses: &[ListenAddress]) -> Vec<String> {
    addresses
        .iter()
        .filter(|a| a.state == AddressState::Ready)
        .map(|a| a.address.clone())
        .collect()
}
