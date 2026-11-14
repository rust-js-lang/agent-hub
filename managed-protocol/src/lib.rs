//! Local managed conversations. Read receipts and execution are independent.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use protocol::Message;

pub mod endpoints;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Runtime {
    Codex,
    Claude,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub name: String,
    pub generation: String,
    pub owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub name: String,
    pub generation: String,
    pub machine: String,
    pub runtime: Runtime,
    pub cwd: String,
    pub runtime_session_id: Option<String>,
    pub lease_until: u64,
    pub turns: u32,
    pub max_turns: u32,
    pub active: bool,
    pub approvals: Vec<Approval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    pub message: Message,
    pub generation: String,
    pub state: DeliveryState,
    pub runtime_turn_id: Option<String>,
    pub output: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryState {
    Queued,
    Running,
    Completed,
    Failed,
    Uncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    pub id: String,
    pub request: Value,
    pub response: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Update {
    SessionReady {
        session_id: String,
    },
    Started {
        message_id: u64,
        turn_id: String,
    },
    Output {
        message_id: u64,
        text: String,
    },
    Finished {
        message_id: u64,
        state: DeliveryState,
        error: Option<String>,
    },
    Approval {
        message_id: u64,
        id: String,
        request: Value,
    },
    ApprovalResolved {
        id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Command {
    Open {
        name: String,
        owner: String,
        machine: String,
        runtime: Runtime,
        cwd: String,
        max_turns: u32,
        include_channels: bool,
    },
    Poll {
        lease: Lease,
        claim: bool,
    },
    Update {
        lease: Lease,
        update: Update,
    },
    Release {
        lease: Lease,
    },
    Respond {
        name: String,
        generation: String,
        id: String,
        response: Value,
        key: String,
    },
    Resolve {
        name: String,
        generation: String,
        message_id: u64,
        state: DeliveryState,
        key: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub session: Session,
    pub delivery: Option<Delivery>,
}
