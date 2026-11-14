use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use managed_protocol::{Command, Response, Update};
use protocol::{Event, Register, check_name};
use std::sync::Arc;

use super::{Hub, Refused, bad_request, check_key, now};
use crate::store::SessionError;

impl From<SessionError> for Refused {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::Storage(error) => Self::from(error),
            SessionError::Refused(reason) => Self(StatusCode::CONFLICT, reason),
        }
    }
}

pub(super) async fn sessions(
    State(hub): State<Arc<Hub>>,
    _: Query<()>,
) -> Result<Json<Vec<Response>>, Refused> {
    Ok(Json(hub.store().sessions()?))
}

pub(super) async fn command(
    State(hub): State<Arc<Hub>>,
    _: HeaderMap,
    Json(command): Json<Command>,
) -> Result<Json<Response>, Refused> {
    let _lifecycle = hub.lifecycle();
    match &command {
        Command::Open {
            name,
            owner,
            machine,
            cwd,
            max_turns,
            ..
        } => {
            check_name(name).map_err(bad_request)?;
            if owner.len() < 16 || owner.len() > 128 {
                return Err(bad_request(
                    "Worker owner must have 16 to 128 characters".into(),
                ));
            }
            if !std::path::Path::new(cwd).is_absolute() || *max_turns == 0 {
                return Err(bad_request(
                    "An absolute working directory and positive turn budget are required".into(),
                ));
            }
            if hub
                .store()
                .agent(name)?
                .is_some_and(|agent| agent.person || agent.retired_at.is_some())
            {
                return Err(bad_request("Cannot manage a person or retired agent; explicitly register a new agent life first".into()));
            }
            hub.store().see(
                &Register {
                    name: name.clone(),
                    machine: machine.clone(),
                    reconnect: true,
                },
                now(),
            )?;
        }
        Command::Respond { key, .. } | Command::Resolve { key, .. } => check_key(&hub, key)?,
        _ => {}
    }
    let response = hub.store().session_command(&command, now())?;
    hub.store().touch(&response.session.name, now())?;
    if let Command::Update {
        lease,
        update: Update::Started { message_id, .. },
    } = &command
    {
        hub.tell(Event::Read {
            name: lease.name.clone(),
            ids: vec![*message_id],
        });
    }
    hub.tell_agent(&response.session.name)?;
    Ok(Json(response))
}
