//! The hub: the HTTP API every agent and the dashboard use, and the
//! dashboard's files.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{self, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use protocol::{
    AGENTS, Agent, EVENTS, Event, HISTORY_LIMIT, INBOX, Inbox, MESSAGES, Message, ReadInbox,
    Refusal, Register, Send, check_name,
};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, wrappers::BroadcastStream};
use tower_http::services::ServeDir;

use crate::store::Store;

pub struct Hub {
    store: Mutex<Store>,
    events: broadcast::Sender<Event>,
    /// How many `watch`es of each agent are connected.
    watchers: Mutex<HashMap<String, usize>>,
}

/// Milliseconds since the Unix epoch.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_millis() as u64
}

/// Why a request was refused, and its status.
pub struct Refused(StatusCode, String);

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        (self.0, Json(Refusal { reason: self.1 })).into_response()
    }
}

impl From<rusqlite::Error> for Refused {
    fn from(error: rusqlite::Error) -> Refused {
        Refused(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the hub's store failed: {error}"),
        )
    }
}

fn bad_request(reason: String) -> Refused {
    Refused(StatusCode::BAD_REQUEST, reason)
}

impl Hub {
    pub fn new(store: Store) -> Hub {
        Hub {
            store: Mutex::new(store),
            events: broadcast::channel(256).0,
            watchers: Mutex::new(HashMap::new()),
        }
    }

    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store
            .lock()
            .expect("no request panicked holding the store")
    }

    fn watching(&self, name: &str) -> bool {
        self.watchers
            .lock()
            .expect("the watchers")
            .get(name)
            .is_some_and(|&count| count > 0)
    }

    /// The agent named `name`, or a refusal saying how to find one.
    fn agent(&self, name: &str) -> Result<Agent, Refused> {
        match self.store().agent(name)? {
            Some(agent) => Ok(Agent {
                watching: self.watching(name),
                ..agent
            }),
            None => Err(Refused(
                StatusCode::NOT_FOUND,
                format!("no agent is named {name}: `cross-agent-message agents` lists them"),
            )),
        }
    }

    /// Tells every stream. One with no listener is fine.
    fn tell(&self, event: Event) {
        let _ = self.events.send(event);
    }

    fn tell_agent(&self, name: &str) -> Result<(), Refused> {
        let agent = self.agent(name)?;
        self.tell(Event::Agent { agent });
        Ok(())
    }
}

/// The hub's routes, and `dashboard`'s files at every other path.
pub fn router(hub: Arc<Hub>, dashboard: PathBuf) -> Router {
    Router::new()
        .route(AGENTS, get(agents).post(register))
        .route(MESSAGES, get(history).post(send))
        .route(INBOX, post(read_inbox))
        .route(EVENTS, get(events))
        .fallback_service(ServeDir::new(dashboard))
        .with_state(hub)
}

async fn agents(State(hub): State<Arc<Hub>>) -> Result<Json<Vec<Agent>>, Refused> {
    let agents = hub.store().agents()?;
    Ok(Json(
        agents
            .into_iter()
            .map(|agent| Agent {
                watching: hub.watching(&agent.name),
                ..agent
            })
            .collect(),
    ))
}

async fn register(
    State(hub): State<Arc<Hub>>,
    Json(agent): Json<Register>,
) -> Result<Json<Agent>, Refused> {
    check_name(&agent.name).map_err(bad_request)?;
    hub.store().see(&agent, now())?;
    let agent = hub.agent(&agent.name)?;
    hub.tell(Event::Agent {
        agent: agent.clone(),
    });
    Ok(Json(agent))
}

#[derive(Deserialize)]
struct HistoryQuery {
    agent: Option<String>,
    limit: Option<u32>,
}

async fn history(
    State(hub): State<Arc<Hub>>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<Message>>, Refused> {
    let limit = query.limit.unwrap_or(HISTORY_LIMIT).min(HISTORY_LIMIT);
    Ok(Json(hub.store().history(query.agent.as_deref(), limit)?))
}

async fn send(
    State(hub): State<Arc<Hub>>,
    Json(send): Json<Send>,
) -> Result<Json<Message>, Refused> {
    hub.agent(&send.from)?;
    hub.agent(&send.to)?;
    if send.from == send.to {
        return Err(bad_request(format!(
            "{} can't send a message to itself",
            send.from
        )));
    }
    if send.body.trim().is_empty() {
        return Err(bad_request("a message can't be empty".to_string()));
    }
    let message = hub.store().send(&send, now())?;
    hub.tell(Event::Sent {
        message: message.clone(),
    });
    hub.tell_agent(&send.to)?;
    Ok(Json(message))
}

async fn read_inbox(
    State(hub): State<Arc<Hub>>,
    Json(read): Json<ReadInbox>,
) -> Result<Json<Inbox>, Refused> {
    hub.agent(&read.name)?;
    let inbox = hub.store().read_inbox(&read.name, read.max, now())?;
    if !inbox.messages.is_empty() {
        hub.tell(Event::Read {
            name: read.name.clone(),
            ids: inbox.messages.iter().map(|message| message.id).collect(),
        });
        hub.tell_agent(&read.name)?;
    }
    Ok(Json(inbox))
}

#[derive(Deserialize)]
struct EventsQuery {
    agent: Option<String>,
}

/// While it's held, its agent is watching.
struct Watch {
    hub: Arc<Hub>,
    name: String,
}

impl Watch {
    fn start(hub: Arc<Hub>, name: String) -> Result<Watch, Refused> {
        hub.agent(&name)?;
        *hub.watchers
            .lock()
            .expect("the watchers")
            .entry(name.clone())
            .or_default() += 1;
        hub.tell_agent(&name)?;
        Ok(Watch { hub, name })
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        if let Some(count) = self
            .hub
            .watchers
            .lock()
            .expect("the watchers")
            .get_mut(&self.name)
        {
            *count -= 1;
        }
        let _ = self.hub.tell_agent(&self.name);
    }
}

async fn events(
    State(hub): State<Arc<Hub>>,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<sse::Event, Infallible>>>, Refused> {
    // Subscribed before the watch starts, so the stream says it started.
    let events = BroadcastStream::new(hub.events.subscribe());
    let watch = query
        .agent
        .map(|name| Watch::start(hub, name))
        .transpose()?;
    // A stream that fell behind skips what it missed: an agent's inbox,
    // and the dashboard's next fetch, still have it.
    let stream = events.filter_map(move |event| {
        let event = event.ok()?;
        if let Some(watch) = &watch
            && !matches!(&event, Event::Sent { message } if message.to == watch.name)
        {
            return None;
        }
        let json = serde_json::to_string(&event).expect("an event is JSON");
        Some(Ok(sse::Event::default().data(json)))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
