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
    AGENTS, Agent, EVENTS, Event, HISTORY_LIMIT, HOLD_MINUTES_MAX, HOLDS, Hold, INBOX, Inbox,
    MESSAGES, Message, RELEASE, ReadInbox, Refusal, Register, Release, Send, Take, Taken,
    check_hold, check_name,
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
    /// What's held now, by its name. Not kept in the store: a hold is of
    /// what runs now, and a hub started again has none.
    holds: Mutex<HashMap<String, Hold>>,
}

/// What a [`Take`] of `holds` at `now` comes to: the hold is `take.holder`'s
/// where it was free, lapsed or its own already, until `take.minutes` from
/// now, held since it first took it; another agent's is left as it is.
pub fn take_hold(holds: &mut HashMap<String, Hold>, take: &Take, now: u64) -> Taken {
    let held = holds.get(&take.name).filter(|hold| hold.until > now);
    if let Some(hold) = held.filter(|hold| hold.holder != take.holder) {
        return Taken {
            hold: hold.clone(),
            yours: false,
        };
    }
    let hold = Hold {
        name: take.name.clone(),
        holder: take.holder.clone(),
        since: held.map_or(now, |hold| hold.since),
        until: now + u64::from(take.minutes) * 60_000,
    };
    holds.insert(take.name.clone(), hold.clone());
    Taken { hold, yours: true }
}

/// `release.holder` lets go of its hold at `now`: gone, as one that lapsed
/// or was never taken is already. Another agent's is refused, naming it.
pub fn release_hold(
    holds: &mut HashMap<String, Hold>,
    release: &Release,
    now: u64,
) -> Result<(), String> {
    match holds.get(&release.name).filter(|hold| hold.until > now) {
        Some(hold) if hold.holder != release.holder => Err(format!(
            "{} holds {}, not {}: only its holder lets it go",
            hold.holder, release.name, release.holder
        )),
        _ => {
            holds.remove(&release.name);
            Ok(())
        }
    }
}

/// What's held at `now`, by name: a hold that lapsed is forgotten.
pub fn held(holds: &mut HashMap<String, Hold>, now: u64) -> Vec<Hold> {
    holds.retain(|_, hold| hold.until > now);
    let mut held: Vec<Hold> = holds.values().cloned().collect();
    held.sort_by(|a, b| a.name.cmp(&b.name));
    held
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
            holds: Mutex::new(HashMap::new()),
        }
    }

    fn holds(&self) -> std::sync::MutexGuard<'_, HashMap<String, Hold>> {
        self.holds
            .lock()
            .expect("no request panicked holding the holds")
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
        .route(HOLDS, get(holds).post(take))
        .route(RELEASE, post(release))
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

async fn holds(State(hub): State<Arc<Hub>>) -> Json<Vec<Hold>> {
    Json(held(&mut hub.holds(), now()))
}

async fn take(State(hub): State<Arc<Hub>>, Json(take): Json<Take>) -> Result<Json<Taken>, Refused> {
    check_hold(&take.name).map_err(bad_request)?;
    hub.agent(&take.holder)?;
    if take.minutes == 0 || take.minutes > HOLD_MINUTES_MAX {
        return Err(bad_request(format!(
            "a hold is taken for 1 to {HOLD_MINUTES_MAX} minutes, not {}: take it again to keep it longer",
            take.minutes
        )));
    }
    Ok(Json(take_hold(&mut hub.holds(), &take, now())))
}

async fn release(
    State(hub): State<Arc<Hub>>,
    Json(release): Json<Release>,
) -> Result<Json<Vec<Hold>>, Refused> {
    check_hold(&release.name).map_err(bad_request)?;
    hub.agent(&release.holder)?;
    let now = now();
    let mut holds = hub.holds();
    release_hold(&mut holds, &release, now)
        .map_err(|reason| Refused(StatusCode::CONFLICT, reason))?;
    Ok(Json(held(&mut holds, now)))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn take(name: &str, holder: &str, minutes: u32) -> Take {
        Take {
            name: name.to_string(),
            holder: holder.to_string(),
            minutes,
        }
    }

    fn release(name: &str, holder: &str) -> Release {
        Release {
            name: name.to_string(),
            holder: holder.to_string(),
        }
    }

    #[test]
    fn a_hold_is_one_agents_until_released_or_lapsed() {
        let mut holds = HashMap::new();
        let minute = 60_000;

        let first = take_hold(&mut holds, &take("check", "a", 30), 1_000);
        assert!(first.yours);
        assert_eq!(
            (first.hold.since, first.hold.until),
            (1_000, 1_000 + 30 * minute)
        );

        // Another's take leaves it, and says whose it is.
        let other = take_hold(&mut holds, &take("check", "b", 30), 2_000);
        assert!(!other.yours);
        assert_eq!(other.hold, first.hold);
        // Another name is another hold.
        assert!(take_hold(&mut holds, &take("deploy", "b", 5), 2_000).yours);

        // Its holder's take again keeps it longer, held since it first was.
        let again = take_hold(&mut holds, &take("check", "a", 10), 5_000);
        assert!(again.yours);
        assert_eq!(
            (again.hold.since, again.hold.until),
            (1_000, 5_000 + 10 * minute)
        );

        // Only its holder lets it go.
        assert_eq!(
            release_hold(&mut holds, &release("check", "b"), 6_000),
            Err("a holds check, not b: only its holder lets it go".to_string())
        );
        assert_eq!(
            release_hold(&mut holds, &release("check", "a"), 6_000),
            Ok(())
        );
        // One not held is let go already.
        assert_eq!(
            release_hold(&mut holds, &release("check", "a"), 6_000),
            Ok(())
        );
        let taken = take_hold(&mut holds, &take("check", "b", 1), 7_000);
        assert!(taken.yours);
        assert_eq!(taken.hold.since, 7_000);

        // Past its time it's anyone's, and no longer listed.
        let names = |held: Vec<Hold>| held.into_iter().map(|hold| hold.name).collect::<Vec<_>>();
        assert_eq!(
            names(held(&mut holds, 7_000 + minute - 1)),
            ["check", "deploy"]
        );
        assert_eq!(names(held(&mut holds, 7_000 + minute)), ["deploy"]);
        let lapsed = take_hold(&mut holds, &take("deploy", "a", 5), 2_000 + 5 * minute);
        assert!(lapsed.yours);
        assert_eq!(lapsed.hold.since, 2_000 + 5 * minute);
    }
}
