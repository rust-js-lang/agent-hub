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
    AGENTS, Agent, BROADCAST, CHANNELS, Channel, EVENTS, EVERYONE, Event, HISTORY_LIMIT,
    HOLD_MINUTES_MAX, HOLDS, HUB, Hold, HubInfo, INBOX, Inbox, Join, LEAVE, MESSAGES, Message,
    PERSON, READ, RELEASE, RETIRE, ReadInbox, ReadMessage, Refusal, Register, Release, Retire,
    Retired, STOP, Send, SignIn, Take, Taken, check_channel, check_hold, check_name,
};
use serde::Deserialize;
use tokio::sync::{broadcast, watch};
use tokio_stream::{Stream, wrappers::BroadcastStream};
use tower_http::services::ServeDir;

use crate::store::Store;

pub struct Hub {
    /// Serializes lifecycle changes with delivery, holds and watch attachment.
    lifecycle: Mutex<()>,
    away_after: u64,
    forget_after: u64,
    reconnect_grace: u64,
    store: Mutex<Store>,
    events: broadcast::Sender<Event>,
    /// How many `watch`es of each agent are connected.
    watchers: Mutex<HashMap<String, usize>>,
    /// When it started.
    started: u64,
    /// Whether it was asked to stop, for another to take its place.
    stop: watch::Sender<bool>,
}

/// The commit this build is of, as `build.rs` found it.
pub const BUILD: &str = env!("CROSS_AGENT_BUILD");

pub use crate::holds::{held, release_hold, take_hold};

/// Milliseconds since the Unix epoch.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_millis() as u64
}

/// Why a request was refused, and its status.
#[derive(Debug)]
pub struct Refused(StatusCode, String);

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        (self.0, Json(Refusal { reason: self.1 })).into_response()
    }
}

impl From<rusqlite::Error> for Refused {
    fn from(error: rusqlite::Error) -> Refused {
        eprintln!("hub SQLite operation failed: {error}");
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
    /// A hub over `store`, holding what it kept held.
    pub fn new(store: Store) -> rusqlite::Result<Hub> {
        Self::with_timeouts(
            store,
            protocol::default_away_after(),
            protocol::default_forget_after(),
        )
    }

    pub fn with_timeouts(
        store: Store,
        away_after: u64,
        forget_after: u64,
    ) -> rusqlite::Result<Hub> {
        Ok(Hub {
            lifecycle: Mutex::new(()),
            away_after,
            forget_after,
            reconnect_grace: 30_000,
            store: Mutex::new(store),
            events: broadcast::channel(256).0,
            watchers: Mutex::new(HashMap::new()),
            started: now(),
            stop: watch::channel(false).0,
        })
    }

    pub fn with_reconnect_grace(mut self, milliseconds: u64) -> Self {
        self.reconnect_grace = milliseconds;
        self
    }

    /// What's running.
    pub fn info(&self) -> HubInfo {
        HubInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            build: BUILD.to_string(),
            started: self.started,
            away_after: self.away_after,
            forget_after: self.forget_after,
            features: vec!["inbox-digests".to_string(), "agent-lifecycle".to_string()],
        }
    }

    /// Done once the hub is asked to stop (`STOP`).
    pub fn stopped(&self) -> impl Future<Output = ()> + use<> {
        let mut stop = self.stop.subscribe();
        async move {
            let _ = stop.wait_for(|stopped| *stopped).await;
        }
    }

    fn lifecycle(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn watching(&self, name: &str) -> bool {
        self.watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .is_some_and(|&count| count > 0)
    }

    fn away_at(&self, agent: &Agent, at: u64) -> bool {
        !agent.person
            && !self.watching(&agent.name)
            && at.saturating_sub(self.started) >= self.reconnect_grace
            && at.saturating_sub(agent.last_seen) >= self.away_after
    }

    /// The agent named `name`, or a refusal saying how to find one.
    fn agent(&self, name: &str) -> Result<Agent, Refused> {
        match self.store().agent(name)? {
            Some(agent) => {
                if let Some(retired_at) = agent.retired_at {
                    return Err(Refused(
                        StatusCode::GONE,
                        format!("{name} retired at {retired_at}"),
                    ));
                }
                let watching = self.watching(name);
                Ok(Agent {
                    away: self.away_at(&agent, now()),
                    watching,
                    ..agent
                })
            }
            None => Err(Refused(
                StatusCode::NOT_FOUND,
                format!("no agent is named {name}: `cross-agent-message agents` lists them"),
            )),
        }
    }

    /// Called while lifecycle is locked; all durable state commits together.
    fn retire(&self, name: &str, at: u64) -> Result<Retired, Refused> {
        let retired = self.store().retire(name, at)?.ok_or_else(|| {
            Refused(
                StatusCode::NOT_FOUND,
                format!("no listed agent is named {name}"),
            )
        })?;
        self.tell(Event::Retired {
            name: name.to_string(),
        });
        if let Some(mut agent) = self.store().agent(name)? {
            agent.watching = false;
            self.tell(Event::Agent { agent });
        }
        Ok(retired)
    }

    fn forget(&self, at: u64) -> Result<(), Refused> {
        if at < self.forget_after || at.saturating_sub(self.started) < self.reconnect_grace {
            return Ok(());
        }
        let names = self.store().forgettable(at - self.forget_after)?;
        for name in names {
            if !self.watching(&name) {
                self.retire(&name, at)?;
            }
        }
        Ok(())
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
        .route(READ, post(read_message))
        .route(RETIRE, post(retire))
        .route(protocol::NOTICE, get(notice))
        .route(HOLDS, get(holds).post(take))
        .route(RELEASE, post(release))
        .route(CHANNELS, get(channels).post(join))
        .route(LEAVE, post(leave))
        .route(BROADCAST, post(broadcast))
        .route(PERSON, post(sign_in))
        .route(HUB, get(info))
        .route(STOP, post(stop))
        .route(EVENTS, get(events))
        .fallback_service(ServeDir::new(dashboard))
        .with_state(hub)
}

async fn agents(State(hub): State<Arc<Hub>>) -> Result<Json<Vec<Agent>>, Refused> {
    let _lifecycle = hub.lifecycle();
    hub.forget(now())?;
    let agents = hub.store().agents()?;
    Ok(Json(
        agents
            .into_iter()
            .map(|agent| {
                let watching = hub.watching(&agent.name);
                Agent {
                    away: hub.away_at(&agent, now()),
                    watching,
                    ..agent
                }
            })
            .collect(),
    ))
}

async fn register(
    State(hub): State<Arc<Hub>>,
    Json(agent): Json<Register>,
) -> Result<Json<Agent>, Refused> {
    let _lifecycle = hub.lifecycle();
    check_name(&agent.name).map_err(bad_request)?;
    // The person's name is theirs: no agent's command takes it.
    if hub
        .store()
        .agent(&agent.name)?
        .is_some_and(|known| known.person)
    {
        return Err(Refused(
            StatusCode::CONFLICT,
            format!(
                "{} is the person's name on this hub, who signs in from the dashboard: pick another",
                agent.name
            ),
        ));
    }
    if agent.reconnect
        && hub
            .store()
            .agent(&agent.name)?
            .is_some_and(|known| known.retired_at.is_some())
    {
        return Err(Refused(
            StatusCode::GONE,
            format!("{} was retired. This watch has ended.", agent.name),
        ));
    }
    hub.store().see(&agent, now())?;
    let agent = hub.agent(&agent.name)?;
    hub.tell(Event::Agent {
        agent: agent.clone(),
    });
    Ok(Json(agent))
}

/// Whether `key` is the hub's, which the person signs in and sends with:
/// by its digits alone, as a phone's number keys type it, with no dash.
fn check_key(hub: &Hub, key: &str) -> Result<(), Refused> {
    let digits = |key: &str| -> String { key.chars().filter(char::is_ascii_digit).collect() };
    let given = digits(key);
    if !given.is_empty() && given == digits(&hub.store().person_key()?) {
        return Ok(());
    }
    Err(Refused(
        StatusCode::FORBIDDEN,
        "that isn't this hub's key: `cross-agent-message key`, on the hub's machine, prints it"
            .to_string(),
    ))
}

/// The person signs in from the dashboard, under the name they'll write
/// as: theirs from then on, where no agent has it.
async fn sign_in(
    State(hub): State<Arc<Hub>>,
    Json(sign_in): Json<SignIn>,
) -> Result<Json<Agent>, Refused> {
    let _lifecycle = hub.lifecycle();
    check_name(&sign_in.name).map_err(bad_request)?;
    check_key(&hub, &sign_in.key)?;
    if hub
        .store()
        .agent(&sign_in.name)?
        .is_some_and(|known| !known.person)
    {
        return Err(Refused(
            StatusCode::CONFLICT,
            format!(
                "{} is an agent's name on this hub: pick another",
                sign_in.name
            ),
        ));
    }
    hub.store().see_person(&sign_in.name, now())?;
    let agent = hub.agent(&sign_in.name)?;
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

/// Whether `send` is the person's: it comes with the hub's key, from the
/// name they signed in under. And nothing else is sent as them.
fn sent_by_person(hub: &Hub, send: &Send) -> Result<bool, Refused> {
    let from = hub.agent(&send.from)?;
    match &send.key {
        Some(key) => {
            check_key(hub, key)?;
            if !from.person {
                return Err(bad_request(format!(
                    "{} isn't the person's name: sign in from the dashboard first",
                    send.from
                )));
            }
            Ok(true)
        }
        None if from.person => Err(Refused(
            StatusCode::FORBIDDEN,
            format!(
                "{} is the person's name on this hub: only the dashboard, with the hub's key, sends as them",
                send.from
            ),
        )),
        None => Ok(false),
    }
}

async fn send(
    State(hub): State<Arc<Hub>>,
    Json(send): Json<Send>,
) -> Result<Json<Message>, Refused> {
    let _lifecycle = hub.lifecycle();
    let person = sent_by_person(&hub, &send)?;
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
    let message = hub.store().send(&send, now(), person)?;
    hub.tell(Event::Sent {
        message: message.clone(),
    });
    hub.tell_agent(&send.to)?;
    Ok(Json(message))
}

/// A channel's name as it's stored: without the `#` it's written with.
fn channel_name(written: &str) -> Result<&str, Refused> {
    let name = written.strip_prefix('#').unwrap_or(written);
    check_channel(name).map_err(bad_request)?;
    Ok(name)
}

async fn channels(State(hub): State<Arc<Hub>>) -> Result<Json<Vec<Channel>>, Refused> {
    Ok(Json(hub.store().channels()?))
}

/// The channel `join` names, once its agent is one the hub knows and the
/// channel one that's joined: every agent is in `everyone` already, and
/// the person reads every channel on the dashboard.
fn joinable<'a>(hub: &Hub, join: &'a Join) -> Result<&'a str, Refused> {
    let channel = channel_name(&join.channel)?;
    if hub.agent(&join.name)?.person {
        return Err(bad_request(format!(
            "{} is the person, who reads every channel on the dashboard",
            join.name
        )));
    }
    if channel == EVERYONE {
        return Err(bad_request(format!(
            "every agent is in #{EVERYONE}: it's neither joined nor left"
        )));
    }
    Ok(channel)
}

async fn join(
    State(hub): State<Arc<Hub>>,
    Json(join): Json<Join>,
) -> Result<Json<Channel>, Refused> {
    let _lifecycle = hub.lifecycle();
    let channel = joinable(&hub, &join)?;
    let store = hub.store();
    store.join(channel, &join.name)?;
    Ok(Json(Channel {
        name: channel.to_string(),
        members: store.members(channel)?,
    }))
}

async fn leave(
    State(hub): State<Arc<Hub>>,
    Json(join): Json<Join>,
) -> Result<Json<Channel>, Refused> {
    let _lifecycle = hub.lifecycle();
    let channel = joinable(&hub, &join)?;
    let store = hub.store();
    store.leave(channel, &join.name)?;
    Ok(Json(Channel {
        name: channel.to_string(),
        members: store.members(channel)?,
    }))
}

/// A message to a channel: a copy to each agent in it but its sender.
async fn broadcast(
    State(hub): State<Arc<Hub>>,
    Json(send): Json<Send>,
) -> Result<Json<Vec<Message>>, Refused> {
    let _lifecycle = hub.lifecycle();
    hub.forget(now())?;
    let person = sent_by_person(&hub, &send)?;
    let channel = channel_name(&send.to)?;
    if send.body.trim().is_empty() {
        return Err(bad_request("a message can't be empty".to_string()));
    }
    let messages = {
        let mut store = hub.store();
        let recipients: Vec<String> = (store.members(channel)?.into_iter())
            .filter(|member| *member != send.from)
            .collect();
        if recipients.is_empty() {
            return Err(bad_request(format!(
                "no other agent is in #{channel}: `cross-agent-message join {channel}` puts one in"
            )));
        }
        let at = now();
        let recipients = recipients
            .into_iter()
            .map(|name| {
                let away = !person
                    && !hub.watching(&name)
                    && store
                        .agent(&name)?
                        .is_some_and(|agent| hub.away_at(&agent, at));
                Ok((name, away))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        store.broadcast_with_away(&send, channel, &recipients, at, person)?
    };
    for message in &messages {
        hub.tell(Event::Sent {
            message: message.clone(),
        });
        if !message.away {
            hub.tell_agent(&message.to)?;
        }
    }
    Ok(Json(messages))
}

async fn read_inbox(
    State(hub): State<Arc<Hub>>,
    Json(read): Json<ReadInbox>,
) -> Result<Json<Inbox>, Refused> {
    let _lifecycle = hub.lifecycle();
    hub.agent(&read.name)?;
    if let Some(channel) = &read.channel {
        check_channel(channel).map_err(bad_request)?;
    }
    if let Some(sender) = &read.sender {
        check_name(sender).map_err(bad_request)?;
    }
    if read.skip && read.channel.is_none() && read.sender.is_none() {
        return Err(bad_request("skip needs a channel or sender".to_string()));
    }
    let inbox = hub.store().read_mail(&read, now())?;
    if !inbox.read_ids.is_empty() {
        hub.tell(Event::Read {
            name: read.name.clone(),
            ids: inbox.read_ids.clone(),
        });
        hub.tell_agent(&read.name)?;
    }
    Ok(Json(inbox))
}

async fn read_message(
    State(hub): State<Arc<Hub>>,
    Json(read): Json<ReadMessage>,
) -> Result<Json<Message>, Refused> {
    let _lifecycle = hub.lifecycle();
    hub.agent(&read.name)?;
    let message = hub
        .store()
        .read_message(&read.name, read.id, now())?
        .ok_or_else(|| {
            Refused(
                StatusCode::NOT_FOUND,
                format!("No message {} to you.", read.id),
            )
        })?;
    hub.tell(Event::Read {
        name: read.name.clone(),
        ids: vec![message.id],
    });
    hub.tell_agent(&read.name)?;
    Ok(Json(message))
}

async fn retire(
    State(hub): State<Arc<Hub>>,
    Json(retire): Json<Retire>,
) -> Result<Json<Retired>, Refused> {
    let _lifecycle = hub.lifecycle();
    check_name(&retire.name).map_err(bad_request)?;
    if let Some(key) = &retire.key {
        check_key(&hub, key)?;
    }
    let agent = hub.agent(&retire.name)?;
    if agent.person {
        return Err(bad_request("the person cannot be retired".to_string()));
    }
    Ok(Json(hub.retire(&retire.name, now())?))
}

#[derive(Deserialize)]
struct NoticeQuery {
    name: String,
    #[serde(default)]
    channels: bool,
}

async fn notice(
    State(hub): State<Arc<Hub>>,
    Query(query): Query<NoticeQuery>,
) -> Result<Json<Vec<(String, bool, u32)>>, Refused> {
    let _lifecycle = hub.lifecycle();
    hub.agent(&query.name)?;
    Ok(Json(hub.store().notice(&query.name, query.channels)?))
}

async fn info(State(hub): State<Arc<Hub>>) -> Json<HubInfo> {
    Json(hub.info())
}

/// Stops the hub a moment after it answers, so the answer is sent.
async fn stop(State(hub): State<Arc<Hub>>) -> Json<HubInfo> {
    let stop = hub.stop.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let _ = stop.send(true);
    });
    Json(hub.info())
}

async fn holds(State(hub): State<Arc<Hub>>) -> Result<Json<Vec<Hold>>, Refused> {
    Ok(Json(hub.store().held(now())?))
}

async fn take(State(hub): State<Arc<Hub>>, Json(take): Json<Take>) -> Result<Json<Taken>, Refused> {
    let _lifecycle = hub.lifecycle();
    check_hold(&take.name).map_err(bad_request)?;
    hub.agent(&take.holder)?;
    if take.minutes == 0 || take.minutes > HOLD_MINUTES_MAX {
        return Err(bad_request(format!(
            "a hold is taken for 1 to {HOLD_MINUTES_MAX} minutes, not {}: take it again to keep it longer",
            take.minutes
        )));
    }
    Ok(Json(hub.store().take_hold(&take, now())?))
}

async fn release(
    State(hub): State<Arc<Hub>>,
    Json(release): Json<Release>,
) -> Result<Json<Vec<Hold>>, Refused> {
    let _lifecycle = hub.lifecycle();
    check_hold(&release.name).map_err(bad_request)?;
    hub.agent(&release.holder)?;
    let holds = hub
        .store()
        .release_hold(&release, now())?
        .map_err(|reason| Refused(StatusCode::CONFLICT, reason))?;
    Ok(Json(holds))
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
        Self::start_with(hub, name, Hub::tell_agent)
    }

    fn start_with(
        hub: Arc<Hub>,
        name: String,
        notify: impl FnOnce(&Hub, &str) -> Result<(), Refused>,
    ) -> Result<Watch, Refused> {
        let lifecycle = hub.lifecycle();
        hub.agent(&name)?;
        hub.store().touch(&name, now())?;
        *hub.watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(name.clone())
            .or_default() += 1;
        drop(lifecycle);
        let watch = Watch { hub, name };
        notify(&watch.hub, &watch.name)?;
        Ok(watch)
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let _lifecycle = self.hub.lifecycle();
        if let Some(count) = self
            .hub
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.name)
        {
            *count = count.saturating_sub(1);
        }
        let _ = self.hub.store().touch(&self.name, now());
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
    Ok(Sse::new(AgentEvents {
        events,
        watch,
        ended: false,
    })
    .keep_alive(KeepAlive::default()))
}

struct AgentEvents {
    events: BroadcastStream<Event>,
    watch: Option<Watch>,
    ended: bool,
}

impl Stream for AgentEvents {
    type Item = Result<sse::Event, Infallible>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if self.ended {
            return Poll::Ready(None);
        }
        loop {
            let event = match std::pin::Pin::new(&mut self.events).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => event,
                Poll::Ready(Some(Err(_))) => {
                    // Even when a slow stream misses the retirement event,
                    // its durable tombstone must terminate the old watch.
                    if let Some(watch) = &self.watch {
                        if watch
                            .hub
                            .store()
                            .agent(&watch.name)
                            .ok()
                            .flatten()
                            .is_some_and(|agent| agent.retired_at.is_some())
                        {
                            Event::Retired {
                                name: watch.name.clone(),
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };
            if let Some(watch) = &self.watch {
                match &event {
                    Event::Sent { message } if message.to == watch.name && !message.away => {}
                    Event::Retired { name } if *name == watch.name => self.ended = true,
                    _ => continue,
                }
            }
            let json = serde_json::to_string(&event).expect("an event is JSON");
            return Poll::Ready(Some(Ok(sse::Event::default().data(json))));
        }
    }
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
    #[test]
    fn a_connected_watch_is_never_forgotten_and_away_does_not_release_holds() {
        let store = Store::in_memory().unwrap();
        store
            .see(
                &Register {
                    name: "a".to_string(),
                    machine: "test".to_string(),
                    reconnect: false,
                },
                1,
            )
            .unwrap();
        let hub = Arc::new(Hub::with_timeouts(store, 10, 100).unwrap());
        let watch = Watch::start(hub.clone(), "a".to_string()).unwrap();
        assert!(!hub.agent("a").unwrap().away);
        hub.forget(now() + 100_000).unwrap();
        assert!(hub.agent("a").is_ok());
        let hold = hub
            .store()
            .take_hold(&take("check", "a", 30), now())
            .unwrap();
        drop(watch);
        assert_eq!(hub.store().holds().unwrap(), [hold.hold]);
        hub.retire("a", now()).unwrap();
        assert!(hub.store().holds().unwrap().is_empty());
        assert!(hub.agent("a").is_err());
    }

    #[test]
    fn only_idle_agents_without_urgent_mail_are_forgotten() {
        let store = Store::in_memory().unwrap();
        for name in ["quiet", "direct", "human"] {
            store
                .see(
                    &Register {
                        name: name.to_string(),
                        machine: "test".to_string(),
                        reconnect: false,
                    },
                    1,
                )
                .unwrap();
        }
        store.see_person("tu", 1).unwrap();
        store
            .send(
                &Send {
                    from: "tu".to_string(),
                    to: "direct".to_string(),
                    body: "ask".to_string(),
                    key: None,
                },
                2,
                true,
            )
            .unwrap();
        let mut store = store;
        store
            .broadcast(
                &Send {
                    from: "tu".to_string(),
                    to: "team".to_string(),
                    body: "ask".to_string(),
                    key: None,
                },
                "team",
                &["human".to_string()],
                2,
                true,
            )
            .unwrap();
        let mut hub = Hub::with_timeouts(store, 10, 100).unwrap();
        hub.started = 0;
        assert!(hub.agent("quiet").unwrap().away);
        assert!(!hub.agent("tu").unwrap().away);
        hub.forget(100_000).unwrap();
        assert!(hub.agent("quiet").is_err());
        assert!(hub.agent("direct").is_ok());
        assert!(hub.agent("human").is_ok());
        assert!(hub.agent("tu").is_ok());
    }
    #[test]
    fn a_failed_watch_notification_does_not_leak_its_watcher_count() {
        let store = Store::in_memory().unwrap();
        store
            .see(
                &Register {
                    name: "a".to_string(),
                    machine: "test".to_string(),
                    reconnect: false,
                },
                1,
            )
            .unwrap();
        let hub = Arc::new(Hub::new(store).unwrap());
        let result = Watch::start_with(hub.clone(), "a".to_string(), |_, _| {
            Err(Refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "injected failure".to_string(),
            ))
        });
        assert!(result.is_err());
        assert!(!hub.watching("a"));
    }

    #[test]
    fn restart_grace_protects_idle_agents_until_watches_can_reconnect() {
        let store = Store::in_memory().unwrap();
        store
            .see(
                &Register {
                    name: "a".to_string(),
                    machine: "test".to_string(),
                    reconnect: false,
                },
                1,
            )
            .unwrap();
        let mut hub = Hub::with_timeouts(store, 10, 100).unwrap();
        hub.started = 1_000_000;
        let agent = hub.store().agent("a").unwrap().unwrap();
        assert!(!hub.away_at(&agent, 1_029_999));
        hub.forget(1_029_999).unwrap();
        assert!(
            hub.store()
                .agent("a")
                .unwrap()
                .unwrap()
                .retired_at
                .is_none()
        );
        assert!(hub.away_at(&agent, 1_030_000));
        hub.forget(1_030_000).unwrap();
        assert!(
            hub.store()
                .agent("a")
                .unwrap()
                .unwrap()
                .retired_at
                .is_some()
        );
    }

    #[test]
    fn poisoned_state_locks_are_reusable_after_unwinding() {
        let hub = Hub::new(Store::in_memory().unwrap()).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _lifecycle = hub.lifecycle();
            let _store = hub.store();
            panic!("injected panic while holding state locks");
        }));
        assert!(result.is_err());
        let _lifecycle = hub.lifecycle();
        assert!(hub.store().agents().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_lagged_stream_still_ends_when_its_agent_was_retired() {
        use tokio_stream::StreamExt;
        let store = Store::in_memory().unwrap();
        store
            .see(
                &Register {
                    name: "a".to_string(),
                    machine: "test".to_string(),
                    reconnect: false,
                },
                1,
            )
            .unwrap();
        let hub = Arc::new(Hub::new(store).unwrap());
        let events = BroadcastStream::new(hub.events.subscribe());
        let watch = Watch::start(hub.clone(), "a".to_string()).unwrap();
        let mut stream = AgentEvents {
            events,
            watch: Some(watch),
            ended: false,
        };
        hub.retire("a", now()).unwrap();
        let agent = hub.store().agent("a").unwrap().unwrap();
        for _ in 0..300 {
            hub.tell(Event::Agent {
                agent: agent.clone(),
            });
        }
        assert!(stream.next().await.is_some());
        assert!(stream.ended);
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(!hub.watching("a"));
    }
}
