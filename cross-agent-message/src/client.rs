//! A hub's API, from the command line.

use std::io::{BufRead, BufReader};

use anyhow::{Context, Result, bail};
use protocol::endpoints::{self as api, Download, Get, Post, Query, Request, Retry, Route, Stream};
use protocol::{
    Agent, Channel, Event, Hold, HubInfo, Inbox, Join, Message, ReadInbox, ReadMessage, Register,
    Release, Retire, Retired, Send, SignIn, Take, Taken,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use ureq::Body;
use ureq::http::Response;

pub struct Client {
    hub: String,
    http: ureq::Agent,
    metadata: std::sync::OnceLock<HubInfo>,
}

impl Client {
    /// A client of the hub at `hub`, such as `http://127.0.0.1:7878`.
    pub fn new(hub: &str) -> Client {
        let http = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_idle_connections(0)
            .timeout_global(Some(std::time::Duration::from_secs(15)))
            .build()
            .into();
        Client {
            hub: hub.trim_end_matches('/').to_string(),
            http,
            metadata: std::sync::OnceLock::new(),
        }
    }

    /// The hub's address.
    pub fn hub(&self) -> &str {
        &self.hub
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.hub)
    }

    fn unreachable(&self) -> String {
        format!(
            "can't reach the hub at {}: start it with `cross-agent-message serve`",
            self.hub
        )
    }

    /// The JSON of a 200 answer, or the hub's reason for refusing.
    fn answer<T: DeserializeOwned>(mut response: Response<Body>) -> Result<T> {
        Self::response_protocol(&response)?;
        let status = response.status().as_u16();
        let text = response.body_mut().read_to_string()?;
        api::read(status, &text).map_err(anyhow::Error::new)
    }

    fn response_protocol(response: &Response<Body>) -> Result<()> {
        if let Some(header) = response.headers().get(protocol::PROTOCOL_HEADER)
            && header
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                != Some(protocol::PROTOCOL)
        {
            bail!("hub protocol is unsupported; upgrade the hub or client before sending commands");
        }
        Ok(())
    }

    fn requirements(&self, route: &Route, request: &impl Request) -> Result<()> {
        if route.needs.is_none() && request.needs().is_empty() {
            return Ok(());
        }
        let info = self.check_protocol()?;
        let missing = api::missing(route, request, &info.features);
        if !missing.is_empty() {
            let names = missing
                .iter()
                .map(|feature| feature.name())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "This hub ({}) lacks {names}: upgrade it on its machine.",
                info.version
            );
        }
        Ok(())
    }

    /// ```compile_fail
    /// use cross_agent_message::client::Client;
    /// use protocol::endpoints::SEND;
    /// Client::new("http://localhost").get(&SEND, &());
    /// ```
    pub fn get<Q: Query + Request, Res: DeserializeOwned>(
        &self,
        endpoint: &Get<Q, Res>,
        query: &Q,
    ) -> Result<Res> {
        self.requirements(&endpoint.route, query)?;
        let response = self
            .http
            .get(self.url(&endpoint.url(query)))
            .header(protocol::PROTOCOL_HEADER, protocol::PROTOCOL.to_string())
            .call()
            .with_context(|| self.unreachable())?;
        Client::answer(response)
    }

    /// ```compile_fail
    /// use cross_agent_message::client::Client;
    /// use protocol::{Register, endpoints::SEND};
    /// Client::new("http://localhost").post(&SEND, &Register {
    ///     name: "a".into(), machine: "m".into(), reconnect: false,
    /// });
    /// ```
    /// ```compile_fail
    /// use cross_agent_message::client::Client;
    /// use protocol::{Agent, Send, endpoints::SEND};
    /// let _: anyhow::Result<Agent> = Client::new("http://localhost").post(&SEND, &Send {
    ///     from: "a".into(), to: "b".into(), body: "hi".into(), key: None,
    /// });
    /// ```
    pub fn post<B: Serialize + Request, Res: DeserializeOwned>(
        &self,
        endpoint: &Post<B, Res>,
        body: &B,
    ) -> Result<Res> {
        self.requirements(&endpoint.route, body)?;
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nonce = format!("{}-{}-{sequence}", std::process::id(), crate::hub::now());
        let value = serde_json::to_value(body)?;
        let safe = match body.retry() {
            Retry::Always => true,
            Retry::Never => false,
            Retry::Feature(feature) => self.has_feature(feature.name())?,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let attempts = if safe { 3 } else { 1 };
        for attempt in 0..attempts {
            let response = self
                .http
                .post(self.url(endpoint.url()))
                .header(protocol::PROTOCOL_HEADER, protocol::PROTOCOL.to_string())
                .header(protocol::WRITE_NONCE_HEADER, &nonce)
                .config()
                .timeout_global(Some(
                    deadline.saturating_duration_since(std::time::Instant::now()),
                ))
                .build()
                .send_json(&value);
            match response {
                Ok(response) => return Client::answer(response),
                Err(error) if attempt + 1 == attempts || std::time::Instant::now() >= deadline => {
                    return Err(error).with_context(|| self.unreachable());
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }
        bail!("request retry budget exhausted")
    }

    pub fn has_feature(&self, feature: &str) -> Result<bool> {
        if self.metadata.get().is_none() {
            let info = self.check_protocol()?;
            let _ = self.metadata.set(info);
        }
        Ok(self
            .metadata
            .get()
            .is_some_and(|info| info.features.iter().any(|f| f == feature)))
    }

    /// `name` is seen now, on this machine.
    pub fn register(&self, name: &str) -> Result<Agent> {
        self.register_connection(name, false)
    }

    pub fn register_connection(&self, name: &str, reconnect: bool) -> Result<Agent> {
        let machine = gethostname::gethostname()
            .to_string_lossy()
            .trim_end_matches(".local")
            .to_string();
        self.post(
            &api::REGISTER,
            &Register {
                name: name.to_string(),
                machine,
                reconnect,
            },
        )
    }

    pub fn agents(&self) -> Result<Vec<Agent>> {
        self.get(&api::AGENTS, &())
    }

    pub fn send(&self, from: &str, to: &str, body: &str) -> Result<Message> {
        self.post(
            &api::SEND,
            &Send {
                from: from.to_string(),
                to: to.to_string(),
                body: body.to_string(),
                key: None,
            },
        )
    }

    /// Every channel, `everyone` first.
    pub fn channels(&self) -> Result<Vec<Channel>> {
        self.get(&api::CHANNELS, &())
    }

    /// `name` joins `channel`: the channel, with it in.
    pub fn join(&self, channel: &str, name: &str) -> Result<Channel> {
        self.post(
            &api::JOIN,
            &Join {
                channel: channel.to_string(),
                name: name.to_string(),
            },
        )
    }

    /// `name` leaves `channel`: the channel as it's left.
    pub fn leave(&self, channel: &str, name: &str) -> Result<Channel> {
        self.post(
            &api::LEAVE,
            &Join {
                channel: channel.to_string(),
                name: name.to_string(),
            },
        )
    }

    /// `from`'s message to every other agent in `channel`: each one's copy.
    /// With the hub's `key`, the person's, as the dashboard sends it.
    pub fn broadcast(
        &self,
        from: &str,
        channel: &str,
        body: &str,
        key: Option<&str>,
    ) -> Result<Vec<Message>> {
        self.post(
            &api::BROADCAST,
            &Send {
                from: from.to_string(),
                to: channel.to_string(),
                body: body.to_string(),
                key: key.map(str::to_string),
            },
        )
    }

    /// The person signs in as `name` with the hub's `key`, as the
    /// dashboard does: their agent.
    pub fn sign_in(&self, name: &str, key: &str) -> Result<Agent> {
        self.post(
            &api::PERSON,
            &SignIn {
                name: name.to_string(),
                key: key.to_string(),
            },
        )
    }

    /// The person's message, sent with the hub's `key`, as the dashboard
    /// sends it.
    pub fn send_as_person(&self, from: &str, to: &str, body: &str, key: &str) -> Result<Message> {
        self.post(
            &api::SEND,
            &Send {
                from: from.to_string(),
                to: to.to_string(),
                body: body.to_string(),
                key: Some(key.to_string()),
            },
        )
    }

    pub fn read_inbox(&self, name: &str, max: u32) -> Result<Inbox> {
        self.post(
            &api::INBOX,
            &ReadInbox {
                name: name.to_string(),
                max,
                channel: None,
                sender: None,
                skip: false,
                digest: false,

                defer_ack: false,

                up_to: None,
            },
        )
    }

    pub fn read_mail(&self, read: &ReadInbox) -> Result<Inbox> {
        self.post(&api::INBOX, read)
    }

    pub fn read_message(&self, name: &str, id: u64) -> Result<Message> {
        self.post(
            &api::READ,
            &ReadMessage {
                name: name.to_string(),
                id,

                defer_ack: false,
            },
        )
    }

    pub fn retire(&self, name: &str) -> Result<Retired> {
        self.post(
            &api::RETIRE,
            &Retire {
                name: name.to_string(),
                key: None,
            },
        )
    }

    pub fn notice(&self, name: &str, channels: bool) -> Result<Vec<(String, bool, u32)>> {
        self.get(
            &api::NOTICE,
            &api::NoticeQuery {
                name: name.to_string(),
                channels,
            },
        )
    }

    pub fn read_message_deferred(&self, name: &str, id: u64, defer_ack: bool) -> Result<Message> {
        self.post(
            &api::READ,
            &ReadMessage {
                name: name.to_string(),
                id,
                defer_ack,
            },
        )
    }
    pub fn acknowledge(&self, name: &str, ids: &[u64]) -> Result<Vec<u64>> {
        self.post(
            &api::ACK,
            &protocol::Acknowledge {
                name: name.to_string(),
                ids: ids.to_vec(),
            },
        )
    }
    pub fn notice_snapshot(&self, name: &str, channels: bool) -> Result<protocol::Notice> {
        self.get(
            &api::NOTICE_SNAPSHOT,
            &api::NoticeSnapshot {
                name: name.to_string(),
                channels,
            },
        )
    }

    /// The hub that's running: its build.
    pub fn info(&self) -> Result<HubInfo> {
        self.get(&api::HUB, &())
    }

    pub fn check_protocol(&self) -> Result<HubInfo> {
        let info = self.info()?;
        if info.protocol != protocol::PROTOCOL {
            bail!(
                "The hub at {} is {} (protocol {}); this command line needs protocol {}. Upgrade the hub on its machine: cross-agent-message upgrade <release>.",
                self.hub,
                info.version,
                info.protocol,
                protocol::PROTOCOL
            );
        }
        let _ = self.metadata.set(info.clone());
        Ok(info)
    }

    pub fn require(&self, feature: &str) -> Result<()> {
        let info = self.check_protocol()?;
        if !info.features.iter().any(|f| f == feature) {
            bail!(
                "This hub ({}) has no {feature} yet: upgrade it on its machine.",
                info.version
            );
        }
        Ok(())
    }

    pub fn download_binary(&self) -> Result<Vec<u8>> {
        self.download(&api::BINARY, &())
    }

    pub fn download<Q: Query + Request>(
        &self,
        endpoint: &Download<Q, Vec<u8>>,
        query: &Q,
    ) -> Result<Vec<u8>> {
        self.requirements(&endpoint.route, query)?;
        let mut response = self
            .http
            .get(self.url(&endpoint.url(query)))
            .header(protocol::PROTOCOL_HEADER, protocol::PROTOCOL.to_string())
            .call()?;
        Self::response_protocol(&response)?;
        if response.status().as_u16() != 200 {
            Client::answer::<()>(response)?;
            bail!("the hub refused the download without an error reason");
        }
        Ok(response
            .body_mut()
            .with_config()
            .limit(128 * 1024 * 1024)
            .read_to_vec()?)
    }

    /// Asks the hub to stop, for another to take its place: what it was.
    pub fn stop(&self) -> Result<HubInfo> {
        self.post(&api::STOP, &())
    }

    /// What's held now, by name.
    pub fn holds(&self) -> Result<Vec<Hold>> {
        self.get(&api::HOLDS, &())
    }

    /// `holder` asks for the hold `name`, for `minutes`: the hold as it is
    /// now, and whether it's `holder`'s.
    pub fn take(&self, name: &str, holder: &str, minutes: u32) -> Result<Taken> {
        self.post(
            &api::TAKE,
            &Take {
                name: name.to_string(),
                holder: holder.to_string(),
                minutes,
            },
        )
    }

    /// `holder` lets go of the hold `name`: what's still held.
    pub fn release(&self, name: &str, holder: &str) -> Result<Vec<Hold>> {
        self.post(
            &api::RELEASE,
            &Release {
                name: name.to_string(),
                holder: holder.to_string(),
            },
        )
    }

    /// The messages sent to `name` from now on, as they arrive. `name` is
    /// watching until the iterator is dropped.
    pub fn watch(&self, name: &str) -> Result<impl Iterator<Item = Result<Event>>> {
        self.stream(
            &api::EVENTS,
            &api::Events {
                agent: Some(name.to_string()),
                protocol: Some(protocol::PROTOCOL),
                types: Some(
                    protocol::EVENT_TYPES
                        .split(',')
                        .map(str::to_string)
                        .collect(),
                ),
            },
        )
    }

    pub fn stream<Q: Query + Request>(
        &self,
        endpoint: &Stream<Q, Event>,
        query: &Q,
    ) -> Result<impl Iterator<Item = Result<Event>> + use<Q>> {
        self.requirements(&endpoint.route, query)?;
        let http: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_idle_connections(0)
            .build()
            .into();
        let response = http
            .get(self.url(&endpoint.url(query)))
            .call()
            .with_context(|| self.unreachable())?;
        Self::response_protocol(&response)?;
        if response.status().as_u16() != 200 {
            Client::answer::<()>(response)?;
            bail!("the hub refused the watch without an error reason");
        }
        Ok(EventStream {
            reader: BufReader::new(response.into_body().into_reader()),
            finished: false,
        })
    }
}

/// SSE framing belongs to the transport; only assembled data reaches the
/// shared event decoder. An incomplete frame at EOF is not dispatched.
struct EventStream<R> {
    reader: R,
    finished: bool,
}

impl<R: BufRead> Iterator for EventStream<R> {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut data = String::new();
        let mut has_data = false;
        loop {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => {
                    self.finished = true;
                    return None;
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error).context("the hub closed the stream"));
                }
                Ok(_) => {}
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !has_data {
                    continue;
                }
                match api::event(&data) {
                    Ok(Some(event)) => return Some(Ok(event)),
                    Err(error) => {
                        return Some(Err(error).context("an event this version doesn't read"));
                    }
                    Ok(None) => {
                        data.clear();
                        has_data = false;
                    }
                }
            } else if let Some(value) = line.strip_prefix("data:") {
                if has_data {
                    data.push('\n');
                }
                data.push_str(value.strip_prefix(' ').unwrap_or(value));
                has_data = true;
            } else if line == "data" {
                if has_data {
                    data.push('\n');
                }
                has_data = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn ambiguous_writes_retry_only_with_nonce_support_and_keep_identical_nonce_and_body() {
        for protected in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let client = Client::new(&format!("http://{}", listener.local_addr().unwrap()));
            let mut info: HubInfo =
                serde_json::from_str(r#"{"version":"test","build":"test","started":1}"#).unwrap();
            if protected {
                info.features.push("write-nonce".to_string());
            }
            client.metadata.set(info).unwrap();
            let server = std::thread::spawn(move || {
                let attempts = if protected { 2 } else { 1 };
                let mut requests = Vec::new();
                for attempt in 0..attempts {
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                assert!(
                                    std::time::Instant::now() < deadline,
                                    "client never made expected attempt"
                                );
                                std::thread::sleep(std::time::Duration::from_millis(5));
                            }
                            Err(error) => panic!("{error}"),
                        }
                    };
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                        .unwrap();
                    let mut reader = BufReader::new(&mut stream);
                    let mut headers = String::new();
                    loop {
                        let mut line = String::new();
                        assert!(reader.read_line(&mut line).unwrap() > 0);
                        if line == "\r\n" {
                            break;
                        }
                        headers.push_str(&line);
                    }
                    assert!(headers.starts_with("POST /api/messages HTTP/1.1"));
                    let header = |name: &str| {
                        headers
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(key, _)| key.eq_ignore_ascii_case(name))
                            .map(|(_, value)| value.trim().to_string())
                    };
                    let nonce = header(protocol::WRITE_NONCE_HEADER).unwrap();
                    let length: usize = header("content-length")
                        .expect("fixture expects a buffered JSON body")
                        .parse()
                        .unwrap();
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).unwrap();
                    requests.push((nonce, body));
                    if attempt == 1 {
                        let answer = r#"{"id":1,"from":"a","to":"b","body":"once","sentAt":1,"readAt":null,"person":false,"channel":null}"#;
                        write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}", answer.len()).unwrap();
                    }
                    // First attempt deliberately closes after accepting the
                    // complete write but before sending any response headers.
                }
                requests
            });
            let result = client.send("a", "b", "once");
            let requests = server.join().unwrap();
            if protected {
                assert_eq!(result.unwrap().body, "once");
                assert_eq!(requests.len(), 2);
                assert!(!requests[0].0.is_empty());
                assert_eq!(requests[0], requests[1]);
            } else {
                assert!(result.is_err());
                assert_eq!(
                    requests.len(),
                    1,
                    "an old hub must not get an ambiguous write twice"
                );
            }
        }
    }

    #[test]
    fn sse_frames_join_data_lines_ignore_comments_and_survive_unknown_or_broken_events() {
        let bytes = b": keep alive\r\nid: 9\r\ndata: {\r\ndata: \"type\":\"restarting\"}\r\n\r\n\
            data: {\"type\":\"future\"}\n\n\
            data: {\"type\":\"sent\"}\n\n\
            data: {\"type\":\"retired\",\"name\":\"a\"}\n\n\
            data: {\"type\":\"restarting\"}";
        let mut events = EventStream {
            reader: &bytes[..],
            finished: false,
        };
        assert_eq!(events.next().unwrap().unwrap(), Event::Restarting);
        let error = events.next().unwrap().unwrap_err();
        assert!(error.downcast_ref::<serde_json::Error>().is_some());
        assert_eq!(
            events.next().unwrap().unwrap(),
            Event::Retired {
                name: "a".to_string()
            }
        );
        assert!(
            events.next().is_none(),
            "EOF does not dispatch an incomplete SSE frame"
        );
        assert!(events.next().is_none());
    }

    #[test]
    fn sse_empty_data_is_a_decode_error_and_non_data_fields_are_not_payload() {
        let bytes = b"event: restarting\nretry: 2000\n\ndata\n\ndata:{\"type\":\"restarting\"}\n\n";
        let mut events = EventStream {
            reader: &bytes[..],
            finished: false,
        };
        assert!(events.next().unwrap().is_err());
        assert_eq!(events.next().unwrap().unwrap(), Event::Restarting);
        assert!(events.next().is_none());
    }
}
