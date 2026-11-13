//! Worker descriptor ownership and parked lifecycle, independent of a server framework.
use anyhow::{Result, bail};
use std::{
    collections::BTreeMap,
    os::fd::{FromRawFd, RawFd},
    time::Duration,
};

pub struct Inherited {
    listeners: Vec<std::net::TcpListener>,
    pub channel: crate::handoff::Channel,
}
impl Inherited {
    /// Take exclusive ownership of the inherited listener and lifecycle FDs.
    ///
    /// # Safety
    /// The caller must be the freshly exec'd worker. FDs 3..ready_fd must be
    /// unique TCP listeners and ready_fd a unique Unix stream, all transferred
    /// by this library's supervisor and not owned by any existing Rust value.
    pub unsafe fn take(count: usize, ready_fd: RawFd, expected_count: usize) -> Result<Self> {
        if count == 0 || count > 64 || count != expected_count {
            bail!("invalid inherited listener count");
        }
        if ready_fd != 3 + count as RawFd {
            bail!("invalid readiness descriptor");
        }
        let mut listeners = Vec::new();
        for fd in 3..ready_fd {
            // SAFETY: the supervisor hands exactly these unique socket descriptors
            // to this worker. fcntl verifies they are open before ownership transfers.
            if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
                bail!("listener descriptor is not open");
            }
            close_on_exec(fd)?;
            let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
            listener.set_nonblocking(true)?;
            // Keep the inherited descriptors outside the serving runtime. During
            // quiescence the process still owns them but no task accepts on them.
            listeners.push(listener);
        }
        if unsafe { libc::fcntl(ready_fd, libc::F_GETFD) } < 0 {
            bail!("readiness descriptor is not open");
        }
        close_on_exec(ready_fd)?;
        // SAFETY: this unique endpoint is owned solely by this worker after exec.
        let ready = unsafe { std::os::unix::net::UnixStream::from_raw_fd(ready_fd) };
        let ready = crate::handoff::Channel::new(ready)?;
        Ok(Self {
            listeners,
            channel: ready,
        })
    }
}

/// Negotiated parent capabilities and application identity for each generation.
#[derive(Debug, Clone)]
pub struct Options {
    build: String,
    cooperative: bool,
    startup: bool,
}
impl Options {
    pub fn new(build: String, parent_protocol: u32, parent_startup: bool) -> Self {
        let cooperative = parent_protocol == crate::handoff::VERSION;
        Self {
            build,
            cooperative,
            startup: cooperative && parent_startup,
        }
    }
}

/// Run one serving generation at a time. Returning quiesced promises every
/// exclusive resource and every accepting task has been released on all threads.
pub fn run(
    inherited: Inherited,
    options: Options,
    mut serve: impl FnMut(&mut Generation<'_>) -> Result<Outcome>,
) -> Result<()> {
    run_checked(inherited, options, &mut serve, |_| Ok(()))
}

/// Conformance hook runs after the generation releases application state and
/// before Drained; production checks must fit inside the admission budget.
pub fn run_checked(
    inherited: Inherited,
    options: Options,
    mut serve: impl FnMut(&mut Generation<'_>) -> Result<Outcome>,
    mut released: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    let Inherited {
        listeners,
        channel: mut ready,
    } = inherited;
    let signals = crate::supervisor::ArcSignal::new()?;
    let mut request = 0;
    loop {
        let outcome = {
            let mut generation =
                Generation::new(&listeners, &mut ready, request, &options, &signals);
            serve(&mut generation)?
        };
        let Outcome::Quiesced(Quiesced {
            id,
            stopped_accepting_ns,
            first_accepted_ns,
        }) = outcome
        else {
            return Ok(());
        };
        // The generation must release all exclusive resources, including
        // owners on other threads, before this acknowledgement is sent.
        released(std::process::id())?;
        ready.send(
            &crate::handoff::Reply::Drained {
                id,
                stopped_accepting_ns: stopped_accepting_ns.clone(),
                first_accepted_ns: first_accepted_ns.clone(),
            },
            std::time::Instant::now() + Duration::from_secs(1),
        )?;
        eprintln!(
            "Worker {} drained and released application state; awaiting resume or retire.",
            std::process::id()
        );
        let deadline = std::time::Instant::now() + crate::handoff::PARK_LIMIT;
        loop {
            if signals.stopped() {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                bail!(
                    "parked worker received no resume or retire within {} seconds",
                    crate::handoff::PARK_LIMIT.as_secs()
                );
            }
            if let Some(line) = ready.poll()? {
                match serde_json::from_str::<crate::handoff::Command>(&line)? {
                    crate::handoff::Command::Resume { id } => {
                        request = id;
                        break;
                    }
                    crate::handoff::Command::Retire { .. } => return Ok(()),
                    crate::handoff::Command::Quiesce { id } => ready.send(
                        &crate::handoff::Reply::Drained {
                            id,
                            stopped_accepting_ns: stopped_accepting_ns.clone(),
                            first_accepted_ns: first_accepted_ns.clone(),
                        },
                        deadline,
                    )?,
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn close_on_exec(fd: RawFd) -> Result<()> {
    // SAFETY: fcntl only updates flags on an open descriptor owned by the worker.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Lifecycle decisions returned to the application after internal protocol work.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Quiesce { id: u64 },
    Stop,
}

pub enum Outcome {
    Stopped,
    Quiesced(Quiesced),
}
/// Only Generation can construct proof of listener quiescence.
///
/// ```compile_fail
/// use process_supervisor::worker::Quiesced;
/// let proof = Quiesced { id: 1, stopped_accepting_ns: Default::default(), first_accepted_ns: Default::default() };
/// ```
pub struct Quiesced {
    id: u64,
    stopped_accepting_ns: BTreeMap<String, u64>,
    first_accepted_ns: BTreeMap<String, u64>,
}

/// One serving generation. Owns protocol framing, negotiation, request IDs,
/// listener instrumentation and readiness snapshots. Applications only supply
/// service readiness and obey Quiesce/Stop after finishing their own requests.
pub struct Generation<'a> {
    raw: &'a [std::net::TcpListener],
    channel: &'a mut crate::handoff::Channel,
    request: u64,
    options: &'a Options,
    signals: &'a crate::supervisor::ArcSignal,
    timing: Vec<std::sync::Arc<crate::listener::Timing>>,
    listeners_taken: bool,
    accepting: bool,
    addresses: Vec<crate::addresses::ListenAddress>,
    accepted_tx: tokio::sync::mpsc::UnboundedSender<crate::handoff::Acceptance>,
    accepted_rx: tokio::sync::mpsc::UnboundedReceiver<crate::handoff::Acceptance>,
    commands: Option<crate::handoff::Commands>,
    synchronous: bool,
    quiesce: Option<u64>,
}
impl<'a> Generation<'a> {
    fn new(
        raw: &'a [std::net::TcpListener],
        channel: &'a mut crate::handoff::Channel,
        request: u64,
        options: &'a Options,
        signals: &'a crate::supervisor::ArcSignal,
    ) -> Self {
        let (accepted_tx, accepted_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            raw,
            channel,
            request,
            options,
            signals,
            timing: Vec::new(),
            listeners_taken: false,
            accepting: false,
            addresses: Vec::new(),
            accepted_tx,
            accepted_rx,
            commands: None,
            synchronous: false,
            quiesce: None,
        }
    }
    /// Create all tracked acceptors inside the application's Tokio runtime,
    /// completing fallible setup before announcing that this generation accepts.
    pub fn listeners(&mut self) -> Result<Vec<crate::listener::Listener>> {
        if self.listeners_taken {
            bail!("generation listeners already taken");
        }
        let listeners = self
            .raw
            .iter()
            .map(|socket| {
                crate::listener::Listener::new(socket, self.request, self.accepted_tx.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        self.timing = listeners
            .iter()
            .map(crate::listener::Listener::timing)
            .collect();
        self.listeners_taken = true;
        Ok(listeners)
    }
    pub fn accepting(&mut self) -> Result<()> {
        if !self.listeners_taken || self.accepting {
            bail!("generation accepting marker out of order");
        }
        if self.options.startup {
            self.channel.write(
                &format!("accepting {}\n", self.request),
                std::time::Instant::now() + Duration::from_secs(1),
            )?;
        }
        self.accepting = true;
        Ok(())
    }
    fn first_accepted(&self) -> BTreeMap<String, u64> {
        let mut first = BTreeMap::new();
        for timing in &self.timing {
            let at = timing.first_accepted_ns();
            if at != 0 {
                first
                    .entry(timing.address.clone())
                    .and_modify(|old: &mut u64| *old = (*old).min(at))
                    .or_insert(at);
            }
        }
        first
    }
    fn send_ready(&mut self, request: u64) -> Result<()> {
        let first_accepted_ns = self.first_accepted();
        let ready = crate::addresses::Ready {
            build: self.options.build.clone(),
            addresses: self.addresses.clone(),
            handoff_protocol: if self.options.cooperative {
                crate::handoff::VERSION
            } else {
                0
            },
            request,
            accepted: first_accepted_ns.keys().cloned().collect(),
            first_accepted_ns,
        };
        self.channel.write(
            &format!("ready {}\n", serde_json::to_string(&ready)?),
            std::time::Instant::now() + Duration::from_secs(1),
        )
    }
    pub fn ready(&mut self, addresses: Vec<crate::addresses::ListenAddress>) -> Result<()> {
        if !self.accepting {
            bail!("readiness before generation accepts");
        }
        self.addresses = addresses;
        self.send_ready(self.request)
    }
    fn accepted(&mut self, accepted: crate::handoff::Acceptance) -> Result<()> {
        if self.options.startup {
            self.channel.write(
                &format!("accepted {}\n", serde_json::to_string(&accepted)?),
                std::time::Instant::now() + Duration::from_secs(1),
            )?;
        }
        Ok(())
    }
    fn command(&mut self, command: crate::handoff::Command) -> Result<Option<Event>> {
        match command {
            crate::handoff::Command::Quiesce { id } => {
                self.quiesce = Some(id);
                Ok(Some(Event::Quiesce { id }))
            }
            crate::handoff::Command::Retire { .. } => Ok(Some(Event::Stop)),
            crate::handoff::Command::Resume { id } => {
                self.send_ready(id)?;
                Ok(None)
            }
        }
    }
    /// Cancellation-safe asynchronous wait. Internally handles acceptance
    /// notifications and Resume replies; lifecycle EOF asks the service to stop.
    pub async fn next_event(&mut self) -> Result<Event> {
        self.next_event_observing(|| None).await
    }

    /// Refresh application observations for internal Resume readiness replies.
    pub async fn next_event_observing(
        &mut self,
        mut observed: impl FnMut() -> Option<Vec<crate::addresses::ListenAddress>>,
    ) -> Result<Event> {
        if self.synchronous {
            bail!("cannot mix synchronous and async command readers");
        }
        if self.options.cooperative && self.commands.is_none() {
            self.commands = Some(crate::handoff::Commands::new(
                tokio::net::UnixStream::from_std(self.channel.stream.try_clone()?)?,
            ));
        }
        loop {
            tokio::select! {
                Some(accepted) = self.accepted_rx.recv() => self.accepted(accepted)?,
                command = async {
                    match self.commands.as_mut() {
                        Some(commands) => commands.receive().await,
                        None => std::future::pending().await,
                    }
                } => match command {
                    Ok(command) => {
                        if matches!(command, crate::handoff::Command::Resume { .. })
                            && let Some(addresses) = observed() { self.addresses = addresses; }
                        if let Some(event) = self.command(command)? { return Ok(event); }
                    },
                    Err(error) if error.downcast_ref::<std::io::Error>().is_some_and(|e| matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe)) => return Ok(Event::Stop),
                    Err(error) => return Err(error),
                },
                _ = tokio::time::sleep(Duration::from_millis(10)) => if self.signals.stopped() { return Ok(Event::Stop); },
            }
        }
    }
    /// Nonblocking synchronous counterpart. Do not mix with next_event.
    pub fn poll_event(&mut self) -> Result<Option<Event>> {
        if self.commands.is_some() {
            bail!("cannot mix async and synchronous command readers");
        }
        self.synchronous = true;
        while let Ok(accepted) = self.accepted_rx.try_recv() {
            self.accepted(accepted)?;
        }
        if self.signals.stopped() {
            return Ok(Some(Event::Stop));
        }
        if !self.options.cooperative {
            return Ok(None);
        }
        match self.channel.poll() {
            Ok(Some(line)) => self.command(serde_json::from_str(&line)?),
            Ok(None) => Ok(None),
            Err(error) if error.to_string().contains("channel closed") => Ok(Some(Event::Stop)),
            Err(error) => Err(error),
        }
    }
    /// Call only after all accepting tasks and application state owners are
    /// gone. Stamps are gathered from library listeners, never supplied by apps.
    pub fn quiesced(&self, id: u64) -> Result<Outcome> {
        if self.quiesce != Some(id) {
            bail!("quiesce request ID does not match");
        }
        if !self.listeners_taken {
            bail!("generation listeners were not initialized");
        }
        let mut stopped = BTreeMap::new();
        for timing in &self.timing {
            let at = timing.stopped_accepting_ns();
            if at == 0 {
                bail!("listener {} is still accepting", timing.address);
            }
            stopped
                .entry(timing.address.clone())
                .and_modify(|old: &mut u64| *old = (*old).max(at))
                .or_insert(at);
        }
        Ok(Outcome::Quiesced(Quiesced {
            id,
            stopped_accepting_ns: stopped,
            first_accepted_ns: self.first_accepted(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::{Channel, Command, Reply};
    use std::{os::unix::net::UnixStream, time::Instant};

    fn line(channel: &mut Channel) -> String {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(line) = channel.poll().unwrap() {
                return line;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[tokio::test]
    async fn generations_own_negotiation_resume_replies_and_quiescence_proof() {
        for (protocol, startup) in [(0, true), (1, false), (1, true)] {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            socket.set_nonblocking(true).unwrap();
            let address = socket.local_addr().unwrap();
            let raw = [socket];
            let (owned, parent) = UnixStream::pair().unwrap();
            let mut channel = Channel::new(owned).unwrap();
            let mut parent = Channel::new(parent).unwrap();
            let options = Options::new("fixture".into(), protocol, startup);
            let signals = crate::supervisor::ArcSignal::new().unwrap();
            let mut generation = Generation::new(&raw, &mut channel, 0, &options, &signals);
            assert!(generation.ready(Vec::new()).is_err());
            let mut listeners = generation.listeners().unwrap();
            assert!(generation.listeners().is_err());
            generation.accepting().unwrap();
            assert!(generation.accepting().is_err());
            if protocol == 1 && startup {
                assert_eq!(line(&mut parent), "accepting 0\n");
            } else {
                assert!(parent.poll().unwrap().is_none());
            }
            let client = tokio::net::TcpStream::connect(address).await.unwrap();
            let accepted = listeners[0].accept().await.unwrap();
            generation
                .ready(vec![crate::addresses::ListenAddress {
                    address: address.to_string(),
                    state: crate::addresses::AddressState::Ready,
                    reason: None,
                    remote_seen: false,
                }])
                .unwrap();
            let initial = line(&mut parent);
            let ready: crate::addresses::Ready =
                serde_json::from_str(initial.strip_prefix("ready ").unwrap()).unwrap();
            assert_eq!(ready.handoff_protocol, if protocol == 1 { 1 } else { 0 });
            assert_eq!(ready.accepted, vec![address.to_string()]);
            if protocol == 0 {
                drop(client);
                drop(accepted);
                continue;
            }
            parent
                .send(
                    &Command::Resume { id: 17 },
                    Instant::now() + Duration::from_secs(1),
                )
                .unwrap();
            // Resume is answered internally; it is never an application event.
            let latest = vec![crate::addresses::ListenAddress {
                address: address.to_string(),
                state: crate::addresses::AddressState::Blocked,
                reason: Some("fresh observation".into()),
                remote_seen: true,
            }];
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(30),
                    generation.next_event_observing(|| Some(latest.clone()))
                )
                .await
                .is_err()
            );
            let resumed = loop {
                let next = line(&mut parent);
                if next.starts_with("ready ") {
                    break next;
                }
                assert!(startup && next.starts_with("accepted "));
            };
            let resumed: crate::addresses::Ready =
                serde_json::from_str(resumed.strip_prefix("ready ").unwrap()).unwrap();
            assert_eq!(resumed.request, 17);
            assert_eq!(resumed.addresses, latest);
            assert_eq!(resumed.first_accepted_ns, ready.first_accepted_ns);
            parent
                .send(
                    &Command::Quiesce { id: 18 },
                    Instant::now() + Duration::from_secs(1),
                )
                .unwrap();
            assert_eq!(
                generation.next_event().await.unwrap(),
                Event::Quiesce { id: 18 }
            );
            assert!(generation.quiesced(19).is_err());
            assert!(
                generation.quiesced(18).is_err(),
                "cannot acknowledge a live acceptor"
            );
            listeners[0].stop_accepting().unwrap();
            let Outcome::Quiesced(drained) = generation.quiesced(18).unwrap() else {
                panic!("expected quiescence");
            };
            assert_eq!(drained.first_accepted_ns, ready.first_accepted_ns);
            assert!(
                drained.stopped_accepting_ns[&address.to_string()]
                    >= ready.first_accepted_ns[&address.to_string()]
            );
            drop((client, accepted));
        }
    }

    #[tokio::test]
    async fn serving_generation_eof_is_stop_and_never_resume() {
        let (owned, parent) = UnixStream::pair().unwrap();
        let mut channel = Channel::new(owned).unwrap();
        let options = Options::new("fixture".into(), 1, true);
        let signals = crate::supervisor::ArcSignal::new().unwrap();
        let mut generation = Generation::new(&[], &mut channel, 0, &options, &signals);
        drop(parent);
        assert_eq!(generation.next_event().await.unwrap(), Event::Stop);
        assert!(generation.quiesced(0).is_err());
    }

    #[test]
    fn synchronous_generation_handles_resume_and_stop_without_exposing_frames() {
        let (owned, parent) = UnixStream::pair().unwrap();
        let mut channel = Channel::new(owned).unwrap();
        let mut parent = Channel::new(parent).unwrap();
        let options = Options::new("fixture".into(), 1, false);
        let signals = crate::supervisor::ArcSignal::new().unwrap();
        let mut generation = Generation::new(&[], &mut channel, 0, &options, &signals);
        parent
            .send(
                &Command::Resume { id: 7 },
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(generation.poll_event().unwrap(), None);
        let ready = line(&mut parent);
        assert_eq!(
            serde_json::from_str::<crate::addresses::Ready>(ready.strip_prefix("ready ").unwrap())
                .unwrap()
                .request,
            7
        );
        parent
            .send(
                &Command::Retire { id: 8 },
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(generation.poll_event().unwrap(), Some(Event::Stop));
    }

    #[test]
    fn conformance_failure_never_acknowledges_a_false_drain() {
        let (owned, peer) = UnixStream::pair().unwrap();
        let inherited = Inherited {
            listeners: Vec::new(),
            channel: Channel::new(owned).unwrap(),
        };
        let error = run_checked(
            inherited,
            Options::new("fixture".into(), 1, false),
            |_| {
                Ok(Outcome::Quiesced(Quiesced {
                    id: 1,
                    stopped_accepting_ns: BTreeMap::new(),
                    first_accepted_ns: BTreeMap::new(),
                }))
            },
            |pid| {
                assert_eq!(pid, std::process::id());
                bail!("exclusive resource is still open")
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("exclusive resource"));
        let mut peer = Channel::new(peer).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match peer.poll() {
                Ok(Some(line)) => panic!("false drain acknowledgement: {line}"),
                Ok(None) => {
                    assert!(
                        Instant::now() < deadline,
                        "channel remained open after failed check"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    assert!(error.to_string().contains("closed"));
                    break;
                }
            }
        }
    }

    #[test]
    fn conformance_hook_runs_for_every_generation_before_drain_acknowledgement() {
        let (owned, peer) = UnixStream::pair().unwrap();
        let checks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = checks.clone();
        let worker = std::thread::spawn(move || {
            let inherited = Inherited {
                listeners: Vec::new(),
                channel: Channel::new(owned).unwrap(),
            };
            let mut generation = 0;
            run_checked(
                inherited,
                Options::new("fixture".into(), 1, false),
                |generation_handle| {
                    let request = generation_handle.request;
                    generation += 1;
                    assert_eq!(request, if generation == 1 { 0 } else { 2 });
                    Ok(Outcome::Quiesced(Quiesced {
                        id: generation,
                        stopped_accepting_ns: BTreeMap::new(),
                        first_accepted_ns: BTreeMap::new(),
                    }))
                },
                |_| {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
            )
        });
        let mut channel = Channel::new(peer).unwrap();
        for generation in 1..=2 {
            let deadline = Instant::now() + Duration::from_secs(3);
            let line = loop {
                if let Some(line) = channel.poll().unwrap() {
                    break line;
                }
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(1));
            };
            assert!(
                matches!(serde_json::from_str::<Reply>(&line).unwrap(), Reply::Drained { id, .. } if id == generation)
            );
            assert_eq!(
                checks.load(std::sync::atomic::Ordering::SeqCst),
                generation as usize
            );
            channel
                .send(
                    &if generation == 1 {
                        Command::Resume { id: 2 }
                    } else {
                        Command::Retire { id: 3 }
                    },
                    deadline,
                )
                .unwrap();
        }
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn owned_descriptors_are_not_inherited_by_application_subprocesses() {
        use std::os::fd::AsRawFd;
        let (socket, _peer) = UnixStream::pair().unwrap();
        let fd = socket.as_raw_fd();
        // Never make this FD inheritable in the multi-threaded test process:
        // another fixture may exec concurrently. Real inheritable worker FDs
        // are exercised by the separate-process echo/hub fixtures.
        close_on_exec(fd).unwrap();
        let result = std::process::Command::new("python3").arg("-c").arg("import os,sys\ntry: os.fstat(int(sys.argv[1]))\nexcept OSError: sys.exit(0)\nsys.exit(1)").arg(fd.to_string()).status().unwrap();
        assert!(result.success());
        assert!(
            socket.peer_addr().is_ok(),
            "the parent retains its descriptor"
        );
    }
}
