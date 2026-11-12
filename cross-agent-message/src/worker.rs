//! A supervised worker owns the database and borrows inherited listeners.
use crate::{
    hub::{self, Hub},
    store::Store,
    supervisor::Config,
};
use anyhow::{Context, Result, bail};
use std::os::fd::{FromRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

pub fn run(config: Config, dashboard: crate::dashboard::Dashboard) -> Result<()> {
    let count: usize = std::env::var("CROSS_AGENT_LISTEN_COUNT")?.parse()?;
    if count == 0 || count > 64 || count != config.addresses.len() {
        bail!("invalid inherited listener count");
    }
    let ready_fd: RawFd = std::env::var("CROSS_AGENT_READY_FD")?.parse()?;
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
        let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
        listener.set_nonblocking(true)?;
        // Keep the inherited descriptors outside the serving runtime. During
        // quiescence the process still owns them but no task accepts on them.
        listeners.push(listener);
    }
    if unsafe { libc::fcntl(ready_fd, libc::F_GETFD) } < 0 {
        bail!("readiness descriptor is not open");
    }
    // SAFETY: this unique endpoint is owned solely by this worker after exec.
    let ready = unsafe { std::os::unix::net::UnixStream::from_raw_fd(ready_fd) };
    let mut ready = crate::handoff::Channel::new(ready)?;
    if config.worker_path.is_none() {
        ready.write("refused: this worker requires a stable installed-path supervisor; run the new serve --replace first\n", std::time::Instant::now() + Duration::from_secs(1))?;
        bail!(
            "this worker requires a stable installed-path supervisor; run the new serve --replace first"
        );
    }
    // Expensive executable hashing belongs to the paused startup window.
    let _ = crate::updates::binary();
    let signals = crate::supervisor::ArcSignal::new()?;
    let mut request = 0;
    loop {
        let outcome = serve(&config, dashboard.clone(), &listeners, &mut ready, request)?;
        let Outcome::Quiesced(id) = outcome else {
            return Ok(());
        };
        // serve's runtime has now been dropped: all Hub/Router clones and
        // SQLite connections are gone before this acknowledgement is sent.
        ready.send(
            &crate::handoff::Reply::Drained { id },
            std::time::Instant::now() + Duration::from_secs(1),
        )?;
        eprintln!(
            "Worker {} drained and closed SQLite; awaiting resume or retire.",
            std::process::id()
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if signals.stopped() {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                bail!("parked worker received no resume or retire within 30 seconds");
            }
            if let Some(line) = ready.poll()? {
                match serde_json::from_str::<crate::handoff::Command>(&line)? {
                    crate::handoff::Command::Resume { id } => {
                        request = id;
                        break;
                    }
                    crate::handoff::Command::Retire { .. } => return Ok(()),
                    crate::handoff::Command::Quiesce { id } => {
                        ready.send(&crate::handoff::Reply::Drained { id }, deadline)?
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

enum Outcome {
    Stopped,
    Quiesced(u64),
}

fn readiness(
    config: &Config,
    addresses: Vec<protocol::ListenAddress>,
    accepted: u64,
    request: u64,
) -> crate::addresses::Ready {
    crate::addresses::Ready {
        build: hub::BUILD.to_string(),
        addresses,
        handoff_protocol: if config.handoff_protocol == crate::handoff::VERSION {
            crate::handoff::VERSION
        } else {
            0
        },
        request,
        accepted: config
            .addresses
            .iter()
            .enumerate()
            .filter(|(index, _)| accepted & (1u64 << index) != 0)
            .map(|(_, address)| address.to_string())
            .collect(),
    }
}

#[tokio::main]
async fn serve(
    config: &Config,
    dashboard: crate::dashboard::Dashboard,
    listeners: &[std::net::TcpListener],
    ready: &mut crate::handoff::Channel,
    request: u64,
) -> Result<Outcome> {
    let mut store = Store::open(&config.db).context("worker cannot open database")?;
    store.resume_holds(hub::now())?;
    store.person_key()?;
    let hub = Arc::new(
        Hub::with_timeouts(store, config.away_after, config.forget_after)?
            .with_reconnect_grace(config.reconnect_grace)
            .supervised(true)
            .cooperative_handoff(config.handoff_protocol == crate::handoff::VERSION),
    );
    let app = hub::router(hub.clone(), dashboard);
    let local_ips = Arc::new(crate::addresses::local_ips());
    let requested = if config.requested_addresses.is_empty() {
        &config.addresses
    } else {
        &config.requested_addresses
    };
    hub.set_addresses(
        requested
            .iter()
            .map(|address| protocol::ListenAddress {
                address: address.to_string(),
                state: if config.addresses.contains(address) {
                    protocol::AddressState::Blocked
                } else {
                    protocol::AddressState::Pending
                },
                reason: Some("checking reachability".to_string()),
                remote_seen: false,
            })
            .collect(),
    );
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    let accepted_on = Arc::new(std::sync::atomic::AtomicU64::new(0));
    crate::runtime::prepare(&config.db)?;
    let control = config.worker_control();
    if control.exists() {
        std::fs::remove_file(&control)?;
    }
    let local = tokio::net::UnixListener::bind(&control)?;
    crate::runtime::legacy_alias(&config.db.with_extension("worker.sock"), &control)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o600))?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut commands = if config.handoff_protocol == crate::handoff::VERSION {
        Some(crate::handoff::Commands::new(
            tokio::net::UnixStream::from_std(ready.stream.try_clone()?)?,
        ))
    } else {
        None
    };
    // Finish fallible startup before any task can accept a TCP connection.
    let listeners = listeners
        .iter()
        .map(|listener| {
            let address = listener.local_addr()?.to_string();
            Ok((
                tokio::net::TcpListener::from_std(listener.try_clone()?)?,
                address,
            ))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    if config.handoff_startup && config.handoff_protocol == crate::handoff::VERSION {
        ready.write(
            &format!("accepting {request}\n"),
            std::time::Instant::now() + Duration::from_secs(1),
        )?;
    }
    for (index, (listener, local_address)) in listeners.into_iter().enumerate() {
        let accepted_on = accepted_on.clone();
        let app = app.clone();
        let peer_hub = hub.clone();
        let local_ips = local_ips.clone();
        let shutdown = shutdown.subscribe();
        tasks.spawn(async move {
            let mut shutdown = shutdown;
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = connections.join_next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => {
                        let (socket, peer) = accepted?;
                        accepted_on.fetch_or(1u64 << index, std::sync::atomic::Ordering::SeqCst);
                        let external = crate::addresses::is_remote(peer.ip(), &local_ips);
                        let peer_hub = peer_hub.clone(); let local_address = local_address.clone();
                        let app = app.clone(); let mut shutdown = shutdown.clone();
                        connections.spawn(async move {
                            let mut builder = hyper::server::conn::http1::Builder::new();
                            builder.timer(hyper_util::rt::TokioTimer::new()).header_read_timeout(Duration::from_secs(4));
                            let observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                            let closing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                            let seen = observed.clone(); let is_closing = closing.clone();
                            let service = hyper::service::service_fn(move |request| {
                                if external { peer_hub.note_remote(&local_address); }
                                seen.store(true, std::sync::atomic::Ordering::SeqCst);
                                let app = app.clone(); let closing = is_closing.clone();
                                async move {
                                    let mut response = tower::ServiceExt::oneshot(app, request).await?;
                                    if closing.load(std::sync::atomic::Ordering::SeqCst) { response.headers_mut().insert("connection", hyper::header::HeaderValue::from_static("close")); }
                                    Ok::<_, std::convert::Infallible>(response)
                                }
                            });
                            let connection = builder.serve_connection(hyper_util::rt::TokioIo::new(socket), service);
                            tokio::pin!(connection);
                            tokio::select! {
                                biased;
                                _ = &mut connection => {},
                                _ = shutdown.changed() => {
                                    closing.store(true, std::sync::atomic::Ordering::SeqCst);
                                    // A fresh accepted TCP connection is not an idle
                                    // keep-alive. Let its first request reach the
                                    // service before closing, within the drain bound.
                                    if observed.load(std::sync::atomic::Ordering::SeqCst) { connection.as_mut().graceful_shutdown(); }
                                    let _ = tokio::time::timeout(Duration::from_secs(4), &mut connection).await;
                                },
                            }
                        });
                    }
                }
            }
            while connections.join_next().await.is_some() {}
            Ok::<(), anyhow::Error>(())
        });
    }
    let local_hub = hub.clone();
    let mut local_shutdown = shutdown.subscribe();
    let control_db = config.db.clone();
    tasks.spawn(async move {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, AsyncReadExt};
        let mut local = local;
        let mut check = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = local_shutdown.changed() => return Ok::<(), anyhow::Error>(()),
                _ = check.tick() => {
                    let path = crate::runtime::socket(&control_db, "worker.sock");
                    if !path.exists() {
                        crate::runtime::prepare(&control_db)?;
                        local = tokio::net::UnixListener::bind(&path)?;
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
                    }
                },
                accepted = local.accept() => {
                    let (stream, _) = accepted?;
                    let (read, mut write) = stream.into_split();
                    let mut line = String::new();
                    let request = tokio::time::timeout(Duration::from_secs(1), tokio::io::BufReader::new(read).take(64*1024).read_line(&mut line)).await;
                    if matches!(request, Ok(Ok(_)))
                        && let Ok(value) = serde_json::from_str::<serde_json::Value>(&line)
                        && let Some(body) = value.get("announce").and_then(|v| v.as_str())
                        && local_hub.announcement(body).is_ok() {
                        let _ = write.write_all(b"{\"ok\":true}\n").await;
                    }
                }
            }
        }
    });
    // Initial probes run alongside lifecycle commands. A failed startup can
    // therefore be quiesced even before it has published a readiness reply.
    let initial = crate::addresses::initial(config, &hub);
    tokio::pin!(initial);
    let mut ready_sent = false;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
    let stopped = hub.stopped();
    tokio::pin!(stopped);
    let outcome = loop {
        tokio::select! {
            addresses = &mut initial, if !ready_sent => {
                ready.write(&format!("ready {}\n", serde_json::to_string(&readiness(config, addresses?, accepted_on.load(std::sync::atomic::Ordering::SeqCst), request))?), std::time::Instant::now() + Duration::from_secs(1))?;
                ready_sent = true;
                tasks.spawn(crate::addresses::monitor(config.clone(), hub.clone(), shutdown.subscribe()));
            },
            _ = heartbeat.tick() => hub.heartbeat().map_err(|e| anyhow::anyhow!("heartbeat failed: {e:?}"))?,
            _ = term.recv() => break Outcome::Stopped,
            _ = interrupt.recv() => break Outcome::Stopped,
            _ = &mut stopped => break Outcome::Stopped,
            command = async {
                match commands.as_mut() {
                    Some(stream) => stream.receive().await,
                    None => std::future::pending().await,
                }
            } => {
                match command? {
                    crate::handoff::Command::Quiesce { id } => break Outcome::Quiesced(id),
                    crate::handoff::Command::Retire { .. } => break Outcome::Stopped,
                    crate::handoff::Command::Resume { id } => {
                        ready.write(&format!("ready {}\n", serde_json::to_string(&readiness(config, hub.info().addresses, accepted_on.load(std::sync::atomic::Ordering::SeqCst), id))?), std::time::Instant::now() + Duration::from_secs(1))?;
                    }
                }
            },
            result = tasks.join_next() => { if let Some(result) = result { result??; } bail!("listener stopped unexpectedly"); },
        }
    };
    hub.begin_drain()
        .map_err(|e| anyhow::anyhow!("cannot record drain: {e:?}"))?;
    let _ = shutdown.send(true);
    while tasks.join_next().await.is_some() {}
    // Connection tasks finish under HTTP graceful shutdown. Track the finite
    // drain window before dropping the runtime (the supervisor enforces it too).

    let _ = std::fs::remove_file(control);
    Ok(outcome)
}
