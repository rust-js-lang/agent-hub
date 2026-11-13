//! A supervised worker owns the database and borrows inherited listeners.
use crate::{
    hub::{self, Hub},
    store::Store,
    supervisor::Config,
};
use anyhow::{Context, Result, bail};
use process_supervisor::worker::{Event, Generation, Options, Outcome};
use std::sync::Arc;
use std::time::Duration;

pub fn run(config: Config, dashboard: crate::dashboard::Dashboard) -> Result<()> {
    let count: usize = std::env::var("CROSS_AGENT_LISTEN_COUNT")?.parse()?;
    let ready_fd = std::env::var("CROSS_AGENT_READY_FD")?.parse()?;
    // SAFETY: this private worker entry point is exec'd by the supervisor,
    // which transfers the validated unique listener and lifecycle descriptors.
    let mut inherited = unsafe {
        process_supervisor::worker::Inherited::take(count, ready_fd, config.addresses.len())?
    };
    if config.worker_path.is_none() {
        inherited.channel.write("refused: this worker requires a stable installed-path supervisor; run the new serve --replace first\n", std::time::Instant::now() + Duration::from_secs(1))?;
        bail!(
            "this worker requires a stable installed-path supervisor; run the new serve --replace first"
        );
    }
    // Hash the executable before the application starts serving.
    let _ = crate::updates::binary();
    let options = Options::new(
        hub::BUILD.to_string(),
        config.handoff_protocol,
        config.handoff_startup,
    );
    process_supervisor::worker::run(inherited, options, |generation| {
        serve(&config, dashboard.clone(), generation)
    })
}

#[tokio::main]
async fn serve(
    config: &Config,
    dashboard: crate::dashboard::Dashboard,
    generation: &mut Generation<'_>,
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
    // The generation owns listener instrumentation and lifecycle framing.
    let listeners = generation.listeners()?;
    generation.accepting()?;
    for mut listener in listeners {
        let local_address = listener.timing().address.clone();
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
                    _ = shutdown.changed() => {
                        listener.stop_accepting()?;
                        break;
                    },
                    _ = connections.join_next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => {
                        let (socket, peer) = accepted?;
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
                                    let _ = process_supervisor::listener::drain_connection(&mut connection).await;
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
                generation.ready(addresses?.into_iter().map(crate::addresses::process_address).collect())?;
                ready_sent = true;
                tasks.spawn(crate::addresses::monitor(config.clone(), hub.clone(), shutdown.subscribe()));
            },
            _ = heartbeat.tick() => hub.heartbeat().map_err(|e| anyhow::anyhow!("heartbeat failed: {e:?}"))?,
            _ = term.recv() => break None,
            _ = interrupt.recv() => break None,
            _ = &mut stopped => break None,
            event = generation.next_event_observing(|| Some(hub.info().addresses.into_iter().map(crate::addresses::process_address).collect())) => match event? {
                Event::Quiesce { id } => break Some(id),
                Event::Stop => break None,
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
    match outcome {
        None => Ok(Outcome::Stopped),
        Some(id) => generation.quiesced(id),
    }
}
