//! The stable process owns the listeners. Only its worker opens the live DB.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::{
    fs::PermissionsExt,
    net::{UnixListener, UnixStream},
    process::CommandExt,
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub addresses: Vec<SocketAddr>,
    pub db: PathBuf,
    pub away_after: u64,
    pub forget_after: u64,
    pub reconnect_grace: u64,
    #[serde(default)]
    pub activate_current: bool,
}
impl Config {
    pub fn control(&self) -> PathBuf {
        self.db.with_extension("sock")
    }
    pub fn config_path(&self) -> PathBuf {
        self.db.with_extension("worker.json")
    }
    fn active_path(&self) -> PathBuf {
        self.db.with_extension("active.json")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Swap {
    pub binary: PathBuf,
    pub sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Swapped {
    pub build: String,
    pub previous: String,
    pub rolled_back: bool,
    pub reason: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Active {
    current: PathBuf,
    previous: Option<PathBuf>,
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub fn snapshot(binary: &Path, config: &Config) -> Result<PathBuf> {
    let bytes = std::fs::read(binary)?;
    let hash = crate::updates::sha256(&bytes);
    let directory = config
        .db
        .parent()
        .context("database has no parent")?
        .join("releases")
        .join(hash);
    std::fs::create_dir_all(&directory)?;
    let destination = directory.join("cross-agent-message");
    if !destination.exists() {
        let temp = directory.join("binary.tmp");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o755))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, &destination)?;
    }
    if crate::updates::sha256(&std::fs::read(&destination)?) != crate::updates::sha256(&bytes) {
        bail!("managed binary checksum mismatch");
    }
    Ok(destination)
}

/// Every inherited source is duplicated above the destination range before dup2,
/// so rearranging descriptors cannot overwrite a source that is still needed.
fn child(binary: &Path, config: &Config, listeners: &[TcpListener]) -> Result<(Child, UnixStream)> {
    let (parent, ready) = UnixStream::pair()?;
    let sources: Vec<RawFd> = listeners
        .iter()
        .map(AsRawFd::as_raw_fd)
        .chain(std::iter::once(ready.as_raw_fd()))
        .collect();
    let ready_fd = 3 + listeners.len() as RawFd;
    let mut command = Command::new(binary);
    command
        .arg("worker")
        .arg("--config")
        .arg(config.config_path())
        .env("CROSS_AGENT_LISTEN_COUNT", listeners.len().to_string())
        .env("CROSS_AGENT_READY_FD", ready_fd.to_string())
        .env("CROSS_AGENT_CONTROL_SOCK", config.control())
        .stdin(Stdio::null());
    // SAFETY: the closure executes only async-signal-safe libc descriptor calls.
    // Each source is owned and alive in the parent until spawn has completed.
    unsafe {
        command.pre_exec(move || {
            let mut duplicated = [0; 65];
            if sources.len() > duplicated.len() {
                return Err(std::io::Error::other("too many listeners"));
            }
            for (index, source) in sources.iter().enumerate() {
                let fd = libc::fcntl(*source, libc::F_DUPFD_CLOEXEC, 128);
                if fd < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                duplicated[index] = fd;
            }
            for (index, source) in duplicated.iter().take(sources.len()).enumerate() {
                if libc::dup2(*source, 3 + index as RawFd) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                libc::close(*source);
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .with_context(|| format!("cannot start {}", binary.display()))?;
    parent.set_nonblocking(true)?;
    Ok((child, parent))
}

fn terminate(child: &mut Child, grace: Duration) -> Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    // SAFETY: this PID belongs to our live Child; SIGTERM requests a drain.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill()?;
    child.wait()?;
    Ok(())
}

fn start(
    binary: &Path,
    config: &Config,
    listeners: &[TcpListener],
    limit: Duration,
) -> Result<Child> {
    let bytes = serde_json::to_vec(config)?;
    if std::fs::read(config.config_path()).ok().as_deref() != Some(bytes.as_slice()) {
        atomic_json(&config.config_path(), config)?;
    }
    let (mut worker, mut ready) = child(binary, config, listeners)?;
    let result = (|| -> Result<()> {
        let deadline = Instant::now() + limit;
        let mut data = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            match ready.read(&mut buffer) {
                Ok(0) => bail!(
                    "worker closed readiness pipe before ready (status: {:?})",
                    worker.try_wait()?
                ),
                Ok(n) => {
                    data.extend_from_slice(&buffer[..n]);
                    if data.ends_with(b"\n") {
                        let text = std::str::from_utf8(&data)?;
                        if text.starts_with("ready ") {
                            return Ok(());
                        }
                        bail!("worker readiness refused: {text}");
                    }
                    if data.len() > 1024 {
                        bail!("invalid readiness response");
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.into()),
            }
            if let Some(status) = worker.try_wait()? {
                bail!("worker exited before ready: {status}");
            }
            if Instant::now() >= deadline {
                bail!("worker readiness timed out");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    })();
    if let Err(error) = result {
        // Every failure path, including malformed readiness, reaps the candidate
        // before a rollback is allowed to open the live database.
        terminate(&mut worker, Duration::ZERO)?;
        return Err(error);
    }
    Ok(worker)
}

pub fn request(config: &Config, swap: &Swap) -> Result<Swapped> {
    let mut socket = UnixStream::connect(config.control())
        .context("no supervised hub here; start one with serve --replace first")?;
    socket.set_read_timeout(Some(Duration::from_secs(30)))?;
    socket.write_all(&serde_json::to_vec(swap)?)?;
    socket.write_all(b"\n")?;
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(socket), &mut line)?;
    serde_json::from_str(&line).context("invalid supervisor reply")
}

pub fn stop(db: &Path) -> Result<()> {
    let mut socket = UnixStream::connect(db.with_extension("sock"))?;
    socket.set_read_timeout(Some(Duration::from_secs(6)))?;
    socket.write_all(b"{\"stop\":true}\n")?;
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(socket), &mut line)?;
    if !line.contains("stopped") {
        bail!("supervisor did not confirm shutdown");
    }
    Ok(())
}

pub fn load_config(db: &Path) -> Result<Config> {
    serde_json::from_slice(&std::fs::read(db.with_extension("worker.json"))?)
        .context("invalid worker configuration")
}

pub fn bind_listener(address: SocketAddr) -> Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

pub fn run(mut config: Config) -> Result<()> {
    if !config.db.is_absolute() {
        config.db = std::env::current_dir()?.join(&config.db);
    }
    std::fs::create_dir_all(config.db.parent().context("database has no parent")?)?;
    config.db = std::fs::canonicalize(config.db.parent().context("database parent")?)?
        .join(config.db.file_name().context("database name")?);
    let control = config.control();
    if control.exists() {
        use std::os::unix::fs::FileTypeExt;
        if UnixStream::connect(&control).is_ok() {
            bail!("a supervisor is already using {}", control.display());
        }
        if !std::fs::symlink_metadata(&control)?.file_type().is_socket() {
            bail!("refusing to replace non-socket {}", control.display());
        }
        std::fs::remove_file(&control)?;
    }
    let socket = UnixListener::bind(&control)?;
    std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o600))?;
    socket.set_nonblocking(true)?;
    let mut listeners = Vec::new();
    let mut bound = Vec::new();
    let requested = config.addresses.clone();
    for address in &requested {
        match bind_listener(*address) {
            Ok(listener) => {
                listener.set_nonblocking(true)?;
                bound.push(*address);
                listeners.push(listener);
            }
            Err(error) if !address.ip().is_loopback() => {
                eprintln!("waiting for private address {address}: {error}")
            }
            Err(error) => return Err(error),
        }
    }
    if listeners.is_empty() {
        bail!("no loopback listener available");
    }
    config.addresses = bound;
    let own_binary = snapshot(&std::env::current_exe()?, &config)?;
    let mut active: Active = std::fs::read(config.active_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(Active {
            current: own_binary.clone(),
            previous: None,
        });
    if config.activate_current {
        active.previous = Some(active.current);
        active.current = own_binary;
    }
    let signals = ArcSignal::new()?;
    let mut worker = start(&active.current, &config, &listeners, Duration::from_secs(3))?;
    atomic_json(&config.active_path(), &active)?;
    println!(
        "Supervisor {} holds {:?}; worker {} is ready.",
        std::process::id(),
        config.addresses,
        worker.id()
    );
    let mut crashes = Vec::<Instant>::new();
    let mut activated = Instant::now();
    let mut bind_retry = Instant::now();
    loop {
        if signals.stopped() {
            terminate(&mut worker, Duration::from_secs(4))?;
            let _ = std::fs::remove_file(&control);
            return Ok(());
        }
        if worker.try_wait()?.is_some() {
            crashes.retain(|at| at.elapsed() < Duration::from_secs(60));
            crashes.push(Instant::now());
            let mut rolled_back = false;
            if crashes.len() >= 3
                && activated.elapsed() < Duration::from_secs(60)
                && let Some(previous) = active.previous.take()
            {
                active.current = previous;
                rolled_back = true;
                atomic_json(&config.active_path(), &active)?;
            }
            std::thread::sleep(Duration::from_millis(
                (200 * crashes.len() as u64).min(5000),
            ));
            match start(&active.current, &config, &listeners, Duration::from_secs(3)) {
                Ok(child) => {
                    eprintln!("Worker {} is ready after restart.", child.id());
                    worker = child;
                    if rolled_back {
                        let build = crate::updates::inspect(&active.current)?.build;
                        if let Err(error) = crate::upgrade::announce(
                            &config,
                            &format!(
                                "The hub rolled back to build {build} after three worker crashes."
                            ),
                        ) {
                            eprintln!("could not announce automatic rollback: {error:#}");
                        }
                    }
                }
                Err(error) => {
                    eprintln!("worker restart failed: {error:#}");
                    continue;
                }
            }
        }
        if bind_retry.elapsed() >= Duration::from_secs(1) {
            bind_retry = Instant::now();
            let mut changed = false;
            for address in &requested {
                if config.addresses.contains(address) {
                    continue;
                }
                if let Ok(listener) = bind_listener(*address) {
                    listener.set_nonblocking(true)?;
                    listeners.push(listener);
                    config.addresses.push(*address);
                    changed = true;
                }
            }
            if changed {
                terminate(&mut worker, Duration::from_secs(4))?;
                worker = start(&active.current, &config, &listeners, Duration::from_secs(3))?;
            }
        }
        match socket.accept() {
            Ok((mut stream, _)) => {
                stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                let mut line = String::new();
                let parsed = std::io::BufRead::read_line(
                    &mut std::io::BufReader::new(&stream).take(64 * 1024),
                    &mut line,
                )
                .map_err(anyhow::Error::from)
                .and_then(|_| serde_json::from_str::<Swap>(&line).map_err(Into::into));
                if serde_json::from_str::<serde_json::Value>(&line)
                    .ok()
                    .and_then(|v| v.get("stop").and_then(|v| v.as_bool()))
                    == Some(true)
                {
                    terminate(&mut worker, Duration::from_secs(4))?;
                    let _ = writeln!(stream, "{{\"stopped\":true}}");
                    let _ = std::fs::remove_file(&control);
                    return Ok(());
                }
                let previous_build = crate::updates::inspect(&active.current)?.build;
                let result = parsed.and_then(|swap| {
                    if crate::updates::sha256(&std::fs::read(&swap.binary)?) != swap.sha256 {
                        bail!("candidate checksum changed before handoff");
                    }
                    let info = crate::updates::inspect(&swap.binary)?;
                    terminate(&mut worker, Duration::from_secs(4))?;
                    match start(&swap.binary, &config, &listeners, Duration::from_secs(3)) {
                        Ok(mut child) => {
                            let next = Active {
                                current: swap.binary,
                                previous: Some(active.current.clone()),
                            };
                            if let Err(error) = atomic_json(&config.active_path(), &next) {
                                terminate(&mut child, Duration::ZERO)?;
                                worker = start(
                                    &active.current,
                                    &config,
                                    &listeners,
                                    Duration::from_millis(2500),
                                )?;
                                return Ok(Swapped {
                                    build: previous_build.clone(),
                                    previous: previous_build.clone(),
                                    rolled_back: true,
                                    reason: Some(format!("activation failed: {error:#}")),
                                });
                            }
                            worker = child;
                            active = next;
                            activated = Instant::now();
                            crashes.clear();
                            Ok(Swapped {
                                build: info.build,
                                previous: previous_build.clone(),
                                rolled_back: false,
                                reason: None,
                            })
                        }
                        Err(error) => {
                            worker = start(
                                &active.current,
                                &config,
                                &listeners,
                                Duration::from_millis(2500),
                            )
                            .context("rollback worker did not become ready")?;
                            Ok(Swapped {
                                build: previous_build.clone(),
                                previous: previous_build.clone(),
                                rolled_back: true,
                                reason: Some(error.to_string()),
                            })
                        }
                    }
                });
                let reply = result.unwrap_or_else(|error| Swapped {
                    build: previous_build.clone(),
                    previous: previous_build,
                    rolled_back: true,
                    reason: Some(format!("{error:#}")),
                });
                let _ = writeln!(stream, "{}", serde_json::to_string(&reply)?);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::InvalidInput
                ) =>
            {
                // macOS can reject accept after an unnamed peer closes before
                // being accepted. A dead control client must not kill the hub.
            }
            Err(error) => return Err(error.into()),
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Signal delivery wakes a separate Tokio runtime; no Rust work runs in a
/// signal handler. The synchronous supervisor only reads an atomic flag.
struct ArcSignal(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl ArcSignal {
    fn new() -> Result<Self> {
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = stopped.clone();
        let (ready, registered) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return;
                }
            };
            runtime.block_on(async move {
                let mut term =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    {
                        Ok(term) => term,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                let mut interrupt =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    {
                        Ok(interrupt) => interrupt,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                let _ = ready.send(Ok(()));
                tokio::select! { _ = term.recv() => {}, _ = interrupt.recv() => {} }
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        });
        registered
            .recv_timeout(Duration::from_secs(2))
            .context("signal handler did not initialize")??;
        Ok(Self(stopped))
    }
    fn stopped(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
