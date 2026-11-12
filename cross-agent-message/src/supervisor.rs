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
    #[serde(default)]
    pub worker_path: Option<PathBuf>,
    #[serde(default)]
    pub requested_addresses: Vec<SocketAddr>,
    #[serde(default)]
    pub required_ready: Vec<String>,
}
impl Config {
    pub fn control(&self) -> PathBuf {
        crate::runtime::socket(&self.db, "hub.sock")
    }
    pub fn worker_control(&self) -> PathBuf {
        crate::runtime::socket(&self.db, "worker.sock")
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
    #[serde(default)]
    pub addresses: Vec<protocol::ListenAddress>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Active {
    current: PathBuf,
    previous: Option<PathBuf>,
}

fn valid_snapshot(path: &Path, config: &Config) -> bool {
    let Some(directory) = path.parent() else {
        return false;
    };
    let Some(hash) = directory.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(parent) = config.db.parent() else {
        return false;
    };
    if path
        .file_name()
        .is_none_or(|name| name != "cross-agent-message")
        || directory.parent() != Some(parent.join("releases").as_path())
        || hash.len() != 64
        || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
        && std::fs::read(path).is_ok_and(|bytes| crate::updates::sha256(&bytes) == hash)
        && crate::updates::inspect(path).is_ok()
}

struct Worker {
    process: Child,
    addresses: Vec<protocol::ListenAddress>,
}
impl std::ops::Deref for Worker {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.process
    }
}
impl std::ops::DerefMut for Worker {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.process
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        // A recoverable supervisor error must not strand a database writer.
        if self.process.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}

fn start_or_restore(
    active: &mut Active,
    config: &mut Config,
    listeners: &[TcpListener],
) -> Result<Worker> {
    match start(&active.current, config, listeners, Duration::from_secs(3)) {
        Ok(worker) => Ok(worker),
        Err(error) => {
            let previous = active.previous.take().with_context(|| {
                format!("worker startup failed and no previous image is available: {error:#}")
            })?;
            eprintln!("Worker startup failed; restoring the previous installed image: {error:#}");
            active.current = previous;
            atomic_json(&config.active_path(), active)?;
            start(
                &active.current,
                config,
                listeners,
                Duration::from_millis(2500),
            )
            .context("previous worker did not recover")
        }
    }
}

/// An installer may replace the executable while the old worker keeps serving.
/// A later restart must validate that replacement as an upgrade, not adopt it
/// silently. Failed preflight leaves the recorded image to be restored by start.
fn reconcile_installation(active: &mut Active, config: &Config) -> Result<bool> {
    let executable = config
        .worker_path
        .as_deref()
        .context("stable worker path missing")?;
    let Ok(current) = std::fs::read(executable) else {
        return Ok(false);
    };
    let expected = crate::updates::sha256(&std::fs::read(&active.current)?);
    if crate::updates::sha256(&current) == expected {
        return Ok(false);
    }
    eprintln!(
        "The installed executable changed outside upgrade; running preflight before restarting it."
    );
    let candidate = snapshot(executable, config)?;
    if let Err(error) = crate::upgrade::preflight(&candidate, config) {
        eprintln!("External replacement failed preflight; restoring the recorded image: {error:#}");
        return Ok(false);
    }
    let next = Active {
        current: candidate,
        previous: Some(active.current.clone()),
    };
    atomic_json(&config.active_path(), &next)?;
    *active = next;
    Ok(true)
}

pub(crate) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let parent = path
        .parent()
        .context("state file has no parent directory")?;
    let temporary = crate::installation::temporary(parent, "state");
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
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
        let temp = crate::installation::temporary(&directory, "snapshot");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o755))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp, &destination)?;
        std::fs::File::open(&directory)?.sync_all()?;
        if let Some(parent) = directory.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
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
    config: &mut Config,
    listeners: &[TcpListener],
    limit: Duration,
) -> Result<Worker> {
    let deadline = Instant::now() + limit;
    let bytes = serde_json::to_vec(config)?;
    if std::fs::read(config.config_path()).ok().as_deref() != Some(bytes.as_slice()) {
        atomic_json(&config.config_path(), config)?;
    }
    let executable = config
        .worker_path
        .clone()
        .context("stable worker path is missing; restart with the new serve --replace")?;
    let checksum = binary
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .context("worker image is not a managed recovery snapshot")?;
    crate::installation::activate_verified(binary, &executable, checksum)?;
    crate::runtime::prepare_worker(&config.db)?;
    let (mut worker, mut ready) = child(&executable, config, listeners)?;
    let result = (|| -> Result<Vec<protocol::ListenAddress>> {
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
                        if let Some(payload) = text.strip_prefix("ready ") {
                            let observed = if let Ok(ready) =
                                serde_json::from_str::<crate::addresses::Ready>(payload.trim())
                            {
                                ready.addresses
                            } else {
                                // A previous release reports only its build. Verify
                                // every address ourselves before accepting it.
                                let remaining = deadline.saturating_duration_since(Instant::now());
                                if remaining.is_zero() {
                                    bail!("worker readiness timed out");
                                }
                                crate::addresses::probe_for(
                                    config,
                                    payload.trim(),
                                    remaining.min(Duration::from_secs(1)),
                                )
                            };
                            for address in config
                                .addresses
                                .first()
                                .map(ToString::to_string)
                                .into_iter()
                                .chain(config.required_ready.iter().cloned())
                            {
                                if !observed.iter().any(|status| {
                                    status.address == address
                                        && status.state == protocol::AddressState::Ready
                                }) {
                                    let reason = observed
                                        .iter()
                                        .find(|status| status.address == address)
                                        .and_then(|status| status.reason.as_deref())
                                        .unwrap_or("address is not ready");
                                    bail!("required address {address} is not ready: {reason}");
                                }
                            }
                            if Instant::now() > deadline {
                                bail!("worker readiness timed out");
                            }
                            return Ok(observed);
                        }
                        bail!("worker readiness refused: {text}");
                    }
                    if data.len() > 1024 * 1024 {
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
    let addresses = match result {
        Ok(addresses) => addresses,
        Err(error) => {
            // Every failure path reaps the candidate before rollback opens SQLite.
            terminate(&mut worker, Duration::ZERO)?;
            return Err(error);
        }
    };
    let worker = Worker {
        process: worker,
        addresses,
    };
    config.required_ready = crate::addresses::ready(&worker.addresses);
    eprintln!(
        "Worker {} is ready at {}.",
        worker.id(),
        executable.display()
    );
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
    let config = load_config(db)?;
    // Cold replacement must also stop a supervisor from before short IPC paths.
    let mut socket = UnixStream::connect(config.control())
        .or_else(|_| UnixStream::connect(db.with_extension("sock")))?;
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
    let canonical = canonical_db(db)?;
    let bytes = std::fs::read(canonical.with_extension("worker.json"))
        .or_else(|_| std::fs::read(db.with_extension("worker.json")))?;
    serde_json::from_slice(&bytes).context("invalid worker configuration")
}

fn canonical_db(db: &Path) -> Result<PathBuf> {
    if db.exists() {
        return Ok(std::fs::canonicalize(db)?);
    }
    if std::fs::symlink_metadata(db).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        bail!(
            "database path {} is a dangling or invalid symlink",
            db.display()
        );
    }
    let absolute = if db.is_absolute() {
        db.to_path_buf()
    } else {
        std::env::current_dir()?.join(db)
    };
    Ok(
        std::fs::canonicalize(absolute.parent().context("database parent")?)?
            .join(absolute.file_name().context("database name")?),
    )
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

fn attach_pending(
    config: &mut Config,
    listeners: &mut Vec<TcpListener>,
    requested: &[SocketAddr],
    mut bind: impl FnMut(SocketAddr) -> Result<TcpListener>,
) -> Result<bool> {
    let mut changed = false;
    for address in requested {
        if config.addresses.contains(address) {
            continue;
        }
        if let Ok(listener) = bind(*address) {
            listener.set_nonblocking(true)?;
            listeners.push(listener);
            config.addresses.push(*address);
            changed = true;
        }
    }
    Ok(changed)
}

pub fn run(mut config: Config) -> Result<()> {
    if config.addresses.is_empty()
        || config.addresses.len() > 64
        || !config.addresses[0].ip().is_loopback()
    {
        bail!("configure a loopback address first and at most 64 listening addresses");
    }
    let original_db = config.db.clone();
    if !config.db.is_absolute() {
        config.db = std::env::current_dir()?.join(&config.db);
    }
    std::fs::create_dir_all(config.db.parent().context("database has no parent")?)?;
    config.db = canonical_db(&config.db)?;
    let installation =
        crate::installation::Installation::acquire(&std::env::current_exe()?, &config.db)?;
    let prior_config = load_config(&config.db).ok();
    let same_installation = prior_config
        .as_ref()
        .and_then(|prior| prior.worker_path.as_ref())
        == Some(&installation.executable);
    config.worker_path = Some(installation.executable.clone());
    crate::runtime::prepare(&config.db)?;
    let control = config.control();
    if UnixStream::connect(config.db.with_extension("sock")).is_ok()
        || UnixStream::connect(original_db.with_extension("sock")).is_ok()
    {
        bail!(
            "a legacy supervisor is still using this database; stop it before starting the stable-path supervisor"
        );
    }
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
    crate::runtime::prepare_worker(&config.db)?;
    let mut socket = UnixListener::bind(&control)?;
    crate::runtime::legacy_alias(&config.db.with_extension("sock"), &control)?;
    std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o600))?;
    socket.set_nonblocking(true)?;
    let mut listeners = Vec::new();
    let mut bound = Vec::new();
    let requested = config.addresses.clone();
    config.requested_addresses = requested.clone();
    for address in &requested {
        match bind_listener(*address) {
            Ok(listener) => {
                listener.set_nonblocking(true)?;
                bound.push(*address);
                listeners.push(listener);
            }
            Err(error)
                if !address.ip().is_loopback()
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.raw_os_error() == Some(libc::EADDRNOTAVAIL)) =>
            {
                eprintln!("waiting for private address {address}: {error}")
            }
            Err(error) => return Err(error),
        }
    }
    if listeners.is_empty() {
        bail!("no loopback listener available");
    }
    config.addresses = bound;
    let own_binary = snapshot(&installation.executable, &config)?;
    let recorded: Option<Active> = std::fs::read(config.active_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let valid_record = recorded
        .as_ref()
        .is_some_and(|active| same_installation && valid_snapshot(&active.current, &config));
    if recorded.is_some() && !valid_record {
        eprintln!(
            "Ignoring stale supervisor state from before this stable installation; using {}.",
            installation.executable.display()
        );
    }
    let mut active = recorded.filter(|_| valid_record).unwrap_or(Active {
        current: own_binary.clone(),
        previous: None,
    });
    if active
        .previous
        .as_ref()
        .is_some_and(|previous| !valid_snapshot(previous, &config))
    {
        eprintln!("Ignoring stale previous-worker snapshot.");
        active.previous = None;
    }
    if config.activate_current {
        active.previous = Some(active.current);
        active.current = own_binary;
    }
    let signals = ArcSignal::new()?;
    reconcile_installation(&mut active, &config)?;
    let mut worker = start_or_restore(&mut active, &mut config, &listeners)?;
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
            // This is only observation data from the child we actually spawned,
            // never a PID to signal or a process identity loaded from disk.
            if let Ok(bytes) = std::fs::read(config.db.with_extension("addresses.json"))
                && let Ok(observed) =
                    serde_json::from_slice::<crate::addresses::Observation>(&bytes)
                && observed.pid == worker.id()
            {
                config.required_ready = crate::addresses::ready(&observed.addresses);
            }
            let replaced = reconcile_installation(&mut active, &config)?;
            if replaced {
                crashes.clear();
                activated = Instant::now();
            }
            crashes.retain(|at| at.elapsed() < Duration::from_secs(60));
            if !replaced {
                crashes.push(Instant::now());
            }
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
            let restart = if replaced {
                start_or_restore(&mut active, &mut config, &listeners)
            } else {
                start(
                    &active.current,
                    &mut config,
                    &listeners,
                    Duration::from_secs(3),
                )
            };
            match restart {
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
            // /tmp may be cleaned while the supervisor still owns its listener.
            // Recover the directory entry rather than leaving upgrade unreachable.
            if !control.exists() {
                crate::runtime::prepare(&config.db)?;
                socket = UnixListener::bind(&control)?;
                std::fs::set_permissions(&control, std::fs::Permissions::from_mode(0o600))?;
                socket.set_nonblocking(true)?;
            }
            let changed = attach_pending(&mut config, &mut listeners, &requested, bind_listener)?;
            if changed {
                let build = crate::updates::inspect(&active.current)?.build;
                let replaced = reconcile_installation(&mut active, &config)?;
                worker.addresses = crate::addresses::probe(&config, &build);
                config.required_ready = crate::addresses::ready(&worker.addresses);
                terminate(&mut worker, Duration::from_secs(4))?;
                let restart = if replaced {
                    start_or_restore(&mut active, &mut config, &listeners)
                } else {
                    start(
                        &active.current,
                        &mut config,
                        &listeners,
                        Duration::from_secs(3),
                    )
                };
                match restart {
                    Ok(next) => worker = next,
                    Err(error) => eprintln!(
                        "listener handoff failed; retaining sockets and retrying worker: {error:#}"
                    ),
                }
                if replaced {
                    activated = Instant::now();
                    crashes.clear();
                }
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
                    let candidate = snapshot(&swap.binary, &config)?;
                    if candidate
                        .parent()
                        .and_then(|path| path.file_name())
                        .and_then(|name| name.to_str())
                        != Some(swap.sha256.as_str())
                    {
                        bail!("candidate changed while it was being staged");
                    }
                    let info = crate::updates::inspect(&candidate)?;
                    crate::installation::check_writable(
                        config
                            .worker_path
                            .as_deref()
                            .context("stable worker path missing")?,
                    )?;
                    // Assess the exact final file while the old worker serves.
                    // A fresh Mach-O's first execution may otherwise consume the
                    // handoff deadline. start reuses this inode when unchanged.
                    let executable = config.worker_path.as_deref().context("stable worker path missing")?;
                    let prepared = (|| -> Result<()> {
                        crate::installation::activate_verified(&candidate, executable, &swap.sha256)?;
                        crate::updates::inspect(executable)?;
                        if crate::updates::sha256(&std::fs::read(executable)?) != swap.sha256 { bail!("installed image changed during final-path preflight"); }
                        Ok(())
                    })();
                    if let Err(error) = prepared {
                        let previous_sha = active.current.parent().and_then(|parent| parent.file_name()).and_then(|name| name.to_str()).context("previous image checksum missing")?;
                        if let Err(restore) = crate::installation::activate_verified(&active.current, executable, previous_sha) {
                            eprintln!("could not restore installed image; existing worker is still serving: {restore:#}");
                        }
                        return Err(error.context("candidate final-path preflight failed; existing worker retained"));
                    }
                    // Probe before draining: already-pending/blocked addresses do
                    // not prevent an upgrade, but every ready address is protected.
                    worker.addresses = crate::addresses::probe(&config, &previous_build);
                    config.required_ready = crate::addresses::ready(&worker.addresses);
                    let protected_before_swap = config.required_ready.clone();
                    terminate(&mut worker, Duration::from_secs(4))?;
                    match start(&candidate, &mut config, &listeners, Duration::from_secs(3)) {
                        Ok(mut child) => {
                            let next = Active {
                                current: candidate,
                                previous: Some(active.current.clone()),
                            };
                            if let Err(error) = atomic_json(&config.active_path(), &next) {
                                terminate(&mut child, Duration::ZERO)?;
                                config.required_ready = protected_before_swap;
                                worker = start(
                                    &active.current,
                                    &mut config,
                                    &listeners,
                                    Duration::from_millis(2500),
                                )?;
                                return Ok(Swapped {
                                    build: previous_build.clone(),
                                    previous: previous_build.clone(),
                                    rolled_back: true,
                                    reason: Some(format!("activation failed: {error:#}")),
                                    addresses: worker.addresses.clone(),
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
                                addresses: worker.addresses.clone(),
                            })
                        }
                        Err(error) => {
                            worker = start(
                                &active.current,
                                &mut config,
                                &listeners,
                                Duration::from_millis(2500),
                            )
                            .context("rollback worker did not become ready")?;
                            Ok(Swapped {
                                build: previous_build.clone(),
                                previous: previous_build.clone(),
                                rolled_back: true,
                                reason: Some(error.to_string()),
                                addresses: worker.addresses.clone(),
                            })
                        }
                    }
                });
                let reply = result.unwrap_or_else(|error| Swapped {
                    build: previous_build.clone(),
                    previous: previous_build,
                    rolled_back: true,
                    reason: Some(format!("{error:#}")),
                    addresses: Vec::new(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pending_address_is_attached_once_when_it_can_be_bound() {
        let first = bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let initial = first.local_addr().unwrap();
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let pending = probe.local_addr().unwrap();
        drop(probe);
        let requested = vec![initial, pending];
        let mut config = Config {
            addresses: vec![initial],
            requested_addresses: requested.clone(),
            required_ready: Vec::new(),
            worker_path: None,
            db: PathBuf::from("unused.db"),
            away_after: 60_000,
            forget_after: 120_000,
            reconnect_grace: 0,
            activate_current: false,
        };
        let mut listeners = vec![first];
        assert!(
            !attach_pending(&mut config, &mut listeners, &requested, |_| Err(
                std::io::Error::from_raw_os_error(libc::EADDRNOTAVAIL).into()
            ))
            .unwrap()
        );
        assert_eq!(config.addresses, vec![initial]);
        assert!(attach_pending(&mut config, &mut listeners, &requested, bind_listener).unwrap());
        assert_eq!(config.addresses, requested);
        assert_eq!(
            listeners
                .iter()
                .map(|listener| listener.local_addr().unwrap())
                .collect::<Vec<_>>(),
            config.addresses
        );
        assert!(
            !attach_pending(&mut config, &mut listeners, &requested, |_| panic!(
                "an already-bound address must not be rebound"
            ))
            .unwrap()
        );
    }
}
