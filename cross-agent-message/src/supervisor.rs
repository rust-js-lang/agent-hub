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
    #[serde(default)]
    pub handoff_protocol: u32,
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
    channel: crate::handoff::Channel,
    handoff: bool,
    sequence: u64,
    phase: Phase,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Quiescing,
    Parked,
    Resuming(u64),
    Retiring,
}

// ALF may look up a flow's already-attributed PID asynchronously. Keep the
// database-free predecessor alive after replacement readiness so those
// lookups can still resolve it, including when the firewall daemon is busy.
const RETIRE_GRACE: Duration = Duration::from_secs(5);

struct Retirement {
    worker: Worker,
    after: Instant,
}

fn activate_worker(worker: &mut Worker, next: Worker, retirement: &mut Option<Retirement>) {
    assert!(retirement.is_none(), "worker handoffs must be serialized");
    let mut previous = std::mem::replace(worker, next);
    if previous.handoff {
        previous.phase = Phase::Retiring;
        *retirement = Some(Retirement {
            worker: previous,
            after: Instant::now() + RETIRE_GRACE,
        });
    }
}

impl Worker {
    fn next_request(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }
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
    let (mut process, stream) = child(&executable, config, listeners)?;
    let mut channel = crate::handoff::Channel::new(stream)?;
    let ready = match wait_ready(&mut process, &mut channel, config, 0, deadline) {
        Ok(ready) => ready,
        Err(error) => {
            // Reap the candidate before any rollback can reopen SQLite.
            terminate(&mut process, Duration::ZERO)?;
            return Err(error);
        }
    };
    let worker = Worker {
        process,
        channel,
        handoff: ready.handoff_protocol == crate::handoff::VERSION
            && config.handoff_protocol == crate::handoff::VERSION,
        addresses: ready.addresses,
        sequence: 0,
        phase: Phase::Running,
    };
    config.required_ready = crate::addresses::ready(&worker.addresses);
    eprintln!(
        "Worker {} is ready at {}.",
        worker.id(),
        executable.display()
    );
    Ok(worker)
}

fn protected(config: &Config) -> impl Iterator<Item = String> + '_ {
    config
        .addresses
        .first()
        .map(ToString::to_string)
        .into_iter()
        .chain(config.required_ready.iter().cloned())
}

fn validate_ready(ready: &crate::addresses::Ready, config: &Config) -> Result<()> {
    if ready.handoff_protocol != 0 && ready.handoff_protocol != crate::handoff::VERSION {
        bail!(
            "unsupported worker handoff protocol {}",
            ready.handoff_protocol
        );
    }
    for address in protected(config) {
        if !ready.addresses.iter().any(|status| {
            status.address == address && status.state == protocol::AddressState::Ready
        }) {
            let reason = ready
                .addresses
                .iter()
                .find(|status| status.address == address)
                .and_then(|status| status.reason.as_deref())
                .unwrap_or("address is not ready");
            bail!("required address {address} is not ready: {reason}");
        }
        if ready.handoff_protocol == crate::handoff::VERSION && !ready.accepted.contains(&address) {
            bail!("worker has not accepted on required address {address}");
        }
    }
    Ok(())
}

fn wait_ready(
    process: &mut Child,
    channel: &mut crate::handoff::Channel,
    config: &Config,
    request: u64,
    deadline: Instant,
) -> Result<crate::addresses::Ready> {
    loop {
        if Instant::now() >= deadline {
            bail!("worker readiness timed out");
        }
        if let Some(line) = channel.poll()? {
            if let Some(payload) = line.strip_prefix("ready ") {
                let ready = if let Ok(ready) =
                    serde_json::from_str::<crate::addresses::Ready>(payload.trim())
                {
                    ready
                } else if request == 0 {
                    // Legacy workers have no handoff channel; their own ready
                    // signal plus fresh full HTTP probes proves startup.
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        bail!("worker readiness timed out");
                    }
                    crate::addresses::Ready {
                        build: payload.trim().to_string(),
                        addresses: crate::addresses::probe_for(
                            config,
                            payload.trim(),
                            remaining.min(Duration::from_secs(1)),
                        ),
                        handoff_protocol: 0,
                        request: 0,
                        accepted: Vec::new(),
                    }
                } else {
                    bail!("invalid resumed worker readiness");
                };
                if ready.request != request {
                    continue;
                }
                validate_ready(&ready, config)?;
                if Instant::now() > deadline {
                    bail!("worker readiness timed out");
                }
                return Ok(ready);
            }
            // A late quiesce reply may precede the matching resume readiness.
            if serde_json::from_str::<crate::handoff::Reply>(&line).is_err() {
                bail!("worker readiness refused: {line}");
            }
        }
        if let Some(status) = process.try_wait()? {
            bail!("worker exited before ready: {status}");
        }
        if Instant::now() >= deadline {
            bail!("worker readiness timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn restore_image(snapshot: &Path, config: &Config) -> Result<()> {
    let checksum = snapshot
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|p| p.to_str())
        .context("recovery snapshot checksum missing")?;
    crate::installation::activate_verified(
        snapshot,
        config
            .worker_path
            .as_deref()
            .context("stable worker path missing")?,
        checksum,
    )
}

fn resume(worker: &mut Worker, image: &Path, config: &Config) -> Result<()> {
    restore_image(image, config)?;
    let id = worker.next_request();
    worker.phase = Phase::Resuming(id);
    let deadline = Instant::now() + Duration::from_millis(2500);
    worker
        .channel
        .send(&crate::handoff::Command::Resume { id }, deadline)?;
    let ready = wait_ready(
        &mut worker.process,
        &mut worker.channel,
        config,
        id,
        deadline,
    )?;
    worker.addresses = ready.addresses;
    worker.phase = Phase::Running;
    eprintln!(
        "Worker {} resumed on the same process after rollback.",
        worker.id()
    );
    Ok(())
}

/// A late drain never licenses another database writer. On any failure,
/// request resume and keep owning the original child instead of killing it.
fn quiesce(worker: &mut Worker, image: &Path, config: &Config) -> Result<()> {
    if !worker.handoff {
        eprintln!(
            "Legacy outgoing worker has no cooperative handoff; this transition may interrupt a remote connection."
        );
        return terminate(worker, Duration::from_secs(4));
    }
    if worker.phase != Phase::Running {
        bail!("worker handoff is still awaiting recovery");
    }
    let id = worker.next_request();
    let deadline = Instant::now() + Duration::from_secs(4);
    worker.phase = Phase::Quiescing;
    let result = (|| -> Result<()> {
        worker
            .channel
            .send(&crate::handoff::Command::Quiesce { id }, deadline)?;
        loop {
            if let Some(line) = worker.channel.poll()? {
                match serde_json::from_str::<crate::handoff::Reply>(&line) {
                    Ok(crate::handoff::Reply::Drained { id: reply }) if reply == id => {
                        worker.phase = Phase::Parked;
                        eprintln!(
                            "Worker {} acknowledged drained; database is closed and PID retained.",
                            worker.id()
                        );
                        return Ok(());
                    }
                    Ok(_) => {}
                    Err(_) => bail!("invalid worker drain acknowledgement"),
                }
            }
            if worker.try_wait()?.is_some() {
                bail!("worker exited before acknowledging drained");
            }
            if Instant::now() >= deadline {
                bail!("worker drain timed out; cancelling handoff");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    if let Err(error) = result {
        resume(worker, image, config)
            .with_context(|| format!("{error:#}; old worker resume did not finish"))?;
        return Err(error);
    }
    Ok(())
}

fn retire(worker: &mut Worker) -> Result<()> {
    if !worker.handoff {
        return Ok(());
    } // legacy child was already reaped
    if !matches!(worker.phase, Phase::Parked | Phase::Retiring) {
        bail!("cannot retire a worker whose database may still be open");
    }
    let id = worker.next_request();
    worker.channel.send(
        &crate::handoff::Command::Retire { id },
        Instant::now() + Duration::from_millis(250),
    )?;
    let deadline = Instant::now() + Duration::from_millis(250);
    while worker.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            // Drained was acknowledged, so this process owns no SQLite handle.
            worker.kill()?;
            worker.wait()?;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn recover(
    worker: &mut Worker,
    image: &Path,
    config: &mut Config,
    listeners: &[TcpListener],
) -> Result<()> {
    if worker.handoff && worker.try_wait()?.is_none() {
        resume(worker, image, config)
    } else {
        *worker = start(image, config, listeners, Duration::from_millis(2500))?;
        Ok(())
    }
}

/// Shutdown is complete only when every inherited listener has been released.
/// In particular, a parked predecessor must not outlive the stop reply.
fn shutdown(
    worker: &mut Worker,
    retirement: &mut Option<Retirement>,
    listeners: &mut Vec<TcpListener>,
    pending_upgrade: &mut Option<(UnixStream, String)>,
    image: &Path,
) -> Result<()> {
    if let Some((mut stream, _)) = pending_upgrade.take() {
        let build =
            crate::updates::inspect(image).map_or_else(|_| String::new(), |info| info.build);
        let reply = Swapped {
            build: build.clone(),
            previous: build,
            rolled_back: true,
            reason: Some("supervisor is stopping; queued upgrade was not started".into()),
            addresses: Vec::new(),
        };
        let _ = writeln!(stream, "{}", serde_json::to_string(&reply)?);
    }
    // Attempt both children even if one shutdown fails. Dropping the owned
    // predecessor also reaps it if the graceful retirement channel failed.
    let active = terminate(worker, Duration::from_secs(4));
    let parked = retirement
        .take()
        .map_or(Ok(()), |mut old| retire(&mut old.worker));
    listeners.clear();
    active.and(parked)
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
    config.handoff_protocol = crate::handoff::VERSION;
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
    let mut handoff_retry = false;
    let mut retirement: Option<Retirement> = None;
    let mut pending_upgrade: Option<(UnixStream, String)> = None;
    loop {
        if signals.stopped() {
            shutdown(
                &mut worker,
                &mut retirement,
                &mut listeners,
                &mut pending_upgrade,
                &active.current,
            )?;
            let _ = std::fs::remove_file(&control);
            return Ok(());
        }
        if retirement
            .as_ref()
            .is_some_and(|old| Instant::now() >= old.after)
            && let Some(mut old) = retirement.take()
            && let Err(error) = retire(&mut old.worker)
        {
            eprintln!("could not gracefully retire drained predecessor: {error:#}");
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
        if let Phase::Resuming(id) = worker.phase {
            match worker.channel.poll() {
                Ok(Some(line)) => {
                    if let Some(payload) = line.strip_prefix("ready ")
                        && let Ok(ready) = serde_json::from_str::<crate::addresses::Ready>(payload)
                        && ready.request == id
                        && validate_ready(&ready, &config).is_ok()
                    {
                        worker.addresses = ready.addresses;
                        worker.phase = Phase::Running;
                        eprintln!("Worker {} completed delayed resume.", worker.id());
                    }
                }
                Ok(None) => {}
                Err(error) => eprintln!("waiting for worker recovery: {error:#}"),
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
                handoff_retry = true;
            }
            if handoff_retry && retirement.is_none() {
                handoff_retry = false;
                let prior = active.clone();
                let build = crate::updates::inspect(&prior.current)?.build;
                let replaced = reconcile_installation(&mut active, &config)?;
                worker.addresses = crate::addresses::probe(&config, &build);
                config.required_ready = crate::addresses::ready(&worker.addresses);
                let protected_before = config.required_ready.clone();
                if let Err(error) = quiesce(&mut worker, &prior.current, &config) {
                    eprintln!("listener handoff cancelled; retaining worker: {error:#}");
                    active = prior;
                    atomic_json(&config.active_path(), &active)?;
                    handoff_retry = true;
                    continue;
                }
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
                    Ok(next) => {
                        activate_worker(&mut worker, next, &mut retirement);
                    }
                    Err(error) => {
                        eprintln!("listener handoff failed; resuming previous worker: {error:#}");
                        active = prior;
                        atomic_json(&config.active_path(), &active)?;
                        config.required_ready = protected_before;
                        if let Err(recovery) =
                            recover(&mut worker, &active.current, &mut config, &listeners)
                        {
                            eprintln!("listener handoff recovery pending: {recovery:#}");
                        }
                        handoff_retry = true;
                    }
                }
                if replaced {
                    activated = Instant::now();
                    crashes.clear();
                }
            }
        }
        let incoming =
            if let Some((stream, line)) = pending_upgrade.take_if(|_| retirement.is_none()) {
                Ok((stream, Some(line)))
            } else {
                socket.accept().map(|(stream, _)| (stream, None))
            };
        match incoming {
            Ok((mut stream, buffered)) => {
                stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                let mut line = String::new();
                let received = if let Some(buffered) = buffered {
                    line = buffered;
                    Ok(line.len())
                } else {
                    std::io::BufRead::read_line(
                        &mut std::io::BufReader::new(&stream).take(64 * 1024),
                        &mut line,
                    )
                };
                let mut parsed = received
                    .map_err(anyhow::Error::from)
                    .and_then(|_| serde_json::from_str::<Swap>(&line).map_err(Into::into));
                if serde_json::from_str::<serde_json::Value>(&line)
                    .ok()
                    .and_then(|v| v.get("stop").and_then(|v| v.as_bool()))
                    == Some(true)
                {
                    shutdown(
                        &mut worker,
                        &mut retirement,
                        &mut listeners,
                        &mut pending_upgrade,
                        &active.current,
                    )?;
                    let _ = writeln!(stream, "{{\"stopped\":true}}");
                    let _ = std::fs::remove_file(&control);
                    return Ok(());
                }
                if parsed.is_ok() && retirement.is_some() {
                    if pending_upgrade.is_none() {
                        eprintln!(
                            "Upgrade queued for predecessor retirement; active worker remains serving."
                        );
                        pending_upgrade = Some((stream, line));
                        continue;
                    }
                    parsed = Err(anyhow::anyhow!(
                        "another upgrade is waiting for predecessor retirement; retry after it finishes"
                    ));
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
                    quiesce(&mut worker, &active.current, &config)?;
                    match start(&candidate, &mut config, &listeners, Duration::from_secs(3)) {
                        Ok(mut child) => {
                            let next = Active {
                                current: candidate,
                                previous: Some(active.current.clone()),
                            };
                            if let Err(error) = atomic_json(&config.active_path(), &next) {
                                terminate(&mut child, Duration::ZERO)?;
                                config.required_ready = protected_before_swap;
                                recover(&mut worker, &active.current, &mut config, &listeners)?;
                                return Ok(Swapped {
                                    build: previous_build.clone(),
                                    previous: previous_build.clone(),
                                    rolled_back: true,
                                    reason: Some(format!("activation failed: {error:#}")),
                                    addresses: worker.addresses.clone(),
                                });
                            }
                            activate_worker(&mut worker, child, &mut retirement);
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
                            recover(&mut worker, &active.current, &mut config, &listeners)
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
pub(crate) struct ArcSignal(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl ArcSignal {
    pub(crate) fn new() -> Result<Self> {
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
    pub(crate) fn stopped(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_requires_acceptance_on_protected_addresses_but_not_preexisting_blocked_ones() {
        let loopback = "127.0.0.1:12345";
        let private = "100.64.1.1:12345";
        let mut config = Config {
            addresses: vec![loopback.parse().unwrap(), private.parse().unwrap()],
            requested_addresses: Vec::new(),
            required_ready: vec![private.to_string()],
            handoff_protocol: crate::handoff::VERSION,
            worker_path: None,
            db: PathBuf::from("unused.db"),
            activate_current: false,
            away_after: 60_000,
            forget_after: 120_000,
            reconnect_grace: 0,
        };
        let mut ready = crate::addresses::Ready {
            build: "fixture".to_string(),
            handoff_protocol: crate::handoff::VERSION,
            request: 0,
            accepted: vec![loopback.to_string()],
            addresses: [loopback, private]
                .iter()
                .map(|address| protocol::ListenAddress {
                    address: address.to_string(),
                    state: protocol::AddressState::Ready,
                    reason: None,
                    remote_seen: false,
                })
                .collect(),
        };
        assert!(
            validate_ready(&ready, &config)
                .unwrap_err()
                .to_string()
                .contains("has not accepted")
        );
        ready.accepted.push(private.to_string());
        assert!(validate_ready(&ready, &config).is_ok());
        ready.addresses[1].state = protocol::AddressState::Blocked;
        assert!(validate_ready(&ready, &config).is_err());
        config.required_ready.clear();
        ready.accepted.pop();
        assert!(validate_ready(&ready, &config).is_ok());
        ready.handoff_protocol = 999;
        assert!(validate_ready(&ready, &config).is_err());
    }

    #[test]
    fn late_drain_is_cancelled_by_resume_without_killing_the_database_writer() {
        let root = std::env::temp_dir().join(format!(
            "cam-late-drain-{}-{}",
            std::process::id(),
            crate::hub::now()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let image = b"verified recovery image";
        let hash = crate::updates::sha256(image);
        let directory = root.join("releases").join(&hash);
        std::fs::create_dir_all(&directory).unwrap();
        let snapshot = directory.join("cross-agent-message");
        std::fs::write(&snapshot, image).unwrap();
        let config = Config {
            db: root.join("hub.db"),
            worker_path: Some(root.join("installed")),
            addresses: vec!["127.0.0.1:12345".parse().unwrap()],
            requested_addresses: Vec::new(),
            required_ready: Vec::new(),
            handoff_protocol: crate::handoff::VERSION,
            activate_current: false,
            away_after: 60_000,
            forget_after: 120_000,
            reconnect_grace: 0,
        };
        // This fixture deliberately holds a real SQLite write transaction
        // beyond the supervisor's drain deadline. A forced kill loses its
        // uncommitted marker. Resume is sent while it is still draining.
        let script = r#"
import json, socket, sqlite3, sys, time
wire = socket.socket(fileno=3)
lines = wire.makefile('r')
db = sqlite3.connect(sys.argv[1])
db.execute('create table invariant (value text)')
db.execute('insert into invariant values (\'not lost\')')
def ready(request):
    value = {'build':'fixture', 'handoff_protocol':1, 'request':request,
             'accepted':['127.0.0.1:12345'],
             'addresses':[{'address':'127.0.0.1:12345','state':'ready','reason':None,'remoteSeen':False}]}
    wire.sendall(('ready '+json.dumps(value)+'\n').encode())
ready(0)
for line in lines:
    command=json.loads(line)
    if command['command']=='quiesce':
        time.sleep(4.2)
        db.commit()
        db.close()
        wire.sendall((json.dumps({'state':'drained','id':command['id']})+'\n').encode())
    elif command['command']=='resume':
        db=sqlite3.connect(sys.argv[1])
        ready(command['id'])
    elif command['command']=='retire':
        break
"#;
        let (parent, child_socket) = UnixStream::pair().unwrap();
        let source = child_socket.as_raw_fd();
        let mut command = Command::new("python3");
        command
            .arg("-c")
            .arg(script)
            .arg(&config.db)
            .stdout(Stdio::null());
        // SAFETY: duplicate the owned inherited socket with only libc calls.
        unsafe {
            command.pre_exec(move || {
                let copy = libc::fcntl(source, libc::F_DUPFD_CLOEXEC, 128);
                if copy < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::dup2(copy, 3) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                libc::close(copy);
                Ok(())
            });
        }
        let mut process = command.spawn().unwrap();
        drop(child_socket);
        let pid = process.id();
        let mut channel = crate::handoff::Channel::new(parent).unwrap();
        let ready = wait_ready(
            &mut process,
            &mut channel,
            &config,
            0,
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
        let mut worker = Worker {
            process,
            channel,
            handoff: true,
            sequence: 0,
            phase: Phase::Running,
            addresses: ready.addresses,
        };
        let error = quiesce(&mut worker, &snapshot, &config).unwrap_err();
        assert!(error.to_string().contains("drain timed out"), "{error:#}");
        assert!(worker.phase == Phase::Running);
        assert_eq!(worker.id(), pid);
        assert!(worker.try_wait().unwrap().is_none());
        assert_eq!(
            std::fs::read(config.worker_path.as_ref().unwrap()).unwrap(),
            image
        );
        let db = rusqlite::Connection::open(&config.db).unwrap();
        assert_eq!(
            db.query_row("select value from invariant", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "not lost"
        );
        drop(db);
        drop(worker);
        std::fs::remove_dir_all(root).unwrap();
    }

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
            handoff_protocol: 0,
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
