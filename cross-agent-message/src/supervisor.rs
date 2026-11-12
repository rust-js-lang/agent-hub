//! The stable process owns the listeners. Only its worker opens the live DB.
use crate::handoff::{DRAIN_LIMIT, FIRST_ACCEPT_LIMIT, READY_LIMIT, RETIRE_GRACE};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    /// Negotiate startup markers; older supervisors reject unknown wire lines.
    #[serde(default)]
    pub handoff_startup: bool,
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
    #[serde(default)]
    pub deferred: bool,
    pub reason: Option<String>,
    #[serde(default)]
    pub addresses: Vec<protocol::ListenAddress>,
}

/// A safe, retryable refusal to launch a candidate without its rollback budget.
#[derive(Debug)]
pub struct Deferred(pub String);
impl std::fmt::Display for Deferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Deferred {}

#[derive(Debug)]
struct Exposure {
    started_ns: u64,
    deadline: Instant,
    drain_elapsed: Duration,
}
impl Exposure {
    fn from_start(started_ns: u64) -> Result<Self> {
        // Take Instant first so scheduling delay cannot lengthen the deadline.
        let now = Instant::now();
        let elapsed = crate::handoff::monotonic_ns()?
            .checked_sub(started_ns)
            .context("listener stop timestamp is in the future")?;
        let elapsed = Duration::from_nanos(elapsed);
        Ok(Self {
            started_ns,
            deadline: now + crate::handoff::EXPOSURE_LIMIT.saturating_sub(elapsed),
            drain_elapsed: elapsed,
        })
    }
    fn from_stops(
        stops: &BTreeMap<String, u64>,
        config: &Config,
        fallback_ns: u64,
    ) -> Result<Self> {
        let started_ns = protected(config)
            .map(|address| stops.get(&address).copied().unwrap_or(fallback_ns))
            // The command can precede the listener tasks observing shutdown.
            // Keep that scheduling interval inside the conservative deadline.
            .chain(std::iter::once(fallback_ns))
            .min()
            .unwrap_or(fallback_ns);
        Self::from_start(started_ns)
    }
    fn require_launch(&self) -> Result<()> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining < crate::handoff::LAUNCH_RESERVE {
            let elapsed = crate::handoff::monotonic_ns()?.saturating_sub(self.started_ns);
            return Err(Deferred(format!("upgrade deferred: outgoing drain took {:.3} ms; total exposure {:.3} ms; {} ms remain, but safe startup and rollback need {} ms; retry when requests are quieter", self.drain_elapsed.as_secs_f64() * 1_000.0, elapsed as f64 / 1_000_000.0, remaining.as_millis(), crate::handoff::LAUNCH_RESERVE.as_millis())).into());
        }
        Ok(())
    }
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
    startup: bool,
    observed_accepting: bool,
    stopped_accepting_ns: BTreeMap<String, u64>,
    first_accepted_ns: BTreeMap<String, u64>,
    resume_from_ns: BTreeMap<String, u64>,
    exposure_deadline: Option<Instant>,
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

struct StartLimit {
    until: Instant,
    at_ns: u64,
}

fn log_accept_windows(
    stopped: &BTreeMap<String, u64>,
    accepted: &BTreeMap<String, u64>,
    phase: &str,
) {
    for (address, stopped) in stopped {
        if let Some(first) = accepted.get(address) {
            match first.checked_sub(*stopped) {
                Some(window) => eprintln!(
                    "Handoff {phase} {address}: no accepts for {:.3} ms.",
                    window as f64 / 1_000_000.0
                ),
                None => eprintln!("Handoff {phase} {address}: invalid acceptance timestamp order."),
            }
        }
    }
}

struct Retirement {
    worker: Worker,
    // A rejected candidate cannot retire until rollback has actually resumed.
    after: Option<Instant>,
}

fn activate_worker(
    worker: &mut Worker,
    next: Worker,
    retirement: &mut Option<Retirement>,
) -> std::result::Result<(), Box<Worker>> {
    if retirement.is_some() {
        // Return ownership so the caller can cooperatively roll back instead
        // of panicking or dropping a candidate that has already accepted.
        return Err(Box::new(next));
    }
    log_accept_windows(
        &worker.stopped_accepting_ns,
        &next.first_accepted_ns,
        "upgrade",
    );
    let mut previous = std::mem::replace(worker, next);
    if previous.handoff {
        previous.phase = Phase::Retiring;
        *retirement = Some(Retirement {
            worker: previous,
            after: Some(Instant::now() + RETIRE_GRACE),
        });
    }
    Ok(())
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
    match start(&active.current, config, listeners, READY_LIMIT) {
        Ok(worker) => Ok(worker),
        Err(error) => {
            let previous = active.previous.take().with_context(|| {
                format!("worker startup failed and no previous image is available: {error:#}")
            })?;
            eprintln!("Worker startup failed; restoring the previous installed image: {error:#}");
            active.current = previous;
            atomic_json(&config.active_path(), active)?;
            start(&active.current, config, listeners, READY_LIMIT)
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
    let (mut worker, readiness) = start_candidate(binary, config, listeners, limit, None, None)?;
    if let Err(error) = readiness {
        terminate(&mut worker, Duration::ZERO)?;
        return Err(error);
    }
    Ok(worker)
}

/// Preserve a spawned candidate and its lifecycle channel on readiness failure.
/// The upgrade caller must park it or reap it before reopening the old writer.
fn start_candidate(
    binary: &Path,
    config: &mut Config,
    listeners: &[TcpListener],
    limit: Duration,
    capability: Option<crate::updates::BuildInfo>,
    exposure: Option<&Exposure>,
) -> Result<(Worker, Result<()>)> {
    if let Some(exposure) = exposure {
        exposure.require_launch()?;
    }
    let started = Instant::now();
    let at_ns = crate::handoff::monotonic_ns()?;
    let deadline = started + limit;
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
    // Discover capability before launch: readiness may time out with a startup
    // marker still buffered, or before the child has had a chance to send it.
    let capability = match capability {
        Some(info) => info,
        None => crate::updates::inspect(&executable)?,
    };
    if let Some(exposure) = exposure {
        exposure.require_launch()?;
    }
    let (mut process, stream) = child(&executable, config, listeners)?;
    let channel = match crate::handoff::Channel::new(stream) {
        Ok(channel) => channel,
        Err(error) => {
            terminate(&mut process, Duration::ZERO)?;
            return Err(error);
        }
    };
    let mut worker = Worker {
        process,
        channel,
        handoff: capability.handoff_protocol == crate::handoff::VERSION
            && config.handoff_protocol == crate::handoff::VERSION,
        startup: capability.handoff_startup && config.handoff_startup,
        observed_accepting: !(capability.handoff_startup && config.handoff_startup),
        stopped_accepting_ns: BTreeMap::new(),
        first_accepted_ns: BTreeMap::new(),
        resume_from_ns: BTreeMap::new(),
        exposure_deadline: exposure.map(|exposure| exposure.deadline),
        addresses: Vec::new(),
        sequence: 0,
        phase: Phase::Running,
    };
    let startup = worker.startup;
    let ready = match wait_ready(
        &mut worker,
        config,
        0,
        deadline,
        startup.then_some(StartLimit {
            until: exposure.map_or(started + FIRST_ACCEPT_LIMIT, |exposure| {
                (started + FIRST_ACCEPT_LIMIT)
                    .min(exposure.deadline - crate::handoff::ROLLBACK_RESERVE)
            }),
            at_ns,
        }),
    ) {
        Ok(ready) => ready,
        Err(error) => return Ok((worker, Err(error))),
    };
    worker.first_accepted_ns = ready.first_accepted_ns;
    worker.addresses = ready.addresses;
    config.required_ready = crate::addresses::ready(&worker.addresses);
    eprintln!(
        "Worker {} is ready at {} ({} ms).",
        worker.id(),
        executable.display(),
        started.elapsed().as_millis()
    );
    Ok((worker, Ok(())))
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
    worker: &mut Worker,
    config: &Config,
    request: u64,
    deadline: Instant,
    startup: Option<StartLimit>,
) -> Result<crate::addresses::Ready> {
    let mut first_accepted = BTreeMap::new();
    loop {
        if Instant::now() >= deadline {
            bail!("worker readiness timed out");
        }
        if let Some(line) = worker.channel.poll()? {
            if let Some(payload) = line.strip_prefix("accepted ") {
                if !config.handoff_startup {
                    bail!("worker sent unnegotiated acceptance telemetry");
                }
                let accepted: crate::handoff::Acceptance = serde_json::from_str(payload)?;
                if accepted.request == request {
                    worker.observed_accepting = true;
                    worker
                        .first_accepted_ns
                        .entry(accepted.address.clone())
                        .or_insert(accepted.at_ns);
                    first_accepted
                        .entry(accepted.address)
                        .or_insert(accepted.at_ns);
                }
                continue;
            }
            if let Some(id) = line.strip_prefix("accepting ") {
                if !config.handoff_startup || config.handoff_protocol != crate::handoff::VERSION {
                    bail!("worker sent an unnegotiated startup marker");
                }
                if id.trim().parse::<u64>()? == request {
                    worker.handoff = true;
                    worker.observed_accepting = true;
                }
                continue;
            }
            if let Some(payload) = line.strip_prefix("ready ") {
                let mut ready = if let Ok(ready) =
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
                        first_accepted_ns: BTreeMap::new(),
                    }
                } else {
                    bail!("invalid resumed worker readiness");
                };
                if ready.request != request {
                    continue;
                }
                worker.observed_accepting = true;
                worker.handoff = ready.handoff_protocol == crate::handoff::VERSION
                    && config.handoff_protocol == crate::handoff::VERSION;
                worker.first_accepted_ns = ready.first_accepted_ns.clone();
                validate_ready(&ready, config)?;
                if let Some(limit) = &startup {
                    for address in protected(config) {
                        let first = ready
                            .first_accepted_ns
                            .get(&address)
                            .context("worker omitted first acceptance timing")?;
                        let elapsed = first
                            .checked_sub(limit.at_ns)
                            .context("worker acceptance predates startup")?;
                        if elapsed > FIRST_ACCEPT_LIMIT.as_nanos() as u64 {
                            bail!(
                                "worker first acceptance on {address} exceeded its startup budget"
                            );
                        }
                    }
                }
                if ready.first_accepted_ns.is_empty() {
                    ready.first_accepted_ns = first_accepted;
                }
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
        // Consume buffered telemetry before a deadline decision: the parent
        // itself may have been scheduled after the worker accepted on time.
        if let Some(limit) = &startup
            && Instant::now() >= limit.until
            && protected(config).any(|address| !first_accepted.contains_key(&address))
        {
            bail!("worker first acceptance timed out on a required address");
        }
        if let Some(status) = worker.try_wait()? {
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
    let started = Instant::now();
    let at_ns = crate::handoff::monotonic_ns()?;
    restore_image(image, config)?;
    let id = worker.next_request();
    worker.phase = Phase::Resuming(id);
    worker.first_accepted_ns.clear();
    let deadline = worker
        .exposure_deadline
        .map_or(started + READY_LIMIT, |end| end.min(started + READY_LIMIT));
    worker
        .channel
        .send(&crate::handoff::Command::Resume { id }, deadline)?;
    let startup = worker.startup;
    let worker_deadline = deadline;
    let ready = wait_ready(
        worker,
        config,
        id,
        deadline,
        startup.then_some(StartLimit {
            until: worker_deadline.min(started + FIRST_ACCEPT_LIMIT),
            at_ns,
        }),
    )?;
    worker.first_accepted_ns = ready.first_accepted_ns;
    log_accept_windows(
        &worker.resume_from_ns,
        &worker.first_accepted_ns,
        "rollback",
    );
    worker.resume_from_ns.clear();
    worker.addresses = ready.addresses;
    worker.phase = Phase::Running;
    worker.exposure_deadline = None;
    eprintln!(
        "Worker {} resumed on the same process after rollback ({} ms).",
        worker.id(),
        started.elapsed().as_millis()
    );
    Ok(())
}

/// A late drain never licenses another database writer. On any failure,
/// request resume and keep owning the original child instead of killing it.
fn quiesce(worker: &mut Worker, image: &Path, config: &Config) -> Result<Exposure> {
    let started_ns = crate::handoff::monotonic_ns()?;
    let fallback = Exposure::from_start(started_ns)?;
    worker.exposure_deadline = Some(fallback.deadline);
    if !worker.handoff {
        eprintln!(
            "Legacy outgoing worker has no cooperative handoff; this transition may interrupt a remote connection."
        );
        terminate(worker, Duration::from_secs(4))?;
        return Exposure::from_start(started_ns);
    }
    if let Err(error) = drain(worker) {
        resume(worker, image, config)
            .with_context(|| format!("{error:#}; old worker resume did not finish"))?;
        return Err(error);
    }
    let exposure = match Exposure::from_stops(&worker.stopped_accepting_ns, config, started_ns) {
        Ok(exposure) => exposure,
        Err(error) => {
            resume(worker, image, config).with_context(|| {
                format!("cannot measure drained worker exposure: {error:#}; resume did not finish")
            })?;
            return Err(error);
        }
    };
    worker.exposure_deadline = Some(exposure.deadline);
    Ok(exposure)
}

/// A rejected candidate must drain without automatically resuming itself.
fn drain(worker: &mut Worker) -> Result<()> {
    if worker.phase != Phase::Running {
        bail!("worker handoff is still awaiting recovery");
    }
    let id = worker.next_request();
    let started = Instant::now();
    let early = worker.startup && !worker.observed_accepting;
    let mut deadline = started
        + if early {
            crate::handoff::EARLY_DRAIN_LIMIT
        } else {
            DRAIN_LIMIT
        };
    worker.stopped_accepting_ns.clear();
    worker.phase = Phase::Quiescing;
    worker
        .channel
        .send(&crate::handoff::Command::Quiesce { id }, deadline)?;
    loop {
        if let Some(line) = worker.channel.poll()? {
            match serde_json::from_str::<crate::handoff::Reply>(&line) {
                Ok(crate::handoff::Reply::Drained {
                    id: reply,
                    stopped_accepting_ns,
                    first_accepted_ns,
                }) if reply == id => {
                    worker.stopped_accepting_ns = stopped_accepting_ns;
                    worker.first_accepted_ns = first_accepted_ns;
                    worker.phase = Phase::Parked;
                    eprintln!(
                        "Worker {} acknowledged drained; database is closed and PID retained ({} ms).",
                        worker.id(),
                        started.elapsed().as_millis()
                    );
                    return Ok(());
                }
                Ok(_) => {}
                Err(_)
                    if line.starts_with("ready ")
                        || line.starts_with("accepting ")
                        || line.starts_with("accepted ") =>
                {
                    worker.observed_accepting = true;
                    deadline = started + DRAIN_LIMIT;
                }
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
        *worker = start(image, config, listeners, READY_LIMIT)?;
        Ok(())
    }
}

fn rollback_candidate(
    mut candidate: Worker,
    worker: &mut Worker,
    image: &Path,
    config: &mut Config,
    listeners: &[TcpListener],
    retirement: &mut Option<Retirement>,
) -> Result<()> {
    if let Some(mut pending) = retirement.take() {
        // The serialization guard normally makes this unreachable. If it is
        // violated, keep the ready candidate serving while the existing grace
        // finishes, then use the same cooperative rollback as any other refusal.
        let after = pending
            .after
            .unwrap_or_else(|| Instant::now() + RETIRE_GRACE);
        while Instant::now() < after {
            std::thread::sleep(Duration::from_millis(10));
        }
        if let Err(error) = retire(&mut pending.worker) {
            eprintln!("retirement before controlled activation rollback failed: {error:#}");
        }
    }
    let rollback_started_ns = crate::handoff::monotonic_ns()?;
    worker.resume_from_ns = worker.stopped_accepting_ns.clone();
    if candidate.handoff && candidate.try_wait()?.is_none() {
        match drain(&mut candidate) {
            Ok(()) => {
                // An address the candidate never accepted still spans the
                // original worker's whole stop -> resume window.
                for address in candidate.first_accepted_ns.keys() {
                    if let Some(stopped) = candidate.stopped_accepting_ns.get(address) {
                        worker.resume_from_ns.insert(address.clone(), *stopped);
                    } else {
                        worker.resume_from_ns.remove(address);
                    }
                }
                let resumed_deadline = match Exposure::from_stops(
                    &worker.resume_from_ns,
                    config,
                    rollback_started_ns,
                ) {
                    Ok(exposure) => exposure.deadline,
                    Err(error) => {
                        // Diagnostic failure must not drop a parked candidate
                        // before ownership has moved into retirement/recovery.
                        eprintln!(
                            "cannot measure rollback exposure; retaining the existing deadline: {error:#}"
                        );
                        worker
                            .exposure_deadline
                            .unwrap_or_else(|| Instant::now() + FIRST_ACCEPT_LIMIT)
                    }
                };
                let partial = protected(config)
                    .any(|address| !candidate.first_accepted_ns.contains_key(&address));
                // A still-uncovered address must never gain a later deadline
                // just because a different address was accepted by the candidate.
                worker.exposure_deadline = Some(if partial {
                    worker
                        .exposure_deadline
                        .map_or(resumed_deadline, |previous| previous.min(resumed_deadline))
                } else {
                    resumed_deadline
                });
                candidate.phase = Phase::Retiring;
                // Retain ownership even if resume times out: the main loop
                // starts this grace only after the active worker proves ready.
                *retirement = Some(Retirement {
                    worker: candidate,
                    after: None,
                });
                recover(worker, image, config, listeners)?;
                if let Some(old) = retirement.as_mut() {
                    old.after = Some(Instant::now() + RETIRE_GRACE);
                }
                return Ok(());
            }
            Err(error) => eprintln!("Failed candidate would not park: {error:#}"),
        }
    }
    // A writer that did not acknowledge drain must be reaped before rollback.
    terminate(&mut candidate, Duration::ZERO)?;
    while let Ok(Some(line)) = candidate.channel.poll() {
        if line.starts_with("accepting ")
            || line.starts_with("accepted ")
            || line.starts_with("ready ")
        {
            candidate.observed_accepting = true;
        }
    }
    if !candidate.startup || candidate.observed_accepting {
        worker.resume_from_ns.clear(); // abrupt exit has no trustworthy stop timestamp
        eprintln!(
            "Failed candidate cannot cooperate after possibly accepting; remote connections may be interrupted during rollback."
        );
    } else {
        eprintln!("Rejected candidate never entered acceptance; recovering the previous worker.");
    }
    recover(worker, image, config, listeners)
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
            deferred: false,
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
    socket.set_read_timeout(Some(crate::handoff::CONTROL_LIMIT))?;
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
    config.handoff_startup = true;
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
        if let Some(old) = retirement.as_mut()
            && old.after.is_none()
            && worker.phase == Phase::Running
        {
            old.after = Some(Instant::now() + RETIRE_GRACE);
        }
        if retirement
            .as_ref()
            .is_some_and(|old| old.after.is_some_and(|after| Instant::now() >= after))
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
                start(&active.current, &mut config, &listeners, READY_LIMIT)
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
                        worker.first_accepted_ns = ready.first_accepted_ns;
                        log_accept_windows(
                            &worker.resume_from_ns,
                            &worker.first_accepted_ns,
                            "rollback",
                        );
                        worker.resume_from_ns.clear();
                        worker.addresses = ready.addresses;
                        worker.phase = Phase::Running;
                        worker.exposure_deadline = None;
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
                // Inspect the final file while the current worker still serves.
                let capability = crate::updates::inspect(
                    config
                        .worker_path
                        .as_deref()
                        .context("stable worker path missing")?,
                )?;
                let exposure = match quiesce(&mut worker, &prior.current, &config) {
                    Ok(exposure) => exposure,
                    Err(error) => {
                        eprintln!("listener handoff cancelled; retaining worker: {error:#}");
                        active = prior;
                        atomic_json(&config.active_path(), &active)?;
                        handoff_retry = true;
                        continue;
                    }
                };
                let restart = start_candidate(
                    &active.current,
                    &mut config,
                    &listeners,
                    READY_LIMIT,
                    Some(capability),
                    worker.handoff.then_some(&exposure),
                );
                match restart {
                    Ok((next, Ok(()))) => {
                        if let Err(candidate) = activate_worker(&mut worker, next, &mut retirement)
                        {
                            eprintln!(
                                "listener activation cancelled while predecessor retirement is pending"
                            );
                            active = prior;
                            if let Err(error) = atomic_json(&config.active_path(), &active) {
                                eprintln!(
                                    "could not record previous image; continuing live rollback: {error:#}"
                                );
                            }
                            config.required_ready = protected_before;
                            if let Err(error) = rollback_candidate(
                                *candidate,
                                &mut worker,
                                &active.current,
                                &mut config,
                                &listeners,
                                &mut retirement,
                            ) {
                                eprintln!("listener activation recovery pending: {error:#}");
                            }
                            handoff_retry = true;
                        }
                    }
                    Ok((candidate, Err(error))) => {
                        eprintln!(
                            "listener handoff failed; parking candidate before rollback: {error:#}"
                        );
                        active = prior;
                        if let Err(error) = atomic_json(&config.active_path(), &active) {
                            eprintln!(
                                "could not record previous image; continuing live rollback: {error:#}"
                            );
                        }
                        config.required_ready = protected_before;
                        if let Err(recovery) = rollback_candidate(
                            candidate,
                            &mut worker,
                            &active.current,
                            &mut config,
                            &listeners,
                            &mut retirement,
                        ) {
                            eprintln!("listener handoff recovery pending: {recovery:#}");
                        }
                        handoff_retry = true;
                    }
                    Err(error) => {
                        let outcome = if error.is::<Deferred>() {
                            "deferred"
                        } else {
                            "failed"
                        };
                        eprintln!(
                            "listener handoff {outcome}; resuming previous worker: {error:#}"
                        );
                        active = prior;
                        if let Err(error) = atomic_json(&config.active_path(), &active) {
                            eprintln!(
                                "could not record previous image; continuing live rollback: {error:#}"
                            );
                        }
                        config.required_ready = protected_before;
                        worker.resume_from_ns = worker.stopped_accepting_ns.clone();
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
                // BSD accept can inherit O_NONBLOCK from the listening socket.
                // A local client may send JSON and newline in separate writes;
                // wait for the bounded complete frame instead of refusing EAGAIN.
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                stream.set_write_timeout(Some(Duration::from_secs(1)))?;
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
                    .context("cannot read complete supervisor control request")
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
                    let prepared = (|| -> Result<crate::updates::BuildInfo> {
                        crate::installation::activate_verified(&candidate, executable, &swap.sha256)?;
                        let info = crate::updates::inspect(executable)?;
                        if crate::updates::sha256(&std::fs::read(executable)?) != swap.sha256 { bail!("installed image changed during final-path preflight"); }
                        Ok(info)
                    })();
                    let capability = match prepared {
                        Ok(info) => info,
                        Err(error) => {
                        let previous_sha = active.current.parent().and_then(|parent| parent.file_name()).and_then(|name| name.to_str()).context("previous image checksum missing")?;
                        if let Err(restore) = crate::installation::activate_verified(&active.current, executable, previous_sha) {
                            eprintln!("could not restore installed image; existing worker is still serving: {restore:#}");
                        }
                        return Err(error.context("candidate final-path preflight failed; existing worker retained"));
                        }
                    };
                    // Probe before draining: already-pending/blocked addresses do
                    // not prevent an upgrade, but every ready address is protected.
                    worker.addresses = crate::addresses::probe(&config, &previous_build);
                    config.required_ready = crate::addresses::ready(&worker.addresses);
                    let protected_before_swap = config.required_ready.clone();
                    let exposure = quiesce(&mut worker, &active.current, &config)?;
                    match start_candidate(&candidate, &mut config, &listeners, READY_LIMIT, Some(capability), worker.handoff.then_some(&exposure)) {
                        Ok((child, readiness)) => {
                            let next = Active {
                                current: candidate,
                                previous: Some(active.current.clone()),
                            };
                            if let Err(error) = readiness.and_then(|()| atomic_json(&config.active_path(), &next)) {
                                config.required_ready = protected_before_swap;
                                rollback_candidate(child, &mut worker, &active.current, &mut config, &listeners, &mut retirement)?;
                                return Ok(Swapped {
                                    build: previous_build.clone(),
                                    previous: previous_build.clone(),
                                    rolled_back: true,
        deferred: false,
                                    reason: Some(format!("candidate readiness or activation failed: {error:#}")),
                                    addresses: worker.addresses.clone(),
                                });
                            }
                            if let Err(candidate) = activate_worker(&mut worker, child, &mut retirement) {
                                config.required_ready = protected_before_swap;
                                let recorded = atomic_json(&config.active_path(), &active);
                                rollback_candidate(*candidate, &mut worker, &active.current, &mut config, &listeners, &mut retirement)?;
                                recorded.context("could not record previous image after activation refusal")?;
                                bail!("activation cancelled while predecessor retirement is pending; previous worker resumed");
                            }
                            active = next;
                            activated = Instant::now();
                            crashes.clear();
                            Ok(Swapped {
                                build: info.build,
                                previous: previous_build.clone(),
                                rolled_back: false,
        deferred: false,
                                reason: None,
                                addresses: worker.addresses.clone(),
                            })
                        }
                        Err(error) => {
                            let deferred = error.is::<Deferred>();
                            worker.resume_from_ns = worker.stopped_accepting_ns.clone();
                            recover(&mut worker, &active.current, &mut config, &listeners)
                                .context("rollback worker did not become ready")?;
                            Ok(Swapped {
                                build: previous_build.clone(),
                                previous: previous_build.clone(),
                                rolled_back: true, // older clients still receive a safe refusal
                                deferred,
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
                    deferred: false,
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
            handoff_startup: true,
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
            first_accepted_ns: BTreeMap::new(),
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
    fn first_acceptance_is_per_address_and_independent_of_readiness_completion() {
        let loopback = "127.0.0.1:12345";
        let private = "127.0.0.2:12345";
        let config = Config {
            addresses: vec![loopback.parse().unwrap(), private.parse().unwrap()],
            requested_addresses: Vec::new(),
            required_ready: vec![private.into()],
            db: PathBuf::from("unused.db"),
            worker_path: None,
            handoff_protocol: crate::handoff::VERSION,
            handoff_startup: true,
            activate_current: false,
            away_after: 60_000,
            forget_after: 120_000,
            reconnect_grace: 0,
        };
        for all_addresses in [true, false] {
            let (parent, child_socket) = UnixStream::pair().unwrap();
            let source = child_socket.as_raw_fd();
            let mut command = Command::new("python3");
            command
                .arg("-c")
                .arg(
                    r#"
import json, socket, sys, time
wire = socket.socket(fileno=3)
addresses = ['127.0.0.1:12345', '127.0.0.2:12345']
if sys.argv[1] == 'false': addresses.pop()
first = {address: time.clock_gettime_ns(time.CLOCK_MONOTONIC) for address in addresses}
wire.sendall(b'accepting 0\n')
for address, at in first.items():
    wire.sendall(('accepted '+json.dumps({'request':0,'address':address,'at_ns':at})+'\n').encode())
time.sleep(float(sys.argv[2])+0.2)
ready = {'build':'fixture','request':0,'handoff_protocol':1,'accepted':addresses,
         'first_accepted_ns':first,
         'addresses':[{'address':address,'state':'ready'} for address in addresses]}
wire.sendall(('ready '+json.dumps(ready)+'\n').encode())
wire.recv(1)
"#,
                )
                .arg(all_addresses.to_string())
                .arg(FIRST_ACCEPT_LIMIT.as_secs_f64().to_string());
            // SAFETY: only descriptor duplication/close runs between fork and exec.
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
            let began = Instant::now();
            let at_ns = crate::handoff::monotonic_ns().unwrap();
            let mut worker = Worker {
                process: command.spawn().unwrap(),
                channel: crate::handoff::Channel::new(parent).unwrap(),
                handoff: true,
                startup: true,
                observed_accepting: false,
                stopped_accepting_ns: BTreeMap::new(),
                first_accepted_ns: BTreeMap::new(),
                resume_from_ns: BTreeMap::new(),
                exposure_deadline: None,
                sequence: 0,
                phase: Phase::Running,
                addresses: Vec::new(),
            };
            drop(child_socket);
            let result = wait_ready(
                &mut worker,
                &config,
                0,
                began + READY_LIMIT,
                Some(StartLimit {
                    until: began + FIRST_ACCEPT_LIMIT,
                    at_ns,
                }),
            );
            if all_addresses {
                assert!(result.is_ok(), "{result:?}");
                assert!(
                    began.elapsed() >= FIRST_ACCEPT_LIMIT,
                    "readiness should finish after its shorter acceptance deadline"
                );
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("first acceptance timed out")
                );
                assert!(
                    began.elapsed() < READY_LIMIT,
                    "one ready address cannot mask a missing protected address"
                );
            }
        }
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
            handoff_startup: true,
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
        time.sleep(float(sys.argv[2]) + 0.2)
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
            .arg(DRAIN_LIMIT.as_secs_f64().to_string())
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
        let process = command.spawn().unwrap();
        drop(child_socket);
        let pid = process.id();
        let mut worker = Worker {
            process,
            channel: crate::handoff::Channel::new(parent).unwrap(),
            handoff: true,
            startup: false,
            observed_accepting: true,
            stopped_accepting_ns: BTreeMap::new(),
            first_accepted_ns: BTreeMap::new(),
            resume_from_ns: BTreeMap::new(),
            exposure_deadline: None,
            sequence: 0,
            phase: Phase::Running,
            addresses: Vec::new(),
        };
        let ready =
            wait_ready(&mut worker, &config, 0, Instant::now() + READY_LIMIT, None).unwrap();
        worker.addresses = ready.addresses;
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
            handoff_startup: false,
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
