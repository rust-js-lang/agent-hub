//! Preflight never writes the live database. A swap is requested only afterwards.
use crate::supervisor::{self, Config, Swap};
use anyhow::{Context, Result, bail};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

pub fn self_check(path: &Path) -> Result<()> {
    let mut store = crate::store::Store::open(path)?;
    store.person_key()?;
    let at = crate::hub::now();
    for name in ["self-check-a", "self-check-b"] {
        store.see(
            &protocol::Register {
                name: name.to_string(),
                machine: "preflight".to_string(),
                reconnect: false,
            },
            at,
        )?;
    }
    let message = store.send(
        &protocol::Send {
            from: "self-check-a".to_string(),
            to: "self-check-b".to_string(),
            body: "preflight".to_string(),
            key: None,
        },
        at,
        false,
    )?;
    let inbox = store.read_inbox("self-check-b", 1000, at)?;
    if !inbox.messages.iter().any(|m| m.id == message.id) {
        bail!("self-check did not read back its write");
    }
    store.health()?;
    Ok(())
}

fn check_process(binary: &Path, db: &Path) -> Result<()> {
    let mut child = std::process::Command::new(binary)
        .arg("self-check")
        .arg("--db")
        .arg(db)
        .stdout(std::process::Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                bail!("candidate self-check failed: {status}");
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("candidate self-check timed out");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn preflight(binary: &Path, config: &Config) -> Result<()> {
    if let Some(executable) = &config.worker_path {
        crate::installation::check_writable(executable)?;
    }
    crate::updates::inspect(binary)?;
    let backup = config.db.with_extension(format!(
        "before-{}.db",
        crate::updates::sha256(&std::fs::read(binary)?)
    ));
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    let source = rusqlite::Connection::open_with_flags(
        &config.db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    source.backup("main", &backup, None)?;
    drop(source);
    check_process(binary, &backup)
}

fn candidate(path: &Path, checksum: Option<&str>, config: &Config) -> Result<PathBuf> {
    let bytes = std::fs::read(path)?;
    let actual = crate::updates::sha256(&bytes);
    if let Some(expected) = checksum
        && expected != actual
    {
        bail!("candidate checksum mismatch");
    }
    if path.extension().is_some_and(|x| x == "gz") {
        if checksum.is_none() {
            bail!("an archive requires --sha256 from the release checksums");
        }
        let listing = std::process::Command::new("tar")
            .arg("-tzf")
            .arg(path)
            .output()?;
        if !listing.status.success() {
            bail!("cannot list archive");
        }
        let text = String::from_utf8(listing.stdout)?;
        let member = text
            .lines()
            .find(|name| {
                Path::new(name)
                    .file_name()
                    .is_some_and(|n| n == "cross-agent-message")
            })
            .context("archive has no executable")?;
        let result = std::process::Command::new("tar")
            .arg("-xOzf")
            .arg(path)
            .arg(member)
            .output()?;
        if !result.status.success() {
            bail!("cannot extract executable");
        }
        let directory = config
            .db
            .parent()
            .context("database directory")?
            .join("staging");
        std::fs::create_dir_all(&directory)?;
        let temporary = directory.join(format!("candidate-{}", std::process::id()));
        std::fs::write(&temporary, result.stdout)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o755))?;
        let result = supervisor::snapshot(&temporary, config);
        let _ = std::fs::remove_file(temporary);
        result
    } else {
        supervisor::snapshot(path, config)
    }
}

#[derive(Default)]
struct Probes {
    count: u32,
    failed: u32,
    longest_ms: u64,
}

pub fn run(path: &Path, db: &Path, checksum: Option<&str>) -> Result<()> {
    let config = supervisor::load_config(db)?;
    if config.worker_path.is_none() {
        bail!(
            "this supervisor uses snapshot executable paths; restart it with the new serve --replace before upgrading"
        );
    }
    if config.handoff_protocol != crate::handoff::VERSION {
        bail!(
            "this supervisor cannot coordinate the cooperative handoff; perform the announced cold migration with the new `serve --replace` first"
        );
    }
    let address = config
        .addresses
        .iter()
        .find(|a| a.ip().is_loopback())
        .context("no loopback listener")?;
    let client = crate::client::Client::new(&format!("http://{address}"));
    client.require("live-upgrade")?;
    let candidate = candidate(path, checksum, &config)?;
    preflight(&candidate, &config)?;
    let stopped = Arc::new(AtomicBool::new(false));
    let probes = Arc::new(Mutex::new(Probes::default()));
    let stop = stopped.clone();
    let results = probes.clone();
    let url = format!("http://{address}/api/hub");
    let probe = std::thread::spawn(move || {
        let http: ureq::Agent = ureq::Agent::config_builder()
            .max_idle_connections(0)
            .timeout_global(Some(Duration::from_secs(15)))
            .build()
            .into();
        while !stop.load(Ordering::SeqCst) {
            let start = Instant::now();
            let result = http
                .get(&url)
                .call()
                .and_then(|mut response| response.body_mut().read_to_vec().map(|_| ()));
            let mut stats = results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            stats.count += 1;
            if result.is_err() {
                stats.failed += 1;
            }
            stats.longest_ms = stats.longest_ms.max(start.elapsed().as_millis() as u64);
            drop(stats);
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    let result = supervisor::request(
        &config,
        &Swap {
            sha256: crate::updates::sha256(&std::fs::read(&candidate)?),
            binary: candidate,
        },
    );
    stopped.store(true, Ordering::SeqCst);
    let _ = probe.join();
    let result = result?;
    let stats = probes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if result.deferred {
        let reason = result
            .reason
            .unwrap_or_else(|| "insufficient safe rollback budget; retry later".into());
        let message = format!(
            "Upgrade deferred; the hub remains build {}. {}. {} probe connections, {} failed, longest wait {} ms.",
            result.build, reason, stats.count, stats.failed, stats.longest_ms
        );
        println!("{message}");
        if let Err(error) = announce(&config, &message) {
            eprintln!("could not announce upgrade deferral: {error:#}");
        }
        return Err(supervisor::Deferred(reason).into());
    }
    let ready_addresses: Vec<&str> = result
        .addresses
        .iter()
        .filter(|address| address.state == protocol::AddressState::Ready)
        .map(|address| address.address.as_str())
        .collect();
    let mut message = format!(
        "The hub moved from build {} to {}: {} probe connections, {} failed, longest wait {} ms{}.",
        result.previous,
        result.build,
        stats.count,
        stats.failed,
        stats.longest_ms,
        if result.rolled_back {
            "; rolled back"
        } else {
            ""
        }
    );
    if !ready_addresses.is_empty() {
        message.push_str(&format!(" Reachable at {}.", ready_addresses.join(", ")));
    }
    println!("{message}");
    for address in result
        .addresses
        .iter()
        .filter(|address| address.state != protocol::AddressState::Ready)
    {
        eprintln!(
            "{}: {}",
            address.address,
            address.reason.as_deref().unwrap_or("not ready")
        );
    }
    // The result is sent to the live worker using the private control socket;
    // clients cannot impersonate the reserved hub sender.
    if let Err(error) = announce(&config, &message) {
        eprintln!("could not announce upgrade: {error:#}");
    }
    if result.rolled_back {
        bail!(
            "upgrade rolled back: {}",
            result
                .reason
                .unwrap_or_else(|| "candidate failed".to_string())
        );
    }
    Ok(())
}

pub fn announce(config: &Config, message: &str) -> Result<()> {
    let mut socket =
        std::os::unix::net::UnixStream::connect(config.worker_control()).or_else(|_| {
            std::os::unix::net::UnixStream::connect(config.db.with_extension("worker.sock"))
        })?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    writeln!(socket, "{}", serde_json::json!({"announce":message}))?;
    let mut text = String::new();
    socket.read_to_string(&mut text)?;
    let reply: serde_json::Value =
        serde_json::from_str(&text).context("worker did not confirm the announcement")?;
    if reply.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        bail!("worker did not store the upgrade announcement");
    }
    Ok(())
}
