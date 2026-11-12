//! Real-process upgrade invariants. Each test owns its listeners and database.
use cross_agent_message::client::Client;
use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::os::unix::{fs::PermissionsExt, process::CommandExt};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

struct Running {
    // Timing budgets measure one upgrade, not competing debug workers on a
    // small CI machine. Keep the real-process scenarios isolated.
    _serial: std::sync::MutexGuard<'static, ()>,
    root: PathBuf,
    process: Child,
    url: String,
    binary: PathBuf,
    source: PathBuf,
    db: PathBuf,
}
fn wait(mut condition: impl FnMut() -> bool, seconds: u64) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("condition not met within {seconds} seconds");
}
impl Running {
    fn new() -> Self {
        Self::database(false)
    }
    fn database(long_path: bool) -> Self {
        Self::configured(long_path, &[])
    }
    fn configured(long_path: bool, extra: &[&str]) -> Self {
        static SERIAL: Mutex<()> = Mutex::new(());
        let serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = PathBuf::from(format!(
            "/tmp/cam-supervisor-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let db = if long_path {
            root.join("long-".repeat(20))
                .join("path-".repeat(20))
                .join("hub.db")
        } else {
            root.join("hub.db")
        };
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let source = PathBuf::from(env!("CARGO_BIN_EXE_cross-agent-message"));
        let binary = root.join("cross-agent-message");
        std::fs::copy(&source, &binary).unwrap();
        let log = std::fs::File::create(root.join("log")).unwrap();
        let process = Command::new(&binary)
            .args(["serve", "--db"])
            .arg(&db)
            .args(["--port", &port.to_string()])
            .args(extra.iter().flat_map(|address| ["--listen", *address]))
            .env("CROSS_AGENT_STATE_DIR", &root)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0)
            .spawn()
            .unwrap();
        let result = Self {
            _serial: serial,
            root,
            process,
            url: format!("http://127.0.0.1:{port}"),
            binary,
            source,
            db,
        };
        wait(
            || {
                result.db.with_extension("active.json").exists()
                    && result.client().info().is_ok_and(|info| info.supervised)
            },
            10,
        );
        result
    }
    fn client(&self) -> Client {
        Client::new(&self.url)
    }
    fn upgrade(&self, binary: &std::path::Path) -> std::process::Output {
        Command::new(&self.binary)
            .arg("upgrade")
            .arg(binary)
            .arg("--db")
            .arg(&self.db)
            .env("CROSS_AGENT_STATE_DIR", &self.root)
            .output()
            .unwrap()
    }
    fn candidate(&self, action: &str) -> PathBuf {
        let path = self.root.join(format!(
            "candidate-{}",
            &cross_agent_message::updates::sha256(action.as_bytes())[..12]
        ));
        // Only fixed fixture actions enter this trusted test script.
        let text = format!(
            "#!/bin/sh\nif [ \"$1\" = worker ]; then {action}; fi\nexec '{}' \"$@\"\n",
            self.source.display()
        );
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }
    fn worker_pid(&self) -> i32 {
        std::fs::read_to_string(self.root.join("log"))
            .unwrap()
            .lines()
            .rev()
            .find_map(|line| {
                line.strip_prefix("Worker ")
                    .and_then(|tail| tail.split_whitespace().next())
                    .and_then(|pid| pid.parse().ok())
            })
            .unwrap()
    }
    fn restart(&mut self) {
        assert!(self.process.try_wait().unwrap().is_some());
        let before = self.worker_pid();
        let log = std::fs::OpenOptions::new()
            .append(true)
            .open(self.root.join("log"))
            .unwrap();
        let port = self.url.rsplit(':').next().unwrap();
        self.process = Command::new(&self.binary)
            .args(["serve", "--db"])
            .arg(&self.db)
            .args(["--port", port])
            .env("CROSS_AGENT_STATE_DIR", &self.root)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0)
            .spawn()
            .unwrap();
        wait(
            || {
                self.worker_pid() != before
                    && self.client().info().is_ok_and(|info| info.supervised)
            },
            10,
        );
    }
    fn assert_worker_path(&self) {
        let pid = self.worker_pid();
        #[cfg(target_os = "macos")]
        {
            let output = Command::new("ps")
                .args(["-p", &pid.to_string(), "-o", "comm="])
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap().trim(),
                self.binary.to_str().unwrap()
            );
        }
        #[cfg(not(target_os = "macos"))]
        assert_eq!(
            std::fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
            self.binary
        );
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        // SAFETY: every process in this fresh process group belongs to the fixture.
        unsafe {
            libc::kill(-(self.process.id() as libc::pid_t), libc::SIGTERM);
        }
        let end = Instant::now() + Duration::from_secs(5);
        while Instant::now() < end {
            if self.process.try_wait().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if self.process.try_wait().unwrap().is_none() {
            unsafe {
                libc::kill(-(self.process.id() as libc::pid_t), libc::SIGKILL);
            }
            let _ = self.process.wait();
        }
        if std::thread::panicking() {
            eprintln!(
                "supervisor fixture log:\n{}",
                std::fs::read_to_string(self.root.join("log")).unwrap_or_default()
            );
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct Watcher(Child);
impl Drop for Watcher {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Availability {
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<(u32, u32, u128)>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Availability {
    fn start(url: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(Mutex::new((0, 0, 0)));
        let flag = stop.clone();
        let result = stats.clone();
        let url = format!("{url}/api/hub");
        let thread = std::thread::spawn(move || {
            let http: ureq::Agent = ureq::Agent::config_builder()
                .max_idle_connections(0)
                .timeout_global(Some(Duration::from_secs(12)))
                .build()
                .into();
            while !flag.load(Ordering::SeqCst) {
                let at = Instant::now();
                let response = http
                    .get(&url)
                    .call()
                    .and_then(|mut response| response.body_mut().read_to_vec());
                if let Err(error) = &response {
                    eprintln!(
                        "availability probe failed after {} ms: {error:?}",
                        at.elapsed().as_millis()
                    );
                }
                let mut stats = result.lock().unwrap();
                stats.0 += 1;
                stats.1 += u32::from(response.is_err());
                stats.2 = stats.2.max(at.elapsed().as_millis());
                drop(stats);
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        Self {
            stop,
            stats,
            thread: Some(thread),
        }
    }
    fn finish(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.thread.take().unwrap().join().unwrap();
        let stats = *self.stats.lock().unwrap();
        assert!(stats.0 > 0);
        assert_eq!(stats.1, 0, "availability probe saw failures: {stats:?}");
        assert!(stats.2 < 10_000, "handoff exceeded its budget: {stats:?}");
    }
}
impl Drop for Availability {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn activation_returns_during_grace_and_the_next_upgrade_waits_without_stopping_service() {
    let running = Running::new();
    let client = running.client();
    client.register("grace-a").unwrap();
    client.register("grace-b").unwrap();
    let original = running.worker_pid();
    let probe = Availability::start(&running.url);
    let first = running.upgrade(&running.binary);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let active = running.worker_pid();
    assert_ne!(active, original);
    let children = || {
        let output = Command::new("pgrep")
            .args(["-P", &running.process.id().to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>()
    };
    assert!(
        children().contains(&original),
        "activation must report before retiring the predecessor"
    );
    assert!(children().contains(&active));
    let files = Command::new("lsof")
        .args(["-a", "-p", &original.to_string(), "-Fn"])
        .output()
        .unwrap();
    assert!(files.status.success());
    assert!(
        !String::from_utf8_lossy(&files.stdout)
            .lines()
            .any(|line| line.starts_with(&format!("n{}", running.db.display()))),
        "predecessor must not retain SQLite handles during grace"
    );
    std::thread::scope(|scope| {
        let second = scope.spawn(|| running.upgrade(&running.binary));
        wait(
            || {
                std::fs::read_to_string(running.root.join("log"))
                    .unwrap()
                    .contains("Upgrade queued for predecessor retirement")
            },
            3,
        );
        assert!(!second.is_finished());
        client
            .send(
                "grace-a",
                "grace-b",
                "service continues while retirement is pending",
            )
            .unwrap();
        assert_eq!(
            running.worker_pid(),
            active,
            "queued upgrade must not overlap the first retirement"
        );
        let result = second.join().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    });
    assert!(!children().contains(&original));
    assert_eq!(
        client
            .read_inbox("grace-b", 10)
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.from == "grace-a")
            .count(),
        1
    );
    probe.finish();
}

#[test]
fn successful_swap_serves_new_connections_and_keeps_acknowledged_mail_and_holds() {
    let running = Running::new();
    let client = running.client();
    client.register("sender").unwrap();
    client.register("reader").unwrap();
    let payload = serde_json::json!({"from":"sender","to":"reader","body":"nonce survives the swap","future_field":true});
    let once: protocol::Message = ureq::post(format!("{}/api/messages", running.url))
        .header(protocol::WRITE_NONCE_HEADER, "across-process-upgrade")
        .send_json(&payload)
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    let hold = client.take("check", "sender", 30).unwrap().hold;
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let url = running.url.clone();
    let sender = std::thread::spawn(move || {
        let client = Client::new(&url);
        let mut ids = Vec::new();
        let mut n = 0;
        while !stopped.load(Ordering::SeqCst) || n < 3 {
            ids.push(
                client
                    .send("sender", "reader", &format!("number {n}"))
                    .unwrap()
                    .id,
            );
            n += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
        ids
    });
    let probe = Availability::start(&running.url);
    let old = client.info().unwrap().started;
    let result = running.upgrade(&running.binary);
    stop.store(true, Ordering::SeqCst);
    let acknowledged = sender.join().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_ne!(client.info().unwrap().started, old);
    probe.finish();
    let replayed: protocol::Message = ureq::post(format!("{}/api/messages", running.url))
        .header(protocol::WRITE_NONCE_HEADER, "across-process-upgrade")
        .send_json(&payload)
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(
        once.id, replayed.id,
        "the receipt must survive the worker process swap"
    );
    let messages = client.read_inbox("reader", 100_000).unwrap().messages;
    let received: Vec<u64> = messages
        .iter()
        .filter(|m| m.from == "sender")
        .map(|m| m.id)
        .collect();
    let expected: Vec<u64> = std::iter::once(once.id).chain(acknowledged).collect();
    assert_eq!(received, expected);
    let current = client.holds().unwrap();
    assert_eq!(current[0].holder, hold.holder);
    assert_eq!(current[0].since, hold.since);
    assert!(current[0].until >= hold.until);
    assert!(messages.iter().any(|m| m.from == "hub" && m.urgent));
}

#[test]
fn failed_and_hung_candidates_roll_back_on_the_same_live_database() {
    let running = Running::new();
    let client = running.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let message = client
        .send("a", "b", "must survive both rollbacks")
        .unwrap();
    for action in [
        "exit 17",
        "exec sleep 60",
        "printf 'invalid readiness\\n' >&4; exec sleep 60",
    ] {
        eprintln!("testing rollback action: {action}");
        let probe = Availability::start(&running.url);
        let start = Instant::now();
        let result = running.upgrade(&running.candidate(action));
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("rolled back"));
        assert!(start.elapsed() < Duration::from_secs(15));
        probe.finish();
        assert_eq!(
            client.read_message("b", message.id).unwrap().body,
            "must survive both rollbacks"
        );
        assert!(client.info().unwrap().supervised);
        let children = Command::new("pgrep")
            .args(["-P", &running.process.id().to_string()])
            .output()
            .unwrap();
        assert!(children.status.success());
        assert_eq!(
            String::from_utf8_lossy(&children.stdout).lines().count(),
            1,
            "a rejected candidate must be reaped before rollback"
        );
    }
}

#[test]
fn watch_reconnects_and_already_away_agents_stay_away_across_the_swap() {
    let running = Running::new();
    let client = running.client();
    client.register("reader").unwrap();
    client.register("away").unwrap();
    // The fixture alone owns this DB; make one known agent stale without waiting an hour.
    let db = rusqlite::Connection::open(running.root.join("hub.db")).unwrap();
    db.execute(
        "UPDATE agents SET last_seen=?1 WHERE name='away'",
        [cross_agent_message::hub::now() - 7_200_000],
    )
    .unwrap();
    drop(db);
    let mut watcher = Watcher(
        Command::new(&running.binary)
            .args(["--hub", &running.url, "watch", "--as", "reader"])
            .env("CROSS_AGENT_STATE_DIR", &running.root)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = watcher.0.stdout.take().unwrap();
    let lines = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = lines.clone();
    let reading = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            captured.lock().unwrap().push(line.unwrap());
        }
    });
    wait(
        || {
            client
                .agents()
                .unwrap()
                .iter()
                .any(|a| a.name == "reader" && a.watching)
        },
        5,
    );
    assert!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "away")
            .unwrap()
            .away
    );
    let result = running.upgrade(&running.binary);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "away")
            .unwrap()
            .away
    );
    assert!(
        client
            .agents()
            .unwrap()
            .iter()
            .find(|a| a.name == "reader")
            .unwrap()
            .watching
    );
    wait(
        || {
            lines
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains("from hub"))
        },
        5,
    );
    assert!(watcher.0.try_wait().unwrap().is_none());
    watcher.0.kill().unwrap();
    watcher.0.wait().unwrap();
    reading.join().unwrap();
}

#[test]
fn worker_crash_restarts_without_losing_committed_state() {
    let running = Running::new();
    let client = running.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let message = client.send("a", "b", "before crash").unwrap();
    let started = client.info().unwrap().started;
    wait(
        || {
            std::fs::read_to_string(running.root.join("log"))
                .is_ok_and(|log| log.contains(" is ready."))
        },
        3,
    );
    let log = std::fs::read_to_string(running.root.join("log")).unwrap();
    let worker = log
        .lines()
        .find_map(|line| {
            line.split("worker ")
                .nth(1)
                .and_then(|tail| tail.split_whitespace().next())
                .and_then(|pid| pid.parse::<i32>().ok())
        })
        .unwrap();
    unsafe {
        libc::kill(worker, libc::SIGKILL);
    }
    wait(
        || client.info().is_ok_and(|info| info.started != started),
        5,
    );
    assert_eq!(
        client.read_message("b", message.id).unwrap().body,
        "before crash"
    );
}

#[test]
fn queued_burst_and_reused_keep_alive_connections_survive_a_drain() {
    let running = Running::new();
    let pooled = ureq::Agent::new_with_defaults();
    pooled
        .get(format!("{}/api/hub", running.url))
        .call()
        .unwrap()
        .body_mut()
        .read_to_vec()
        .unwrap();
    let address = running.url.trim_start_matches("http://");
    let mut stalled = std::net::TcpStream::connect(address).unwrap();
    stalled.write_all(b"GET /api/hub HTTP/1.1\r\n").unwrap();
    let command = Command::new(&running.binary)
        .arg("upgrade")
        .arg(&running.binary)
        .arg("--db")
        .arg(running.root.join("hub.db"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let mut requests = Vec::new();
    for _ in 0..100 {
        let url = running.url.clone();
        requests.push(std::thread::spawn(move || {
            let http: ureq::Agent = ureq::Agent::config_builder()
                .max_idle_connections(0)
                .timeout_global(Some(Duration::from_secs(12)))
                .build()
                .into();
            http.get(format!("{url}/api/hub"))
                .call()
                .unwrap()
                .body_mut()
                .read_json::<protocol::HubInfo>()
                .unwrap()
        }));
    }
    let result = command.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    for request in requests {
        assert!(request.join().unwrap().supervised);
    }
    pooled
        .get(format!("{}/api/hub", running.url))
        .call()
        .unwrap()
        .body_mut()
        .read_json::<protocol::HubInfo>()
        .unwrap();
    drop(stalled);
}

#[test]
fn three_post_ready_crashes_restore_the_previous_binary_without_erasing_new_writes() {
    let running = Running::new();
    let client = running.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(running.root.join("hub.active.json")).unwrap())
            .unwrap();
    let pid_file = running.root.join("candidate.pid");
    let script = running.candidate(&format!(
        "echo $$ > '{}'; exec '{}' \"$@\"",
        pid_file.display(),
        running.source.display()
    ));
    let result = running.upgrade(&script);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let message = client
        .send("a", "b", "acknowledged by crash-prone new worker")
        .unwrap();
    for crash in 0..3 {
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // The successful upgrade confirms the first readiness handshake.
        // For later workers wait for the supervisor's handshake confirmation,
        // rather than killing them a fixed time after process creation.
        if crash > 0 {
            wait(
                || {
                    std::fs::read_to_string(running.root.join("log")).is_ok_and(|log| {
                        log.contains(&format!("Worker {pid} is ready after restart."))
                    })
                },
                5,
            );
        }
        unsafe {
            assert_eq!(libc::kill(pid, libc::SIGKILL), 0);
        }
        if crash < 2 {
            wait(
                || {
                    std::fs::read_to_string(&pid_file)
                        .is_ok_and(|value| value.trim() != pid.to_string())
                },
                5,
            );
        }
    }
    wait(
        || {
            let current: serde_json::Value = serde_json::from_slice(
                &std::fs::read(running.root.join("hub.active.json")).unwrap(),
            )
            .unwrap();
            current["current"] == before["current"] && client.info().is_ok()
        },
        25,
    );
    assert_eq!(
        client.read_message("b", message.id).unwrap().body,
        "acknowledged by crash-prone new worker"
    );
}

#[test]
fn stop_during_retirement_releases_all_listeners_before_reply_and_refuses_queued_upgrade() {
    use std::os::unix::net::UnixStream;
    let running = Running::new();
    assert!(running.upgrade(&running.binary).status.success());
    let config = cross_agent_message::supervisor::load_config(&running.db).unwrap();
    let swap = cross_agent_message::supervisor::Swap {
        binary: running.binary.clone(),
        sha256: cross_agent_message::updates::sha256(&std::fs::read(&running.binary).unwrap()),
    };
    let request = format!("{}\n", serde_json::to_string(&swap).unwrap());
    let mut queued = UnixStream::connect(config.control()).unwrap();
    queued
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    queued.write_all(request.as_bytes()).unwrap();
    wait(
        || {
            std::fs::read_to_string(running.root.join("log"))
                .unwrap()
                .contains("Upgrade queued for predecessor retirement")
        },
        2,
    );
    cross_agent_message::supervisor::stop(&running.db).unwrap();
    // Do not wait for the supervisor to exit: the acknowledgement is the barrier.
    let address = running.url.trim_start_matches("http://").parse().unwrap();
    let _listener = cross_agent_message::supervisor::bind_listener(address).unwrap();
    let mut reply = String::new();
    std::io::BufReader::new(queued)
        .read_line(&mut reply)
        .unwrap();
    let reply: cross_agent_message::supervisor::Swapped = serde_json::from_str(&reply).unwrap();
    assert!(reply.rolled_back);
    assert!(
        reply
            .reason
            .unwrap()
            .contains("queued upgrade was not started")
    );
}

#[test]
fn legacy_stop_shuts_down_the_supervisor_and_releases_the_listener() {
    let mut running = Running::new();
    running.client().stop().unwrap();
    wait(|| running.process.try_wait().unwrap().is_some(), 6);
    let address = running.url.trim_start_matches("http://");
    let listener =
        cross_agent_message::supervisor::bind_listener(address.parse().unwrap()).unwrap();
    drop(listener);
}

#[test]
fn a_fresh_connection_can_send_its_first_request_after_drain_starts() {
    use std::io::Read;
    let running = Running::new();
    let client = running.client();
    client.register("observer").unwrap();
    let mut events = client.watch("observer").unwrap();
    let config =
        cross_agent_message::supervisor::load_config(&running.root.join("hub.db")).unwrap();
    let candidate = cross_agent_message::supervisor::snapshot(&running.binary, &config).unwrap();
    let sha256 = cross_agent_message::updates::sha256(&std::fs::read(&candidate).unwrap());
    let mut connection =
        std::net::TcpStream::connect(running.url.trim_start_matches("http://")).unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let upgrade = std::thread::spawn(move || {
        cross_agent_message::supervisor::request(
            &config,
            &cross_agent_message::supervisor::Swap {
                binary: candidate,
                sha256,
            },
        )
    });
    assert_eq!(events.next().unwrap().unwrap(), protocol::Event::Restarting);
    connection
        .write_all(b"GET /api/hub HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    connection.read_to_string(&mut response).unwrap();
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "fresh connection failed during drain: {response}"
    );
    let output = upgrade.join().unwrap().unwrap();
    assert!(!output.rolled_back, "{:?}", output.reason);
}

#[test]
fn successful_and_failed_swaps_keep_the_accepting_executable_at_the_installed_path() {
    let running = Running::new();
    running.assert_worker_path();
    assert!(running.upgrade(&running.source).status.success());
    running.assert_worker_path();
    assert!(
        !running
            .upgrade(&running.candidate("exit 17"))
            .status
            .success()
    );
    running.assert_worker_path();
    let config = cross_agent_message::supervisor::load_config(&running.db).unwrap();
    assert_eq!(config.worker_path.as_ref(), Some(&running.binary));
}

#[test]
fn a_second_supervisor_cannot_upgrade_the_same_installation_from_another_database() {
    let running = Running::new();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let output = Command::new(&running.binary)
        .args(["serve", "--port", &port.to_string(), "--db"])
        .arg(running.root.join("other.db"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("already supervises") && error.contains(running.db.to_str().unwrap()),
        "{error}"
    );
    assert!(running.client().info().is_ok());
}

#[test]
fn long_database_paths_and_removed_socket_directories_remain_upgradeable() {
    let running = Running::database(true);
    let client = running.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let message = client.send("a", "b", "survives runtime cleanup").unwrap();
    let config = cross_agent_message::supervisor::load_config(&running.db).unwrap();
    assert!(running.db.as_os_str().len() > 104);
    std::fs::remove_dir_all(config.control().parent().unwrap()).unwrap();
    wait(
        || config.control().exists() && config.worker_control().exists(),
        5,
    );
    let output = running.upgrade(&running.source);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        client.read_message("b", message.id).unwrap().body,
        "survives runtime cleanup"
    );
}

#[test]
fn an_external_installation_change_requires_preflight_before_worker_recovery() {
    let running = Running::new();
    let marker = running.root.join("preflight-ran");
    let candidate = running.root.join("external-candidate");
    std::fs::write(
        &candidate,
        format!(
            "#!/bin/sh\nif [ \"$1\" = self-check ]; then touch '{}'; fi\nexec '{}' \"$@\"\n",
            marker.display(),
            running.source.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o755)).unwrap();
    let pid = running.worker_pid();
    cross_agent_message::installation::activate(&candidate, &running.binary).unwrap();
    unsafe {
        assert_eq!(libc::kill(pid, libc::SIGKILL), 0);
    }
    wait(|| marker.exists() && running.client().info().is_ok(), 10);
    assert!(
        std::fs::read_to_string(running.root.join("log"))
            .unwrap()
            .contains("changed outside upgrade")
    );
}

#[test]
fn an_external_replacement_that_fails_readiness_restores_the_previous_image() {
    let running = Running::new();
    let old_image = std::fs::read(&running.binary).unwrap();
    let pid = running.worker_pid();
    cross_agent_message::installation::activate(&running.candidate("exit 17"), &running.binary)
        .unwrap();
    unsafe {
        assert_eq!(libc::kill(pid, libc::SIGKILL), 0);
    }
    wait(
        || {
            running.worker_pid() != pid
                && std::fs::read(&running.binary).is_ok_and(|bytes| bytes == old_image)
                && running.client().info().is_ok()
        },
        10,
    );
    running.assert_worker_path();
    assert!(
        std::fs::read_to_string(running.root.join("log"))
            .unwrap()
            .contains("restoring the previous installed image")
    );
}

#[test]
fn a_pending_private_address_does_not_prevent_a_safe_upgrade() {
    let running = Running::configured(false, &["203.0.113.243"]);
    let before = running.client().info().unwrap();
    assert!(
        before
            .addresses
            .iter()
            .any(|address| address.address.starts_with("203.0.113.243:")
                && address.state == protocol::AddressState::Pending)
    );
    let output = running.upgrade(&running.source);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let after = running.client().info().unwrap();
    assert!(
        after
            .addresses
            .iter()
            .any(|address| address.address.starts_with("203.0.113.243:")
                && address.state == protocol::AddressState::Pending)
    );
    assert!(
        after
            .addresses
            .iter()
            .any(|address| address.address.starts_with("127.0.0.1:")
                && address.state == protocol::AddressState::Ready)
    );
}

#[test]
fn an_upgrade_rolls_back_when_a_previously_ready_listener_stops_answering_http() {
    let running = Running::configured(false, &["::1"]);
    assert_eq!(
        running
            .client()
            .info()
            .unwrap()
            .addresses
            .iter()
            .filter(|address| address.state == protocol::AddressState::Ready)
            .count(),
        2
    );
    let candidate = running.root.join("unreachable-listener");
    // Both inherited FDs now accept from loopback v4. The supervisor retains
    // the real v6 listener: TCP connects there, but no worker answers HTTP.
    // This reproduces the firewall symptom without changing the host firewall.
    std::fs::write(&candidate, format!("#!/usr/bin/env python3\nimport os, sys\nif sys.argv[1] == 'worker':\n    os.dup2(3, 4)\nos.execv({source:?}, [{source:?}] + sys.argv[1:])\n", source = running.source.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o755)).unwrap();
    let probe = Availability::start(&running.url);
    let output = running.upgrade(&candidate);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("rolled back") && error.contains("not ready") && error.contains("firewall"),
        "{error}"
    );
    assert_eq!(
        running
            .client()
            .info()
            .unwrap()
            .addresses
            .iter()
            .filter(|address| address.state == protocol::AddressState::Ready)
            .count(),
        2
    );
    running.assert_worker_path();
    probe.finish();
}

#[test]
fn stale_state_never_executes_or_signals_an_unrelated_program() {
    let mut running = Running::new();
    let client = running.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    let message = client.send("a", "b", "survives stale state").unwrap();
    let mut unrelated = Watcher(Command::new("sleep").arg("30").spawn().unwrap());
    for legacy in [true, false] {
        client.stop().unwrap();
        wait(|| running.process.try_wait().unwrap().is_some(), 6);
        let mut config: serde_json::Value = serde_json::from_slice(
            &std::fs::read(running.db.with_extension("worker.json")).unwrap(),
        )
        .unwrap();
        if legacy {
            config["worker_path"] = serde_json::Value::Null;
        }
        std::fs::write(
            running.db.with_extension("worker.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        std::fs::write(running.db.with_extension("active.json"), serde_json::to_vec(&serde_json::json!({"current":"/bin/sleep","previous":null,"pid":unrelated.0.id(),"worker_pid":unrelated.0.id()})).unwrap()).unwrap();
        running.restart();
        assert_eq!(
            client.read_message("b", message.id).unwrap().body,
            "survives stale state"
        );
        running.assert_worker_path();
        if legacy {
            assert!(unrelated.0.try_wait().unwrap().is_none());
            unrelated.0.kill().unwrap();
            unrelated.0.wait().unwrap();
        }
    }
    assert!(
        std::fs::read_to_string(running.root.join("log"))
            .unwrap()
            .contains("Ignoring stale supervisor state")
    );
}

#[test]
fn a_read_only_installation_fails_preflight_without_draining_the_worker() {
    if unsafe { libc::getuid() } == 0 {
        return;
    } // Root bypasses directory permission bits.
    let running = Running::new();
    let before = running.worker_pid();
    let permissions = std::fs::metadata(&running.root).unwrap().permissions();
    std::fs::set_permissions(&running.root, std::fs::Permissions::from_mode(0o500)).unwrap();
    let output = running.upgrade(&running.source);
    std::fs::set_permissions(&running.root, permissions).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be writable before upgrading"));
    assert_eq!(running.worker_pid(), before);
    assert!(running.client().info().is_ok());
}

#[test]
fn final_path_preflight_and_worker_exec_use_the_same_inode_before_drain() {
    let running = Running::new();
    let before = running.client().info().unwrap();
    let marker = running.root.join("executions.jsonl");
    let candidate = running.root.join("recording-candidate");
    std::fs::write(&candidate, format!("#!/usr/bin/env python3\nimport os, sys, json, urllib.request\nrow = {{'role': sys.argv[1], 'path': os.path.realpath(sys.argv[0]), 'inode': os.stat(sys.argv[0]).st_ino}}\nif sys.argv[1] == 'build-info' and row['path'] == {installed:?}:\n    with urllib.request.urlopen({url:?} + '/api/hub', timeout=2) as response:\n        row['serving'] = json.load(response)['started']\nwith open({marker:?}, 'a') as output:\n    output.write(json.dumps(row) + '\\n')\nos.execv({source:?}, [{source:?}] + sys.argv[1:])\n",
        installed=running.binary.to_str().unwrap(), marker=marker.to_str().unwrap(), source=running.source.to_str().unwrap(), url=running.url)).unwrap();
    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = running.upgrade(&candidate);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let executions: Vec<serde_json::Value> = std::fs::read_to_string(marker)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let warm = executions
        .iter()
        .find(|row| row["role"] == "build-info" && row["path"] == running.binary.to_str().unwrap())
        .unwrap();
    let worker = executions
        .iter()
        .find(|row| row["role"] == "worker")
        .unwrap();
    assert_eq!(warm["inode"], worker["inode"]);
    assert_eq!(warm["serving"], before.started);
}
