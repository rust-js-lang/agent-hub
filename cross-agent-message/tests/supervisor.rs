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
    root: PathBuf,
    process: Child,
    url: String,
    binary: PathBuf,
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
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = PathBuf::from(format!(
            "/tmp/cam-supervisor-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let binary = PathBuf::from(env!("CARGO_BIN_EXE_cross-agent-message"));
        let log = std::fs::File::create(root.join("log")).unwrap();
        let process = Command::new(&binary)
            .args(["serve", "--db"])
            .arg(root.join("hub.db"))
            .args(["--port", &port.to_string()])
            .env("CROSS_AGENT_STATE_DIR", &root)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0)
            .spawn()
            .unwrap();
        let result = Self {
            root,
            process,
            url: format!("http://127.0.0.1:{port}"),
            binary,
        };
        wait(
            || {
                result.root.join("hub.active.json").exists()
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
            .arg(self.root.join("hub.db"))
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
            self.binary.display()
        );
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
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
    for action in ["exit 17", "exec sleep 60"] {
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
    let script = running.candidate(&format!(
        "(sleep 2; kill -KILL $$) & exec '{}' \"$@\"",
        running.binary.display()
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
