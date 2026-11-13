//! Exercise the inherited lifecycle channel against real worker processes.
use cross_agent_message::{client::Client, supervisor::Config};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::{net::UnixStream, process::CommandExt};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

struct Fixture {
    _serial: std::sync::MutexGuard<'static, ()>,
    root: PathBuf,
    listener: TcpListener,
    config: Config,
}

impl Fixture {
    fn new() -> Self {
        static SERIAL: Mutex<()> = Mutex::new(());
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
        let root = PathBuf::from(format!(
            "/tmp/cam-handoff-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let config = Config {
            addresses: vec![address],
            requested_addresses: vec![address],
            required_ready: vec![address.to_string()],
            db: root.join("hub.db"),
            worker_path: Some(PathBuf::from(env!("CARGO_BIN_EXE_cross-agent-message"))),
            handoff_protocol: 1,
            handoff_startup: true,
            activate_current: false,
            application: cross_agent_message::supervisor::HubApplication {
                away_after: 60_000,
                forget_after: 120_000,
                reconnect_grace: 30_000,
            },
        };
        std::fs::write(config.config_path(), serde_json::to_vec(&config).unwrap()).unwrap();
        Self {
            _serial: serial,
            root,
            listener,
            config,
        }
    }

    fn spawn(&self) -> Worker {
        let (parent, ready) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let sources = [self.listener.as_raw_fd(), ready.as_raw_fd()];
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("workers.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_cross-agent-message"));
        command
            .arg("worker")
            .arg("--config")
            .arg(self.config.config_path())
            .env("CROSS_AGENT_LISTEN_COUNT", "1")
            .env("CROSS_AGENT_READY_FD", "4")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .process_group(0);
        // SAFETY: only async-signal-safe descriptor operations after fork.
        unsafe {
            command.pre_exec(move || {
                let mut copies = [0; 2];
                for (index, source) in sources.iter().enumerate() {
                    copies[index] = libc::fcntl(*source, libc::F_DUPFD_CLOEXEC, 128);
                    if copies[index] < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                for (index, copy) in copies.iter().enumerate() {
                    if libc::dup2(*copy, 3 + index as i32) < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    libc::close(*copy);
                }
                Ok(())
            });
        }
        Worker {
            child: command.spawn().unwrap(),
            wire: BufReader::new(parent),
            log: self.root.join("workers.log"),
        }
    }

    fn client(&self) -> Client {
        Client::new(&format!("http://{}", self.config.addresses[0]))
    }

    fn database_open_by(&self, pid: u32) -> bool {
        let output = Command::new("lsof")
            .args(["-a", "-p", &pid.to_string(), "-Fn"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cannot inspect live worker descriptors"
        );
        let db = self.config.db.to_string_lossy();
        String::from_utf8_lossy(&output.stdout).lines().any(|line| {
            line == format!("n{db}") || line == format!("n{db}-wal") || line == format!("n{db}-shm")
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.config.control().parent().unwrap());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct Worker {
    child: Child,
    wire: BufReader<UnixStream>,
    log: PathBuf,
}
impl Worker {
    fn send(&mut self, command: &str, id: u64) {
        writeln!(
            self.wire.get_mut(),
            "{{\"command\":\"{command}\",\"id\":{id}}}"
        )
        .unwrap();
    }
    fn line(&mut self) -> String {
        let mut line = String::new();
        let read = self.wire.read_line(&mut line);
        assert!(
            read.as_ref().is_ok_and(|count| *count > 0),
            "worker did not reply: {:?}\n{}",
            read,
            std::fs::read_to_string(&self.log).unwrap()
        );
        line
    }
    fn ready(&mut self, id: u64, address: &str) -> cross_agent_message::addresses::Ready {
        let mut line = self.line();
        while line.starts_with("accepting ") || line.starts_with("accepted ") {
            if line.starts_with("accepting ") {
                assert_eq!(line.trim(), format!("accepting {id}"));
            }
            line = self.line();
        }
        let ready: cross_agent_message::addresses::Ready =
            serde_json::from_str(line.strip_prefix("ready ").expect(&line)).unwrap();
        assert_eq!(ready.request, id);
        assert_eq!(ready.handoff_protocol, 1);
        assert!(ready.accepted.iter().any(|accepted| accepted == address));
        assert!(
            ready
                .first_accepted_ns
                .get(address)
                .is_some_and(|at| *at > 0)
        );
        ready
    }
    fn drained(&mut self, id: u64) -> std::collections::BTreeMap<String, u64> {
        let mut line = self.line();
        while line.starts_with("accepted ") {
            line = self.line();
        }
        let reply: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(reply["state"], "drained");
        assert_eq!(reply["id"], id);
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "drained PID must remain alive"
        );
        serde_json::from_value(reply["stopped_accepting_ns"].clone()).unwrap()
    }
    fn retire(&mut self, id: u64) {
        self.send("retire", id);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "retire did not finish");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn closing_the_parent_channel_exits_both_serving_and_parked_workers() {
    for parked in [false, true] {
        let fixture = Fixture::new();
        let address = fixture.config.addresses[0].to_string();
        let mut worker = fixture.spawn();
        worker.ready(0, &address);
        if parked {
            worker.send("quiesce", 1);
            worker.drained(1);
        }
        worker
            .wire
            .get_mut()
            .shutdown(std::net::Shutdown::Both)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while worker.child.try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "orphan worker retained its listeners"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // A replacement must be able to own SQLite and the inherited listener.
        let mut replacement = fixture.spawn();
        replacement.ready(0, &address);
        fixture.client().register("after-parent-exit").unwrap();
        replacement.send("quiesce", 1);
        replacement.drained(1);
        replacement.retire(2);
    }
}

#[test]
fn a_starting_worker_can_park_before_its_initial_http_probes_finish() {
    let mut fixture = Fixture::new();
    // Probe a different listening socket whose owner never answers HTTP.
    let blocked = TcpListener::bind("127.0.0.1:0").unwrap();
    fixture.config.addresses = vec![blocked.local_addr().unwrap()];
    fixture.config.requested_addresses = fixture.config.addresses.clone();
    std::fs::write(
        fixture.config.config_path(),
        serde_json::to_vec(&fixture.config).unwrap(),
    )
    .unwrap();
    let mut worker = fixture.spawn();
    assert_eq!(worker.line().trim(), "accepting 0");
    worker.send("quiesce", 1);
    // The first reply must be drained, with no preceding ready reply.
    worker.drained(1);
    assert!(!fixture.database_open_by(worker.child.id()));
    worker.retire(2);
}

#[test]
fn startup_markers_are_not_sent_to_a_parent_that_did_not_negotiate_them() {
    let mut fixture = Fixture::new();
    fixture.config.handoff_startup = false;
    std::fs::write(
        fixture.config.config_path(),
        serde_json::to_vec(&fixture.config).unwrap(),
    )
    .unwrap();
    let mut worker = fixture.spawn();
    assert!(worker.line().starts_with("ready "));
    worker.send("quiesce", 1);
    worker.drained(1);
    worker.retire(2);
}

#[test]
fn a_snapshot_path_parent_is_refused_before_sqlite_is_opened() {
    let mut fixture = Fixture::new();
    fixture.config.worker_path = None;
    std::fs::write(
        fixture.config.config_path(),
        serde_json::to_vec(&fixture.config).unwrap(),
    )
    .unwrap();
    let mut worker = fixture.spawn();
    assert!(worker.line().contains("serve --replace"));
    assert!(!worker.child.wait().unwrap().success());
    assert!(
        !fixture.config.db.exists(),
        "refusal must precede any SQLite open or migration"
    );
}

#[test]
fn a_new_worker_under_a_legacy_parent_does_not_claim_cooperative_handoff() {
    let mut fixture = Fixture::new();
    fixture.config.handoff_protocol = 0;
    std::fs::write(
        fixture.config.config_path(),
        serde_json::to_vec(&fixture.config).unwrap(),
    )
    .unwrap();
    let mut worker = fixture.spawn();
    let line = worker.line();
    let ready: cross_agent_message::addresses::Ready =
        serde_json::from_str(line.strip_prefix("ready ").unwrap()).unwrap();
    assert_eq!(ready.handoff_protocol, 0);
    let client = fixture.client();
    assert!(
        !client
            .info()
            .unwrap()
            .features
            .iter()
            .any(|feature| feature == "cooperative-handoff")
    );
    client.register("works-with-old-parent").unwrap();
    let upgrade = Command::new(env!("CARGO_BIN_EXE_cross-agent-message"))
        .arg("upgrade")
        .arg(env!("CARGO_BIN_EXE_cross-agent-message"))
        .arg("--db")
        .arg(&fixture.config.db)
        .output()
        .unwrap();
    assert!(!upgrade.status.success());
    assert!(String::from_utf8_lossy(&upgrade.stderr).contains("serve --replace"));
    assert!(worker.child.try_wait().unwrap().is_none());
}

#[test]
fn drained_process_has_no_sqlite_handles_and_lives_until_replacement_accepts() {
    let fixture = Fixture::new();
    let address = fixture.config.addresses[0].to_string();
    let mut old = fixture.spawn();
    let before = old.ready(0, &address);
    let client = fixture.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    client.take("invariant", "a", 1).unwrap();
    assert!(fixture.database_open_by(old.child.id()));
    old.send("quiesce", 1);
    let stopped = old.drained(1);
    assert!(stopped[&address] >= before.first_accepted_ns[&address]);
    assert!(!fixture.database_open_by(old.child.id()));
    let mut new = fixture.spawn();
    let after = new.ready(0, &address);
    assert!(
        after.first_accepted_ns[&address] >= stopped[&address],
        "timestamps must be comparable across worker processes"
    );
    assert!(old.child.try_wait().unwrap().is_none());
    assert!(!fixture.database_open_by(old.child.id()));
    assert!(fixture.database_open_by(new.child.id()));
    assert!(
        client
            .info()
            .unwrap()
            .features
            .iter()
            .any(|feature| feature == "cooperative-handoff")
    );
    client.send("a", "b", "after new acceptance").unwrap();
    old.retire(2);
    assert_eq!(
        client.read_inbox("b", 10).unwrap().messages[0].body,
        "after new acceptance"
    );
    assert_eq!(client.holds().unwrap()[0].name, "invariant");
}

#[test]
fn rollback_resumes_the_same_process_and_keeps_writes_from_the_failed_generation() {
    let fixture = Fixture::new();
    let address = fixture.config.addresses[0].to_string();
    let mut old = fixture.spawn();
    old.ready(0, &address);
    let client = fixture.client();
    client.register("a").unwrap();
    client.register("b").unwrap();
    old.send("quiesce", 10);
    old.drained(10);
    let mut candidate = fixture.spawn();
    candidate.ready(0, &address);
    client.send("a", "b", "candidate committed this").unwrap();
    candidate.child.kill().unwrap();
    candidate.child.wait().unwrap();
    old.send("resume", 11);
    old.ready(11, &address);
    assert!(old.child.try_wait().unwrap().is_none());
    assert_eq!(
        client.read_inbox("b", 10).unwrap().messages[0].body,
        "candidate committed this"
    );
    old.send("quiesce", 12);
    old.drained(12);
    old.retire(13);
}

#[test]
fn resume_queued_during_drain_is_not_lost_and_fresh_connections_wait_for_it() {
    let fixture = Fixture::new();
    let address = fixture.config.addresses[0].to_string();
    let mut old = fixture.spawn();
    old.ready(0, &address);
    old.send("quiesce", 1);
    old.drained(1);
    let mut queued = TcpStream::connect(fixture.config.addresses[0]).unwrap();
    queued
        .write_all(b"GET /api/health HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .unwrap();
    queued
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    assert!(matches!(
        queued.read(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    old.send("resume", 2);
    old.ready(2, &address);
    queued
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut response = String::new();
    queued.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    // Queue resume before the next quiesce has completed. Async command
    // framing must leave it for the parked process after SQLite closes.
    old.send("quiesce", 3);
    old.send("resume", 4);
    old.drained(3);
    old.ready(4, &address);
    old.send("quiesce", 5);
    old.drained(5);
    old.retire(6);
}
