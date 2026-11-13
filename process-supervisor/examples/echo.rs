//! A second application: plain TCP, no hub, dashboard, HTTP framework or database.
//! Each connection receives one build string. See the crate README for commands.
use anyhow::{Context, Result, bail};
use process_supervisor::{
    Application, BuildInfo, Config, Swap,
    addresses::{AddressState, ListenAddress, Ready},
    handoff::{self, Acceptance, Channel, Command as Lifecycle},
    runtime::Runtime,
    supervisor,
    worker::{self, Outcome},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

const BUILD: &str = "echo-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Echo {
    greeting: String,
}

impl Application for Echo {
    fn name(&self) -> &'static str {
        "echo"
    }
    fn runtime(&self) -> Runtime {
        Runtime {
            home_directory: ".supervisor-echo",
            temporary_prefix: "ps-echo",
        }
    }
    fn inspect(&self, binary: &Path) -> Result<BuildInfo> {
        let mut child = Command::new(binary)
            .arg("identity")
            .stdout(std::process::Stdio::piped())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait()?.is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                bail!("echo identity timed out");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let output = child.wait_with_output()?;
        if !output.status.success() {
            bail!("echo identity failed");
        }
        let info: BuildInfo = serde_json::from_slice(&output.stdout)?;
        if info.target != std::env::consts::ARCH || info.protocol != 1 {
            bail!("incompatible echo executable");
        }
        Ok(info)
    }
    fn preflight(&self, binary: &Path, _: &Config<Self>) -> Result<()> {
        self.inspect(binary)?;
        Ok(()) // This stateless service needs no disposable database check.
    }
    fn probe(&self, config: &Config<Self>, build: &str, timeout: Duration) -> Vec<ListenAddress> {
        config
            .addresses
            .iter()
            .map(|address| {
                let result = (|| -> Result<()> {
                    let mut socket = TcpStream::connect_timeout(address, timeout)?;
                    socket.set_read_timeout(Some(timeout))?;
                    let mut reply = String::new();
                    socket.read_to_string(&mut reply)?;
                    if reply != format!("{} {build}\n", self.greeting) {
                        bail!("wrong echo identity");
                    }
                    Ok(())
                })();
                ListenAddress {
                    address: address.to_string(),
                    state: if result.is_ok() {
                        AddressState::Ready
                    } else {
                        AddressState::Blocked
                    },
                    reason: result.err().map(|e| e.to_string()),
                    remote_seen: false,
                }
            })
            .collect()
    }
    fn worker_command(&self, binary: &Path, config: &Config<Self>, count: usize) -> Command {
        let mut command = Command::new(binary);
        command
            .arg("worker")
            .arg(config.config_path())
            .env("ECHO_LISTENERS", count.to_string());
        command
    }
    fn announce(&self, _: &Config<Self>, message: &str) -> Result<()> {
        eprintln!("{message}");
        Ok(())
    }
}

fn serve_generation(
    config: &Config<Echo>,
    listeners: &[std::net::TcpListener],
    channel: &mut Channel,
    request: u64,
) -> Result<Outcome> {
    let deadline = Instant::now() + handoff::READY_LIMIT;
    let mut first = BTreeMap::new();
    channel.write(&format!("accepting {request}\n"), deadline)?;
    // Exercise the actual service concurrently, like a database-backed HTTP
    // application's readiness check, rather than trusting a successful bind.
    let probe_config = config.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let probe = std::thread::spawn(move || {
        let addresses =
            probe_config
                .application
                .probe(&probe_config, BUILD, Duration::from_secs(1));
        let _ = sent.send(addresses);
    });
    let mut ready_sent = false;
    loop {
        for listener in listeners {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let address = listener.local_addr()?.to_string();
                    if !first.contains_key(&address) {
                        let at_ns = handoff::monotonic_ns()?;
                        first.insert(address.clone(), at_ns);
                        channel.write(
                            &format!(
                                "accepted {}\n",
                                serde_json::to_string(&Acceptance {
                                    request,
                                    address,
                                    at_ns
                                })?
                            ),
                            Instant::now() + Duration::from_secs(1),
                        )?;
                    }
                    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                    writeln!(stream, "{} {BUILD}", config.greeting)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }
        if !ready_sent && let Ok(addresses) = received.try_recv() {
            let ready = Ready {
                build: BUILD.into(),
                addresses,
                request,
                handoff_protocol: handoff::VERSION,
                accepted: first.keys().cloned().collect(),
                first_accepted_ns: first.clone(),
            };
            channel.write(
                &format!("ready {}\n", serde_json::to_string(&ready)?),
                deadline,
            )?;
            ready_sent = true;
        }
        if let Some(line) = channel.poll()? {
            match serde_json::from_str::<Lifecycle>(&line)? {
                Lifecycle::Quiesce { id } => {
                    probe
                        .join()
                        .map_err(|_| anyhow::anyhow!("probe panicked"))?;
                    // No background connection tasks or exclusive state remain.
                    let stopped = listeners
                        .iter()
                        .map(|l| Ok((l.local_addr()?.to_string(), handoff::monotonic_ns()?)))
                        .collect::<Result<_>>()?;
                    return Ok(Outcome::Quiesced {
                        id,
                        stopped_accepting_ns: stopped,
                        first_accepted_ns: first,
                    });
                }
                Lifecycle::Retire { .. } => {
                    probe
                        .join()
                        .map_err(|_| anyhow::anyhow!("probe panicked"))?;
                    return Ok(Outcome::Stopped);
                }
                Lifecycle::Resume { .. } => bail!("unexpected resume while serving"),
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context("use identity, serve <state-path> <address>, worker <config>, upgrade <state-path> <binary>, or stop <state-path>")?;
    if mode == "identity" {
        println!(
            "{}",
            serde_json::to_string(&BuildInfo::new(
                "1".into(),
                BUILD.into(),
                std::env::consts::ARCH.into(),
                1
            ))?
        );
        return Ok(());
    }
    let path = PathBuf::from(args.next().context("missing state/config path")?);
    match mode.as_str() {
        "serve" => {
            let address: SocketAddr = args.next().context("missing listen address")?.parse()?;
            supervisor::run(Config {
                db: path,
                addresses: vec![address],
                application: Echo {
                    greeting: "hello".into(),
                },
                activate_current: false,
                worker_path: None,
                requested_addresses: Vec::new(),
                required_ready: Vec::new(),
                handoff_protocol: 0,
                handoff_startup: false,
            })
        }
        "worker" => {
            let config: Config<Echo> = serde_json::from_slice(&std::fs::read(path)?)?;
            let count: usize = std::env::var("ECHO_LISTENERS")?.parse()?;
            // SAFETY: the private worker entry point is exec'd by our supervisor.
            let inherited = unsafe {
                worker::Inherited::take(count, 3 + count as i32, config.addresses.len())?
            };
            worker::run(inherited, |listeners, channel, request| {
                serve_generation(&config, listeners, channel, request)
            })
        }
        "upgrade" => {
            let config: Config<Echo> = supervisor::load_config(&path)?;
            let binary = PathBuf::from(args.next().context("missing candidate")?);
            config.application.preflight(&binary, &config)?;
            let binary = supervisor::snapshot(&binary, &config)?;
            let reply = supervisor::request(
                &config,
                &Swap {
                    sha256: process_supervisor::sha256(&std::fs::read(&binary)?),
                    binary,
                },
            )?;
            println!("{}", serde_json::to_string(&reply)?);
            if reply.rolled_back || reply.deferred {
                bail!("upgrade did not activate");
            }
            Ok(())
        }
        "stop" => supervisor::stop_in(
            &path,
            Echo {
                greeting: String::new(),
            }
            .runtime(),
        ),
        _ => bail!("unknown mode"),
    }
}
