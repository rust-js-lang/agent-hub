//! A second application: plain TCP, no hub, dashboard, HTTP framework or database.
//! Each connection receives one build string. See the crate README for commands.
use anyhow::{Context, Result, bail};
use process_supervisor::{
    Application, BuildInfo, Config, Swap,
    addresses::{AddressState, ListenAddress},
    runtime::Runtime,
    supervisor,
    worker::{self, Outcome},
};
use serde::{Deserialize, Serialize};
use std::{
    io::Read,
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
    fn runtime() -> Runtime {
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
    fn announce(
        &self,
        _: &Config<Self>,
        event: &process_supervisor::SupervisorEvent,
    ) -> Result<()> {
        eprintln!("{event:?}");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn serve_generation(
    config: &Config<Echo>,
    generation: &mut worker::Generation<'_>,
) -> Result<Outcome> {
    use tokio::io::AsyncWriteExt;
    let mut listeners = generation.listeners()?;
    let listener = listeners
        .first_mut()
        .context("echo requires one listener")?;
    generation.accepting()?;
    let probe_config = config.clone();
    let initial = tokio::task::spawn_blocking(move || {
        probe_config
            .application
            .probe(&probe_config, BUILD, Duration::from_secs(1))
    });
    tokio::pin!(initial);
    let mut ready = false;
    loop {
        tokio::select! {
            addresses = &mut initial, if !ready => {
                generation.ready(addresses?)?;
                ready = true;
            },
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                let reply = format!("{} {BUILD}\n", config.greeting);
                process_supervisor::listener::drain_connection(stream.write_all(reply.as_bytes())).await??;
            },
            event = generation.next_event() => {
                listener.stop_accepting()?;
                return match event? {
                    worker::Event::Quiesce { id } => generation.quiesced(id),
                    worker::Event::Stop => Ok(Outcome::Stopped),
                };
            },
        }
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
            worker::run(
                inherited,
                worker::Options::new(
                    BUILD.into(),
                    config.handoff_protocol,
                    config.handoff_startup,
                ),
                |generation| serve_generation(&config, generation),
            )
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
        "stop" => supervisor::stop_in(&path, Echo::runtime()),
        _ => bail!("unknown mode"),
    }
}
