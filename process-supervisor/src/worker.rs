//! Worker descriptor ownership and parked lifecycle, independent of a server framework.
use anyhow::{Result, bail};
use std::{
    collections::BTreeMap,
    os::fd::{FromRawFd, RawFd},
    time::Duration,
};

pub struct Inherited {
    listeners: Vec<std::net::TcpListener>,
    pub channel: crate::handoff::Channel,
}
impl Inherited {
    /// Take exclusive ownership of the inherited listener and lifecycle FDs.
    ///
    /// # Safety
    /// The caller must be the freshly exec'd worker. FDs 3..ready_fd must be
    /// unique TCP listeners and ready_fd a unique Unix stream, all transferred
    /// by this library's supervisor and not owned by any existing Rust value.
    pub unsafe fn take(count: usize, ready_fd: RawFd, expected_count: usize) -> Result<Self> {
        if count == 0 || count > 64 || count != expected_count {
            bail!("invalid inherited listener count");
        }
        if ready_fd != 3 + count as RawFd {
            bail!("invalid readiness descriptor");
        }
        let mut listeners = Vec::new();
        for fd in 3..ready_fd {
            // SAFETY: the supervisor hands exactly these unique socket descriptors
            // to this worker. fcntl verifies they are open before ownership transfers.
            if unsafe { libc::fcntl(fd, libc::F_GETFD) } < 0 {
                bail!("listener descriptor is not open");
            }
            close_on_exec(fd)?;
            let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
            listener.set_nonblocking(true)?;
            // Keep the inherited descriptors outside the serving runtime. During
            // quiescence the process still owns them but no task accepts on them.
            listeners.push(listener);
        }
        if unsafe { libc::fcntl(ready_fd, libc::F_GETFD) } < 0 {
            bail!("readiness descriptor is not open");
        }
        close_on_exec(ready_fd)?;
        // SAFETY: this unique endpoint is owned solely by this worker after exec.
        let ready = unsafe { std::os::unix::net::UnixStream::from_raw_fd(ready_fd) };
        let ready = crate::handoff::Channel::new(ready)?;
        Ok(Self {
            listeners,
            channel: ready,
        })
    }
}

/// Run a generation, then acknowledge drain only after its runtime/state has
/// been destroyed. `serve` must finish all requests and release exclusive state
/// before returning Quiesced; it must stop on lifecycle EOF while serving.
/// The library retains listener ownership while parked and never resumes on EOF.
pub fn run(
    inherited: Inherited,
    mut serve: impl FnMut(
        &[std::net::TcpListener],
        &mut crate::handoff::Channel,
        u64,
    ) -> Result<Outcome>,
) -> Result<()> {
    run_checked(inherited, &mut serve, |_| Ok(()))
}

/// Conformance entry point: verify the application's exclusive resources have
/// been released by this PID after every quiesce and before acknowledging it.
/// Failure exits without publishing a false Drained acknowledgement.
pub fn run_checked(
    inherited: Inherited,
    mut serve: impl FnMut(
        &[std::net::TcpListener],
        &mut crate::handoff::Channel,
        u64,
    ) -> Result<Outcome>,
    mut released: impl FnMut(u32) -> Result<()>,
) -> Result<()> {
    let Inherited {
        listeners,
        channel: mut ready,
    } = inherited;
    let signals = crate::supervisor::ArcSignal::new()?;
    let mut request = 0;
    loop {
        let outcome = serve(&listeners, &mut ready, request)?;
        let Outcome::Quiesced {
            id,
            stopped_accepting_ns,
            first_accepted_ns,
        } = outcome
        else {
            return Ok(());
        };
        // The generation must release all exclusive resources, including
        // owners on other threads, before this acknowledgement is sent.
        released(std::process::id())?;
        ready.send(
            &crate::handoff::Reply::Drained {
                id,
                stopped_accepting_ns: stopped_accepting_ns.clone(),
                first_accepted_ns: first_accepted_ns.clone(),
            },
            std::time::Instant::now() + Duration::from_secs(1),
        )?;
        eprintln!(
            "Worker {} drained and released application state; awaiting resume or retire.",
            std::process::id()
        );
        let deadline = std::time::Instant::now() + crate::handoff::PARK_LIMIT;
        loop {
            if signals.stopped() {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                bail!(
                    "parked worker received no resume or retire within {} seconds",
                    crate::handoff::PARK_LIMIT.as_secs()
                );
            }
            if let Some(line) = ready.poll()? {
                match serde_json::from_str::<crate::handoff::Command>(&line)? {
                    crate::handoff::Command::Resume { id } => {
                        request = id;
                        break;
                    }
                    crate::handoff::Command::Retire { .. } => return Ok(()),
                    crate::handoff::Command::Quiesce { id } => ready.send(
                        &crate::handoff::Reply::Drained {
                            id,
                            stopped_accepting_ns: stopped_accepting_ns.clone(),
                            first_accepted_ns: first_accepted_ns.clone(),
                        },
                        deadline,
                    )?,
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn close_on_exec(fd: RawFd) -> Result<()> {
    // SAFETY: fcntl only updates flags on an open descriptor owned by the worker.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

pub enum Outcome {
    Stopped,
    Quiesced {
        id: u64,
        stopped_accepting_ns: BTreeMap<String, u64>,
        first_accepted_ns: BTreeMap<String, u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::{Channel, Command, Reply};
    use std::{os::unix::net::UnixStream, time::Instant};

    #[test]
    fn conformance_failure_never_acknowledges_a_false_drain() {
        let (owned, peer) = UnixStream::pair().unwrap();
        let inherited = Inherited {
            listeners: Vec::new(),
            channel: Channel::new(owned).unwrap(),
        };
        let error = run_checked(
            inherited,
            |_, _, _| {
                Ok(Outcome::Quiesced {
                    id: 1,
                    stopped_accepting_ns: BTreeMap::new(),
                    first_accepted_ns: BTreeMap::new(),
                })
            },
            |pid| {
                assert_eq!(pid, std::process::id());
                bail!("exclusive resource is still open")
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("exclusive resource"));
        let mut peer = Channel::new(peer).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match peer.poll() {
                Ok(Some(line)) => panic!("false drain acknowledgement: {line}"),
                Ok(None) => {
                    assert!(
                        Instant::now() < deadline,
                        "channel remained open after failed check"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    assert!(error.to_string().contains("closed"));
                    break;
                }
            }
        }
    }

    #[test]
    fn conformance_hook_runs_for_every_generation_before_drain_acknowledgement() {
        let (owned, peer) = UnixStream::pair().unwrap();
        let checks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = checks.clone();
        let worker = std::thread::spawn(move || {
            let inherited = Inherited {
                listeners: Vec::new(),
                channel: Channel::new(owned).unwrap(),
            };
            let mut generation = 0;
            run_checked(
                inherited,
                |_, _, request| {
                    generation += 1;
                    assert_eq!(request, if generation == 1 { 0 } else { 2 });
                    Ok(Outcome::Quiesced {
                        id: generation,
                        stopped_accepting_ns: BTreeMap::new(),
                        first_accepted_ns: BTreeMap::new(),
                    })
                },
                |_| {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
            )
        });
        let mut channel = Channel::new(peer).unwrap();
        for generation in 1..=2 {
            let deadline = Instant::now() + Duration::from_secs(3);
            let line = loop {
                if let Some(line) = channel.poll().unwrap() {
                    break line;
                }
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(1));
            };
            assert!(
                matches!(serde_json::from_str::<Reply>(&line).unwrap(), Reply::Drained { id, .. } if id == generation)
            );
            assert_eq!(
                checks.load(std::sync::atomic::Ordering::SeqCst),
                generation as usize
            );
            channel
                .send(
                    &if generation == 1 {
                        Command::Resume { id: 2 }
                    } else {
                        Command::Retire { id: 3 }
                    },
                    deadline,
                )
                .unwrap();
        }
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn owned_descriptors_are_not_inherited_by_application_subprocesses() {
        use std::os::fd::AsRawFd;
        let (socket, _peer) = UnixStream::pair().unwrap();
        let fd = socket.as_raw_fd();
        // Never make this FD inheritable in the multi-threaded test process:
        // another fixture may exec concurrently. Real inheritable worker FDs
        // are exercised by the separate-process echo/hub fixtures.
        close_on_exec(fd).unwrap();
        let result = std::process::Command::new("python3").arg("-c").arg("import os,sys\ntry: os.fstat(int(sys.argv[1]))\nexcept OSError: sys.exit(0)\nsys.exit(1)").arg(fd.to_string()).status().unwrap();
        assert!(result.success());
        assert!(
            socket.peer_addr().is_ok(),
            "the parent retains its descriptor"
        );
    }
}
