//! Private, inherited supervisor/worker channel. Correlation IDs make a late
//! drain acknowledgement harmless after the supervisor has requested resume.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

pub(crate) const VERSION: u32 = 1;
pub(crate) const REQUEST_DRAIN: Duration = Duration::from_secs(4);
pub(crate) const DRAIN_LIMIT: Duration = REQUEST_DRAIN.saturating_add(Duration::from_secs(1));
pub(crate) const FIRST_ACCEPT_LIMIT: Duration = Duration::from_secs(2);
pub(crate) const EARLY_DRAIN_LIMIT: Duration = Duration::from_secs(1);
pub(crate) const READY_LIMIT: Duration = Duration::from_secs(5);
pub(crate) const EXPOSURE_LIMIT: Duration = Duration::from_secs(10);
pub(crate) const ADMISSION_MARGIN: Duration = Duration::from_millis(500);
pub(crate) const ROLLBACK_RESERVE: Duration = DRAIN_LIMIT
    .saturating_add(FIRST_ACCEPT_LIMIT)
    .saturating_add(ADMISSION_MARGIN);
pub(crate) const LAUNCH_RESERVE: Duration = FIRST_ACCEPT_LIMIT.saturating_add(ROLLBACK_RESERVE);
// ALF may resolve a flow's attributed PID asynchronously, including under load.
pub(crate) const RETIRE_GRACE: Duration = Duration::from_secs(5);
// Both attempts can consume drain + readiness, followed by the firewall grace.
const OPERATION_LIMIT: Duration = DRAIN_LIMIT
    .saturating_add(READY_LIMIT)
    .saturating_add(DRAIN_LIMIT)
    .saturating_add(READY_LIMIT)
    .saturating_add(RETIRE_GRACE);
pub(crate) const PARK_LIMIT: Duration = OPERATION_LIMIT.saturating_add(Duration::from_secs(5));
pub(crate) const CONTROL_LIMIT: Duration = PARK_LIMIT
    .saturating_add(RETIRE_GRACE)
    .saturating_add(Duration::from_secs(5));
const _: () = {
    assert!(DRAIN_LIMIT.as_millis() > REQUEST_DRAIN.as_millis());
    assert!(LAUNCH_RESERVE.as_millis() < EXPOSURE_LIMIT.as_millis());
    assert!(DRAIN_LIMIT.as_millis() + FIRST_ACCEPT_LIMIT.as_millis() < 10_000);
    assert!(
        DRAIN_LIMIT.as_millis()
            + 2 * FIRST_ACCEPT_LIMIT.as_millis()
            + EARLY_DRAIN_LIMIT.as_millis()
            <= 10_000
    );
    assert!(PARK_LIMIT.as_millis() > OPERATION_LIMIT.as_millis());
    assert!(CONTROL_LIMIT.as_millis() > PARK_LIMIT.as_millis() + RETIRE_GRACE.as_millis());
};
const MAX_FRAME: usize = 1024 * 1024;

/// All related processes share this host clock; process-local Instant origins
/// cannot be serialized and subtracted across the lifecycle channel.
pub(crate) fn monotonic_ns() -> Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: the pointer is writable and CLOCK_MONOTONIC is a valid host clock.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    u64::try_from(time.tv_sec)?
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(time.tv_nsec as u64))
        .context("monotonic timestamp overflow")
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Acceptance {
    pub request: u64,
    pub address: String,
    pub at_ns: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub(crate) enum Command {
    Quiesce { id: u64 },
    Resume { id: u64 },
    Retire { id: u64 },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub(crate) enum Reply {
    Drained {
        id: u64,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        stopped_accepting_ns: BTreeMap<String, u64>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        first_accepted_ns: BTreeMap<String, u64>,
    },
}

pub(crate) struct Channel {
    pub stream: UnixStream,
    buffered: Vec<u8>,
}

impl Channel {
    pub fn new(stream: UnixStream) -> Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            buffered: Vec::new(),
        })
    }

    pub fn send(&mut self, value: &impl Serialize, deadline: Instant) -> Result<()> {
        self.write(&format!("{}\n", serde_json::to_string(value)?), deadline)
    }

    pub fn write(&mut self, value: &str, deadline: Instant) -> Result<()> {
        let mut bytes = value.as_bytes();
        while !bytes.is_empty() {
            match self.stream.write(bytes) {
                Ok(0) => bail!("handoff channel closed while writing"),
                Ok(count) => bytes = &bytes[count..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        bail!("handoff write timed out");
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub fn poll(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(end) = self.buffered.iter().position(|byte| *byte == b'\n') {
                if end > MAX_FRAME {
                    bail!("handoff frame is too large");
                }
                let line = self.buffered.drain(..=end).collect::<Vec<_>>();
                return Ok(Some(
                    String::from_utf8(line).context("invalid handoff text")?,
                ));
            }
            if self.buffered.len() > MAX_FRAME {
                bail!("handoff frame is too large");
            }
            let mut bytes = [0; 4096];
            match self.stream.read(&mut bytes) {
                Ok(0) => bail!("handoff channel closed"),
                Ok(count) => self.buffered.extend_from_slice(&bytes[..count]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Do not prefetch a subsequent command: after quiesce the runtime is dropped
/// before the parked process consumes resume or retire from this same socket.
pub(crate) struct Commands {
    stream: tokio::net::UnixStream,
    buffered: Vec<u8>,
}

impl Commands {
    pub fn new(stream: tokio::net::UnixStream) -> Self {
        Self {
            stream,
            buffered: Vec::new(),
        }
    }

    // select! may cancel this wait when a heartbeat or listener wins. Keep
    // partial frames on self so cancellation cannot lose their leading bytes.
    pub async fn receive(&mut self) -> Result<Command> {
        use tokio::io::AsyncReadExt;
        loop {
            let byte = self.stream.read_u8().await?;
            if byte == b'\n' {
                let result = serde_json::from_slice(&self.buffered);
                self.buffered.clear();
                return Ok(result?);
            }
            self.buffered.push(byte);
            if self.buffered.len() > MAX_FRAME {
                bail!("handoff command is too large");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_and_pipelined_frames_remain_distinct() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut channel = Channel::new(a).unwrap();
        b.write_all(b"{\"state\":\"drained\",\"id\":").unwrap();
        assert!(channel.poll().unwrap().is_none());
        b.write_all(b"7}\n{\"state\":\"drained\",\"id\":8}\n")
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Reply>(&channel.poll().unwrap().unwrap()).unwrap(),
            Reply::Drained {
                id: 7,
                stopped_accepting_ns: BTreeMap::new(),
                first_accepted_ns: BTreeMap::new()
            }
        );
        assert_eq!(
            serde_json::from_str::<Reply>(&channel.poll().unwrap().unwrap()).unwrap(),
            Reply::Drained {
                id: 8,
                stopped_accepting_ns: BTreeMap::new(),
                first_accepted_ns: BTreeMap::new()
            }
        );
        assert!(channel.poll().unwrap().is_none());
    }

    #[tokio::test]
    async fn async_quiesce_does_not_consume_the_queued_resume() {
        let (a, mut b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.write_all(b"{\"command\":\"quiesce\",\"id\":1}\n{\"command\":\"resume\",\"id\":2}\n")
            .unwrap();
        let mut asynchronous =
            Commands::new(tokio::net::UnixStream::from_std(a.try_clone().unwrap()).unwrap());
        assert_eq!(
            asynchronous.receive().await.unwrap(),
            Command::Quiesce { id: 1 }
        );
        drop(asynchronous);
        let mut parked = Channel::new(a).unwrap();
        assert_eq!(
            serde_json::from_str::<Command>(&parked.poll().unwrap().unwrap()).unwrap(),
            Command::Resume { id: 2 }
        );
    }

    #[tokio::test]
    async fn cancelling_a_command_wait_preserves_a_partial_frame() {
        let (a, mut b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        let mut commands = Commands::new(tokio::net::UnixStream::from_std(a).unwrap());
        b.write_all(b"{\"command\":\"quiesce\",\"id\":").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), commands.receive())
                .await
                .is_err()
        );
        b.write_all(b"19}\n").unwrap();
        assert_eq!(
            commands.receive().await.unwrap(),
            Command::Quiesce { id: 19 }
        );
    }
}
