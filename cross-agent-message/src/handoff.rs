//! Private, inherited supervisor/worker channel. Correlation IDs make a late
//! drain acknowledgement harmless after the supervisor has requested resume.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

pub(crate) const VERSION: u32 = 1;
const MAX_FRAME: usize = 1024 * 1024;

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
    Drained { id: u64 },
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
            Reply::Drained { id: 7 }
        );
        assert_eq!(
            serde_json::from_str::<Reply>(&channel.poll().unwrap().unwrap()).unwrap(),
            Reply::Drained { id: 8 }
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
