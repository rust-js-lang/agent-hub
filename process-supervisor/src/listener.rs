//! Listener acceptance facts owned by the lifecycle library, not the HTTP stack.
use crate::handoff::{Acceptance, monotonic_ns};
use anyhow::{Result, bail};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Finish an already accepted request during shutdown under the engine's drain
/// budget. The application decides how its protocol requests graceful close;
/// the maximum wait is owned by the library and matches admission accounting.
pub async fn drain_connection<F: std::future::Future>(
    connection: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(crate::handoff::REQUEST_DRAIN, connection).await
}

pub struct Timing {
    pub address: String,
    first: AtomicU64,
    stopped: AtomicU64,
}
impl Timing {
    pub fn first_accepted_ns(&self) -> u64 {
        self.first.load(Ordering::SeqCst)
    }
    pub fn stopped_accepting_ns(&self) -> u64 {
        self.stopped.load(Ordering::SeqCst)
    }
}

pub struct Listener {
    socket: tokio::net::TcpListener,
    timing: Arc<Timing>,
    request: u64,
    accepted: tokio::sync::mpsc::UnboundedSender<Acceptance>,
}
impl Listener {
    pub fn new(
        socket: &std::net::TcpListener,
        request: u64,
        accepted: tokio::sync::mpsc::UnboundedSender<Acceptance>,
    ) -> Result<Self> {
        let address = socket.local_addr()?.to_string();
        Ok(Self {
            socket: tokio::net::TcpListener::from_std(socket.try_clone()?)?,
            timing: Arc::new(Timing {
                address,
                first: AtomicU64::new(0),
                stopped: AtomicU64::new(0),
            }),
            request,
            accepted,
        })
    }
    pub fn timing(&self) -> Arc<Timing> {
        self.timing.clone()
    }
    pub async fn accept(&self) -> Result<(tokio::net::TcpStream, SocketAddr)> {
        if self.timing.stopped_accepting_ns() != 0 {
            bail!("listener is quiesced");
        }
        let accepted = self.socket.accept().await?;
        if self.timing.first_accepted_ns() == 0 {
            let at_ns = monotonic_ns()?;
            if self
                .timing
                .first
                .compare_exchange(0, at_ns, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let _ = self.accepted.send(Acceptance {
                    request: self.request,
                    address: self.timing.address.clone(),
                    at_ns,
                });
            }
        }
        Ok(accepted)
    }
    /// Call after cancelling the accept future, before draining connections.
    /// Retains the descriptor while preventing any subsequent accept call.
    pub fn stop_accepting(&self) -> Result<()> {
        let at_ns = monotonic_ns()?;
        let _ = self
            .timing
            .stopped
            .compare_exchange(0, at_ns, Ordering::SeqCst, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn first_acceptance_is_reported_once_and_quiescence_prevents_further_accepts() {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
        let listener = Listener::new(&socket, 17, sent).unwrap();
        let address = socket.local_addr().unwrap();
        let first_client = tokio::net::TcpStream::connect(address).await.unwrap();
        let first = listener.accept().await.unwrap();
        let event = received.recv().await.unwrap();
        assert_eq!(event.request, 17);
        assert_eq!(event.address, address.to_string());
        assert_eq!(event.at_ns, listener.timing().first_accepted_ns());
        let second_client = tokio::net::TcpStream::connect(address).await.unwrap();
        let second = listener.accept().await.unwrap();
        assert!(received.try_recv().is_err());
        listener.stop_accepting().unwrap();
        let stopped = listener.timing().stopped_accepting_ns();
        assert!(stopped >= event.at_ns);
        listener.stop_accepting().unwrap();
        assert_eq!(stopped, listener.timing().stopped_accepting_ns());
        assert!(
            listener
                .accept()
                .await
                .unwrap_err()
                .to_string()
                .contains("quiesced")
        );
        drop((first_client, first, second_client, second));
    }
}
