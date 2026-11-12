//! Full HTTP probes distinguish a bound listener from a reachable hub.
use crate::supervisor::Config;
use protocol::{AddressState, ListenAddress};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub struct Ready {
    pub build: String,
    pub addresses: Vec<ListenAddress>,
}

#[derive(Serialize, Deserialize)]
pub struct Observation {
    pub pid: u32,
    pub addresses: Vec<ListenAddress>,
}

pub fn publish(hub: &crate::hub::Hub, addresses: &[ListenAddress], config: &Config) {
    let previous = hub.info().addresses;
    for address in addresses {
        if !previous.iter().any(|old| {
            old.address == address.address
                && old.state == address.state
                && old.reason == address.reason
        }) {
            if let Some(reason) = &address.reason {
                eprintln!("{}: {reason}", address.address);
            } else {
                eprintln!("Address {} is ready.", address.address);
            }
        }
    }
    hub.set_addresses(addresses.to_vec());
    if hub.info().supervised {
        let observed = Observation {
            pid: std::process::id(),
            addresses: hub.info().addresses,
        };
        if let Err(error) =
            crate::supervisor::atomic_json(&config.db.with_extension("addresses.json"), &observed)
        {
            eprintln!("cannot record listener observations: {error:#}");
        }
    }
}

pub async fn initial(config: &Config, hub: &crate::hub::Hub) -> anyhow::Result<Vec<ListenAddress>> {
    let probe_config = config.clone();
    let addresses =
        tokio::task::spawn_blocking(move || probe(&probe_config, crate::hub::BUILD)).await?;
    publish(hub, &addresses, config);
    Ok(addresses)
}

pub async fn monitor(
    config: Config,
    hub: std::sync::Arc<crate::hub::Hub>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    monitor_every(config, hub, shutdown, Duration::from_secs(60)).await
}

async fn monitor_every(
    config: Config,
    hub: std::sync::Arc<crate::hub::Hub>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    cadence: Duration,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(cadence);
    timer.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = timer.tick() => { initial(&config, &hub).await?; },
        }
    }
}

pub fn ready(addresses: &[ListenAddress]) -> Vec<String> {
    addresses
        .iter()
        .filter(|address| address.state == AddressState::Ready)
        .map(|address| address.address.clone())
        .collect()
}

pub fn is_remote(peer: IpAddr, local: &std::collections::HashSet<IpAddr>) -> bool {
    let peer = match peer {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(peer),
        ip => ip,
    };
    !local.is_empty() && !peer.is_loopback() && !local.contains(&peer)
}

pub fn probe(config: &Config, build: &str) -> Vec<ListenAddress> {
    probe_for(config, build, Duration::from_secs(1))
}

pub fn probe_for(config: &Config, build: &str, timeout: Duration) -> Vec<ListenAddress> {
    let requested = if config.requested_addresses.is_empty() {
        &config.addresses
    } else {
        &config.requested_addresses
    };
    std::thread::scope(|scope| {
        let probes: Vec<_> = requested
            .iter()
            .map(|address| {
                let address = *address;
                (
                    address,
                    scope.spawn(move || {
                        if !config.addresses.contains(&address) {
                            return ListenAddress {
                                address: address.to_string(),
                                state: AddressState::Pending,
                                reason: Some(
                                    "the address is not available on an interface yet".to_string(),
                                ),
                                remote_seen: false,
                            };
                        }
                        let executable = config
                            .worker_path
                            .as_deref()
                            .unwrap_or_else(|| std::path::Path::new("cross-agent-message"));
                        probe_one(address, build, executable, timeout)
                    }),
                )
            })
            .collect();
        probes
            .into_iter()
            .map(|(address, task)| {
                task.join().unwrap_or_else(|_| ListenAddress {
                    address: address.to_string(),
                    state: AddressState::Blocked,
                    reason: Some("the HTTP readiness probe panicked".to_string()),
                    remote_seen: false,
                })
            })
            .collect()
    })
}

fn probe_one(
    address: SocketAddr,
    build: &str,
    executable: &std::path::Path,
    timeout: Duration,
) -> ListenAddress {
    let http: ureq::Agent = ureq::Agent::config_builder()
        .max_idle_connections(0)
        .timeout_global(Some(timeout))
        .build()
        .into();
    let response = http
        .get(format!("http://{address}/api/health"))
        .call()
        .and_then(|mut response| response.body_mut().read_json::<String>());
    let reason = match response {
        Ok(actual) if actual == build => None,
        Ok(actual) => Some(format!(
            "readiness reached build {actual}, expected {build}"
        )),
        Err(error) => Some(format!(
            "the hub is listening on {address} but is not reachable there: {error}; the macOS firewall may be blocking {}; allow it once in System Settings, Network, Firewall",
            executable.display()
        )),
    };
    ListenAddress {
        address: address.to_string(),
        state: if reason.is_none() {
            AddressState::Ready
        } else {
            AddressState::Blocked
        },
        reason,
        remote_seen: false,
    }
}

/// Never infer an external peer from an address merely being reachable locally.
pub fn local_ips() -> std::collections::HashSet<IpAddr> {
    let mut found = std::collections::HashSet::new();
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs owns the returned linked list until freeifaddrs. Family
    // checks select the matching sockaddr layout; null addresses are skipped.
    unsafe {
        if libc::getifaddrs(&mut head) != 0 {
            return found;
        }
        let mut cursor = head;
        while !cursor.is_null() {
            let address = (*cursor).ifa_addr;
            if !address.is_null() {
                if (*address).sa_family as libc::c_int == libc::AF_INET {
                    let address = &*address.cast::<libc::sockaddr_in>();
                    found.insert(IpAddr::V4(std::net::Ipv4Addr::from(
                        address.sin_addr.s_addr.to_ne_bytes(),
                    )));
                } else if (*address).sa_family as libc::c_int == libc::AF_INET6 {
                    let address = &*address.cast::<libc::sockaddr_in6>();
                    found.insert(IpAddr::V6(std::net::Ipv6Addr::from(
                        address.sin6_addr.s6_addr,
                    )));
                }
            }
            cursor = (*cursor).ifa_next;
        }
        libc::freeifaddrs(head);
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    #[test]
    fn a_successful_tcp_connection_without_an_http_answer_is_blocked() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let blocked = probe_one(
            address,
            "build",
            std::path::Path::new("/installed/tool"),
            Duration::from_secs(1),
        );
        assert_eq!(blocked.state, AddressState::Blocked);
        assert!(blocked.reason.unwrap().contains("/installed/tool"));
        // The blocked probe is still in the kernel queue. Drain it first.
        let _ = listener.accept().unwrap();
        let response = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = BufReader::new(&mut socket);
            let mut line = String::new();
            loop {
                line.clear();
                assert_ne!(request.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
            }
            drop(request);
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\n\"build\"",
                )
                .unwrap();
        });
        assert_eq!(
            probe_one(
                address,
                "build",
                std::path::Path::new("/installed/tool"),
                Duration::from_secs(1)
            )
            .state,
            AddressState::Ready
        );
        response.join().unwrap();
    }

    #[test]
    fn interface_discovery_includes_loopback() {
        assert!(local_ips().contains(&IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)));
    }

    #[test]
    fn self_probes_and_unknown_interfaces_never_count_as_remote_observation() {
        let local = std::collections::HashSet::from(["100.64.1.1".parse().unwrap()]);
        assert!(!is_remote("100.64.1.1".parse().unwrap(), &local));
        assert!(!is_remote("::ffff:100.64.1.1".parse().unwrap(), &local));
        assert!(!is_remote("127.0.0.1".parse().unwrap(), &local));
        assert!(!is_remote(
            "100.64.1.2".parse().unwrap(),
            &std::collections::HashSet::new()
        ));
        assert!(is_remote("100.64.1.2".parse().unwrap(), &local));
    }

    #[tokio::test]
    async fn a_blocked_address_becomes_ready_on_reprobe_without_restarting_the_hub() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let config = Config {
            addresses: vec![address],
            requested_addresses: vec![address],
            required_ready: Vec::new(),
            worker_path: Some(std::env::current_exe().unwrap()),
            db: std::path::PathBuf::from("unused.db"),
            away_after: 60_000,
            forget_after: 120_000,
            reconnect_grace: 0,
            activate_current: false,
        };
        let hub = std::sync::Arc::new(
            crate::hub::Hub::new(crate::store::Store::in_memory().unwrap()).unwrap(),
        );
        initial(&config, &hub).await.unwrap();
        assert_eq!(hub.info().addresses[0].state, AddressState::Blocked);
        let started = hub.info().started;
        // Discard the timed-out request still queued on the listener before
        // enabling HTTP responses for the monitor's new connection.
        let _ = listener.accept().unwrap();
        let response = std::thread::spawn(move || {
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = BufReader::new(&mut socket);
                let mut line = String::new();
                if request.read_line(&mut line).unwrap() == 0 {
                    continue;
                }
                loop {
                    line.clear();
                    if request.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                        break;
                    }
                }
                drop(request);
                let body = serde_json::to_string(crate::hub::BUILD).unwrap();
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
                break;
            }
        });
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(monitor_every(
            config,
            hub.clone(),
            stopped,
            Duration::from_millis(50),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            while hub.info().addresses[0].state != AddressState::Ready {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(hub.info().started, started);
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        response.join().unwrap();
    }
}
