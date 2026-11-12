//! Explicit executable updates. Downloads are staged and verified before rename.
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub const TARGET: &str = env!("CROSS_AGENT_TARGET");

pub struct Binary {
    pub bytes: Vec<u8>,
    pub sha256: String,
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn binary() -> Result<&'static Binary, &'static str> {
    static BINARY: OnceLock<Result<Binary, String>> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            let path = std::env::current_exe().map_err(|e| e.to_string())?;
            let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
            Ok(Binary {
                sha256: sha256(&bytes),
                bytes,
            })
        })
        .as_ref()
        .map_err(String::as_str)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    pub version: String,
    pub build: String,
    pub target: String,
    pub protocol: u32,
    /// Supported private lifecycle protocol, independent of the active parent.
    #[serde(default)]
    pub handoff_protocol: u32,
    #[serde(default)]
    pub handoff_startup: bool,
}

pub fn build_info() -> BuildInfo {
    BuildInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build: crate::hub::BUILD.to_string(),
        target: TARGET.to_string(),
        protocol: protocol::PROTOCOL,
        handoff_protocol: crate::handoff::VERSION,
        handoff_startup: true,
    }
}

pub fn inspect(path: &Path) -> Result<BuildInfo> {
    let result = std::process::Command::new(path)
        .arg("build-info")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("cannot inspect staged executable")?;
    let mut child = result;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while child.try_wait()?.is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("staged executable did not answer build-info within 3 seconds");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let result = child.wait_with_output()?;
    if !result.status.success() {
        bail!("staged executable failed its build-info check");
    }
    let info: BuildInfo =
        serde_json::from_slice(&result.stdout).context("invalid executable build information")?;
    if info.target != TARGET {
        bail!(
            "binary target {} does not match this machine ({TARGET})",
            info.target
        );
    }
    if info.protocol != protocol::PROTOCOL {
        bail!("binary protocol {} is unsupported", info.protocol);
    }
    Ok(info)
}

/// Leaves the old executable untouched on any staging/checksum/validation failure.
pub fn install(bytes: &[u8], expected_sha256: &str, destination: &Path) -> Result<BuildInfo> {
    use std::os::unix::fs::PermissionsExt;
    if sha256(bytes) != expected_sha256 {
        bail!("downloaded executable checksum does not match");
    }
    let parent = destination
        .parent()
        .context("executable has no parent directory")?;
    let temp = parent.join(format!(
        ".cross-agent-message-{}.download",
        std::process::id()
    ));
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .context("cannot stage download beside executable")?;
    let result = (|| {
        file.write_all(bytes)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o755))?;
        file.sync_all()?;
        let info = inspect(&temp)?;
        let previous = destination.with_extension("prev");
        // Copying the old inode does not create a window where destination is absent.
        std::fs::copy(destination, &previous).context("cannot keep previous executable")?;
        std::fs::File::open(&previous)?.sync_all()?;
        std::fs::rename(&temp, destination).context("cannot activate verified executable")?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(info)
    })();
    let _ = std::fs::remove_file(&temp);
    result
}

pub fn upgrade_self(client: &crate::client::Client, from_hub: bool) -> Result<()> {
    let info = client.info()?;
    let (bytes, checksum) = if from_hub || info.target == TARGET {
        client.require("binary-download")?;
        if info.target != TARGET {
            bail!("hub target {} does not match {TARGET}", info.target);
        }
        let checksum = info
            .binary_sha256
            .context("hub has no executable checksum")?;
        (client.download_binary()?, checksum)
    } else {
        github_binary()?
    };
    let installed = install(&bytes, &checksum, &std::env::current_exe()?)?;
    println!(
        "Installed {} (build {}); previous executable kept as .prev.",
        installed.version, installed.build
    );
    Ok(())
}

fn github_binary() -> Result<(Vec<u8>, String)> {
    if TARGET != "aarch64-apple-darwin" {
        bail!(
            "no binary release for {TARGET}; build from source: git clone --recurse-submodules https://github.com/rust-js-lang/cross-agent-message.git; cargo install --path cross-agent-message/cross-agent-message"
        );
    }
    #[derive(Deserialize)]
    struct Asset {
        name: String,
        browser_download_url: String,
    }
    #[derive(Deserialize)]
    struct Release {
        assets: Vec<Asset>,
    }
    let http = ureq::Agent::new_with_defaults();
    let release: Release = http
        .get("https://api.github.com/repos/rust-js-lang/cross-agent-message/releases/latest")
        .header("user-agent", "cross-agent-message")
        .call()?
        .body_mut()
        .read_json()?;
    let archive = release
        .assets
        .iter()
        .find(|a| a.name.contains(TARGET) && a.name.ends_with(".tar.gz"))
        .context("latest release has no Apple Silicon archive")?;
    let sums = release
        .assets
        .iter()
        .find(|a| a.name == "SHA256SUMS" || a.name.ends_with(".sha256"))
        .context("latest release has no checksums")?;
    let checksums = http
        .get(&sums.browser_download_url)
        .call()?
        .body_mut()
        .read_to_string()?;
    let expected = checksums
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            let hash = fields.next()?;
            let name = fields.next()?.trim_start_matches('*');
            (name == archive.name).then(|| hash.to_string())
        })
        .context("archive checksum is missing")?;
    let bytes = http
        .get(&archive.browser_download_url)
        .call()?
        .body_mut()
        .with_config()
        .limit(128 * 1024 * 1024)
        .read_to_vec()?;
    if sha256(&bytes) != expected {
        bail!("release archive checksum mismatch");
    }
    let directory = std::env::temp_dir().join(format!("cam-release-{}", std::process::id()));
    std::fs::create_dir(&directory)?;
    let result = (|| {
        let file = directory.join("release.tar.gz");
        std::fs::write(&file, bytes)?;
        // Extract just the executable, not arbitrary archive paths.
        let listing = std::process::Command::new("tar")
            .args(["-tzf"])
            .arg(&file)
            .output()?;
        if !listing.status.success() {
            bail!("cannot list release archive");
        }
        let names = String::from_utf8(listing.stdout)?;
        let member = names
            .lines()
            .find(|name| {
                Path::new(name)
                    .file_name()
                    .is_some_and(|n| n == "cross-agent-message")
            })
            .context("release has no executable")?;
        let extracted = std::process::Command::new("tar")
            .arg("-xOzf")
            .arg(&file)
            .arg(member)
            .output()?;
        if !extracted.status.success() {
            bail!("cannot read executable from archive");
        }
        let checksum = sha256(&extracted.stdout);
        Ok((extracted.stdout, checksum))
    })();
    let _ = std::fs::remove_dir_all(directory);
    result
}

fn version_parts(version: &str) -> Option<Vec<u64>> {
    version
        .split('.')
        .map(str::parse)
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

/// A local, best-effort stamp; diagnostics never appear on watch stdout.
pub fn warn_if_behind(
    info: &protocol::HubInfo,
    hub: &str,
    state_dir: &Path,
    today: u64,
) -> Result<bool> {
    let Some(remote) = version_parts(&info.version) else {
        return Ok(false);
    };
    let Some(local) = version_parts(env!("CARGO_PKG_VERSION")) else {
        return Ok(false);
    };
    if remote <= local {
        return Ok(false);
    }
    std::fs::create_dir_all(state_dir)?;
    let stamp = state_dir.join(format!("skew-{}", sha256(hub.as_bytes())));
    let value = format!("{today} {}", info.version);
    if std::fs::read_to_string(&stamp).ok().as_deref() == Some(&value) {
        return Ok(false);
    }
    std::fs::write(stamp, value)?;
    eprintln!(
        "The hub is {}, this command line {}: cross-agent-message upgrade-self.",
        info.version,
        env!("CARGO_PKG_VERSION")
    );
    Ok(true)
}

pub fn client_state_dir() -> PathBuf {
    std::env::var_os("CROSS_AGENT_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::home_dir()
                .unwrap_or_default()
                .join(".cross-agent-message")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn skew_notices_are_once_a_day_per_hub_release_and_ignore_development_builds() {
        let hub = crate::hub::Hub::new(crate::store::Store::in_memory().unwrap()).unwrap();
        let mut info = hub.info();
        let dir = std::env::temp_dir().join(format!("cam-skew-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        info.build = "different-development-build".to_string();
        assert!(!warn_if_behind(&info, "http://hub", &dir, 1).unwrap());
        info.version = "999.0.0".to_string();
        assert!(warn_if_behind(&info, "http://hub", &dir, 1).unwrap());
        assert!(!warn_if_behind(&info, "http://hub", &dir, 1).unwrap());
        assert!(warn_if_behind(&info, "http://hub", &dir, 2).unwrap());
        info.version = "999.0.1".to_string();
        assert!(warn_if_behind(&info, "http://hub", &dir, 2).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
