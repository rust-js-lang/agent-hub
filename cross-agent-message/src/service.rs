//! Per-user launchd service. Installation and removal are explicit commands.
use anyhow::{Context, Result, bail};
use std::path::Path;

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
pub fn plist(binary: &Path, config: &crate::supervisor::Config) -> String {
    let mut args = vec![
        binary.display().to_string(),
        "serve".to_string(),
        "--db".to_string(),
        config.db.display().to_string(),
        "--port".to_string(),
        config.addresses[0].port().to_string(),
    ];
    for address in config.addresses.iter().filter(|a| !a.ip().is_loopback()) {
        args.push("--listen".to_string());
        args.push(address.ip().to_string());
    }
    let args: String = args
        .iter()
        .map(|arg| format!("<string>{}</string>", xml(arg)))
        .collect();
    let log = xml(&config.db.with_extension("log").display().to_string());
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict><key>Label</key><string>org.rust-js.cross-agent-message</string>
<key>ProgramArguments</key><array>{args}</array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>{log}</string><key>StandardErrorPath</key><string>{log}</string></dict></plist>
"#
    )
}

pub fn install(config: &crate::supervisor::Config) -> Result<()> {
    if !cfg!(target_os = "macos") {
        bail!("service install requires macOS");
    }
    let mut config = config.clone();
    if !config.db.is_absolute() {
        config.db = std::env::current_dir()?.join(&config.db);
    }
    for address in config
        .addresses
        .iter()
        .filter(|address| address.ip().is_loopback())
    {
        let listener = crate::supervisor::bind_listener(*address).context("service address is busy; stop the existing manual hub before installing its login service")?;
        drop(listener);
    }
    let home = std::env::home_dir().context("home directory unavailable")?;
    let directory = home.join("Library/LaunchAgents");
    std::fs::create_dir_all(&directory)?;
    std::fs::create_dir_all(config.db.parent().context("database directory")?)?;
    let executable = std::fs::canonicalize(std::env::current_exe()?)?;
    let file = directory.join("org.rust-js.cross-agent-message.plist");
    std::fs::write(&file, plist(&executable, &config))?;
    // SAFETY: getuid has no preconditions and reads the calling user's ID.
    let domain = format!("gui/{}", unsafe { libc::getuid() });
    let status = std::process::Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&file)
        .status()?;
    if !status.success() {
        bail!(
            "launchctl bootstrap failed; an existing service or running hub may need to be stopped first"
        );
    }
    println!("Installed login service: {}", file.display());
    Ok(())
}

pub fn remove() -> Result<()> {
    if !cfg!(target_os = "macos") {
        bail!("service remove requires macOS");
    }
    let home = std::env::home_dir().context("home directory unavailable")?;
    let file = home.join("Library/LaunchAgents/org.rust-js.cross-agent-message.plist");
    let domain = format!("gui/{}", unsafe { libc::getuid() });
    let status = std::process::Command::new("launchctl")
        .args(["bootout", &domain])
        .arg(&file)
        .status()?;
    if !status.success() {
        bail!("launchctl bootout failed; service file retained");
    }
    std::fs::remove_file(file)?;
    println!("Removed login service.");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn service_payload_is_per_user_persistent_and_escapes_paths() {
        let config = crate::supervisor::Config {
            addresses: vec![
                "127.0.0.1:7878".parse().unwrap(),
                "100.1.2.3:7878".parse().unwrap(),
            ],
            db: std::path::PathBuf::from("/tmp/a & b/hub.db"),
            away_after: 3_600_000,
            forget_after: 86_400_000,
            reconnect_grace: 30_000,
            activate_current: false,
            worker_path: None,
            requested_addresses: Vec::new(),
            required_ready: Vec::new(),
            handoff_protocol: 0,
        };
        let text = super::plist(
            std::path::Path::new("/tmp/bin & stuff/cross-agent-message"),
            &config,
        );
        assert!(
            text.contains("<key>RunAtLoad</key><true/>")
                && text.contains("<key>KeepAlive</key><true/>")
        );
        assert!(text.contains("bin &amp; stuff") && text.contains("a &amp; b"));
        assert!(text.contains("100.1.2.3") && text.contains("--listen"));
    }
}
