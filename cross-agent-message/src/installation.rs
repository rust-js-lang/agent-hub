//! One supervisor owns a stable executable path; snapshots are recovery data.
use anyhow::{Context, Result, bail};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Installation {
    pub executable: PathBuf,
    _lock: File,
}

pub(crate) fn temporary(parent: &Path, purpose: &str) -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!(
        ".cam-{purpose}-{}-{time}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

pub fn check_writable(executable: &Path) -> Result<()> {
    let parent = executable
        .parent()
        .context("installed executable has no directory")?;
    let temporary = temporary(parent, "check");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| {
            format!(
                "installation directory {} must be writable before upgrading",
                parent.display()
            )
        })?;
    drop(file);
    std::fs::remove_file(temporary)?;
    Ok(())
}

impl Installation {
    pub fn acquire(executable: &Path, db: &Path) -> Result<Self> {
        let executable =
            std::fs::canonicalize(executable).context("cannot resolve installed executable")?;
        let name = executable
            .file_name()
            .context("installed executable has no filename")?
            .to_string_lossy();
        let path = executable.with_file_name(format!(".{name}.supervisor.lock"));
        let mut lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .with_context(|| {
                format!(
                    "cannot lock installation {}; its directory must be writable",
                    executable.display()
                )
            })?;
        let metadata = lock.metadata()?;
        // SAFETY: getuid only reads this process's user id; flock receives a live FD.
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::getuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            bail!(
                "installation lock {} must be a regular file owned by this user with mode 0600",
                path.display()
            );
        }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            let mut owner = String::new();
            let _ = (&mut lock).take(4096).read_to_string(&mut owner);
            if error.kind() == std::io::ErrorKind::WouldBlock {
                bail!(
                    "This executable already supervises the hub on {}: install a copy for a second hub, or run it with --no-supervisor",
                    owner.trim()
                );
            }
            return Err(error).context("cannot lock installed executable");
        }
        lock.set_len(0)?;
        lock.rewind()?;
        writeln!(lock, "{}", db.display())?;
        lock.sync_all()?;
        Ok(Self {
            executable,
            _lock: lock,
        })
    }
}

/// A new inode and rename preserve the old executable until activation succeeds.
/// The lock is a separate durable file: replacing the executable cannot release it.
pub fn activate(snapshot: &Path, executable: &Path) -> Result<()> {
    let bytes = std::fs::read(snapshot).context("cannot read recovery snapshot")?;
    let expected = crate::updates::sha256(&bytes);
    activate_bytes(&bytes, executable, &expected)
}

pub fn activate_verified(snapshot: &Path, executable: &Path, expected: &str) -> Result<()> {
    let bytes = std::fs::read(snapshot).context("cannot read recovery snapshot")?;
    if crate::updates::sha256(&bytes) != expected {
        bail!("recovery snapshot checksum mismatch");
    }
    activate_bytes(&bytes, executable, expected)
}

fn activate_bytes(bytes: &[u8], executable: &Path, expected: &str) -> Result<()> {
    if std::fs::read(executable).is_ok_and(|current| crate::updates::sha256(&current) == expected) {
        return Ok(());
    }
    let parent = executable
        .parent()
        .context("installed executable has no directory")?;
    let temporary = temporary(parent, "activate");
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o755)
            .open(&temporary)
            .with_context(|| {
                format!(
                    "installation directory {} is not writable",
                    parent.display()
                )
            })?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        if crate::updates::sha256(&std::fs::read(&temporary)?) != expected {
            bail!("staged installation checksum mismatch");
        }
        std::fs::rename(&temporary, executable)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn installation_lock_follows_canonical_path_and_survives_atomic_replacement() {
        let root = std::env::temp_dir().join(format!("cam-install-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let binary = root.join("cross-agent-message");
        let image = root.join("snapshot");
        std::fs::write(&binary, b"old").unwrap();
        std::fs::write(&image, b"new").unwrap();
        let alias = root.join("alias");
        symlink(&binary, &alias).unwrap();
        let first = Installation::acquire(&alias, Path::new("/first/hub.db")).unwrap();
        let independent = root.join("independent");
        std::fs::write(&independent, b"other installation").unwrap();
        let independent_lock =
            Installation::acquire(&independent, Path::new("/independent/hub.db")).unwrap();
        assert_eq!(first.executable, std::fs::canonicalize(&binary).unwrap());
        activate(&image, &first.executable).unwrap();
        assert_eq!(std::fs::read(&binary).unwrap(), b"new");
        assert!(
            activate_verified(&image, &binary, &crate::updates::sha256(b"different bytes"))
                .is_err()
        );
        assert_eq!(std::fs::read(&binary).unwrap(), b"new");
        let error = Installation::acquire(&binary, Path::new("/second/hub.db"))
            .err()
            .unwrap();
        assert!(error.to_string().contains("/first/hub.db"));
        assert!(activate(&root.join("missing"), &binary).is_err());
        assert_eq!(std::fs::read(&binary).unwrap(), b"new");
        drop(first);
        let second = Installation::acquire(&binary, Path::new("/second/hub.db")).unwrap();
        drop(second);
        drop(independent_lock);
        std::fs::remove_dir_all(root).unwrap();
    }
}
