//! Short, private Unix socket paths, independent of the database path's length.
use anyhow::{Context, Result, bail};
use std::os::unix::{
    ffi::OsStrExt,
    fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
};
use std::path::{Path, PathBuf};

/// Namespaced private IPC paths. Keep these identifiers stable across upgrades.
#[derive(Debug, Clone, Copy)]
pub struct Runtime {
    pub home_directory: &'static str,
    pub temporary_prefix: &'static str,
}
impl Runtime {
    fn root(&self, db: &Path) -> PathBuf {
        if let Some(home) = std::env::home_dir() {
            let durable = home.join(self.home_directory).join("run");
            // Include the longest socket name; sockaddr_un has room for 104 bytes
            // on macOS, including its terminating NUL.
            if directory_under(&durable, db)
                .join("worker.sock")
                .as_os_str()
                .as_bytes()
                .len()
                < 104
            {
                return durable;
            }
        }
        // SAFETY: getuid has no preconditions and reads the calling user's ID.
        PathBuf::from(format!("/tmp/{}-{}", self.temporary_prefix, unsafe {
            libc::getuid()
        }))
    }

    pub fn directory(&self, db: &Path) -> PathBuf {
        directory_under(&self.root(db), db)
    }

    pub fn socket(&self, db: &Path, name: &str) -> PathBuf {
        self.directory(db).join(name)
    }

    pub fn prepare_worker(&self, db: &Path) -> Result<()> {
        let path = db.with_extension("worker.sock");
        let target = self.socket(db, "worker.sock");
        if std::os::unix::net::UnixStream::connect(&target).is_ok()
            || (path.as_os_str().as_bytes().len() < 104
                && std::os::unix::net::UnixStream::connect(&path).is_ok())
        {
            bail!(
                "a worker is still listening for this database; stop the existing service before starting another worker"
            );
        }
        match std::fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.uid() == unsafe { libc::getuid() }
                    && (metadata.file_type().is_socket()
                        || (metadata.file_type().is_symlink()
                            && std::fs::read_link(&path).is_ok_and(|old| old == target))) =>
            {
                std::fs::remove_file(path)?;
            }
            Ok(_) => bail!("refusing to replace existing IPC entry {}", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub fn prepare(&self, db: &Path) -> Result<()> {
        let root = self.root(db);
        if let Some(parent) = root.parent().filter(|parent| *parent != Path::new("/tmp")) {
            std::fs::create_dir_all(parent)?;
            let metadata = std::fs::symlink_metadata(parent)?;
            if !metadata.is_dir()
                || metadata.uid() != unsafe { libc::getuid() }
                || metadata.permissions().mode() & 0o022 != 0
            {
                bail!(
                    "runtime parent {} must be a directory owned by this user and not writable by other users",
                    parent.display()
                );
            }
        }
        private_directory(&root)?;
        private_directory(&self.directory(db))
    }
}
fn directory_under(root: &Path, db: &Path) -> PathBuf {
    let hash = crate::sha256(db.as_os_str().as_bytes());
    root.join(format!("db-{}", &hash[..32]))
}

/// Previous clients know only the socket beside the database. Preserve that
/// entry when its path fits; the actual socket always lives in the private dir.
pub fn legacy_alias(path: &Path, target: &Path) -> Result<()> {
    if path.as_os_str().as_bytes().len() >= 104 {
        return Ok(());
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.uid() != unsafe { libc::getuid() } {
                bail!("IPC entry {} is owned by another user", path.display());
            }
            if metadata.file_type().is_symlink()
                && std::fs::read_link(path).is_ok_and(|old| old == target)
            {
                return Ok(());
            }
            if metadata.file_type().is_socket()
                && metadata.uid() == unsafe { libc::getuid() }
                && std::os::unix::net::UnixStream::connect(path).is_err()
            {
                std::fs::remove_file(path)?;
            } else {
                bail!("refusing to replace existing IPC entry {}", path.display());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::os::unix::fs::symlink(target, path)?;
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| format!("cannot create {}", path.display()));
        }
    }
    let metadata = std::fs::symlink_metadata(path)?;
    // Never follow a symlink or repair a directory another user could control.
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::getuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        bail!(
            "runtime directory {} must be owned by this user, be a real directory, and have mode 0700",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const RUNTIME: Runtime = Runtime {
        home_directory: ".process-supervisor-tests",
        temporary_prefix: "ps-test",
    };

    #[test]
    fn applications_have_distinct_ipc_namespaces_for_the_same_state_anchor() {
        let other = Runtime {
            home_directory: ".other-supervisor-tests",
            temporary_prefix: "ps-other",
        };
        let state = Path::new("/tmp/shared-state");
        assert_ne!(
            RUNTIME.socket(state, "worker.sock"),
            other.socket(state, "worker.sock")
        );
    }
    use std::os::unix::fs::symlink;

    #[test]
    fn socket_names_stay_short_for_long_database_paths_and_do_not_collide() {
        let db = PathBuf::from(format!("/tmp/{}/hub.db", "long-path/".repeat(100)));
        let first = RUNTIME.socket(&db, "worker.sock");
        assert!(first.as_os_str().as_bytes().len() < 104);
        assert_ne!(
            first,
            RUNTIME.socket(&db.with_file_name("other.db"), "worker.sock")
        );
    }

    #[test]
    fn private_runtime_directories_reject_symlinks_and_shared_permissions() {
        let base = std::env::temp_dir().join(format!("cam-runtime-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir(&base).unwrap();
        let real = base.join("real");
        private_directory(&real).unwrap();
        let link = base.join("link");
        symlink(&real, &link).unwrap();
        assert!(private_directory(&link).is_err());
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(private_directory(&real).is_err());
        assert_eq!(
            std::fs::metadata(&real).unwrap().permissions().mode() & 0o777,
            0o755
        );
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn legacy_aliases_connect_without_overwriting_unrelated_files() {
        let root = std::env::temp_dir().join(format!("cam-alias-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let target = root.join("actual.sock");
        let alias = root.join("legacy.sock");
        let listener = std::os::unix::net::UnixListener::bind(&target).unwrap();
        legacy_alias(&alias, &target).unwrap();
        legacy_alias(&alias, &target).unwrap();
        assert!(std::os::unix::net::UnixStream::connect(&alias).is_ok());
        let unrelated = root.join("unrelated");
        std::fs::write(&unrelated, b"keep").unwrap();
        assert!(legacy_alias(&unrelated, &target).is_err());
        assert_eq!(std::fs::read(&unrelated).unwrap(), b"keep");
        drop(listener);
        std::fs::remove_dir_all(root).unwrap();
    }
}
