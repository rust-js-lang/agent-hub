//! The commit this build is of, `CROSS_AGENT_BUILD`, which the hub and the
//! command line each say, so an agent can tell a hub of another build.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Every file under `dir`, by its path from `root` with `/`s, in name order.
fn files(root: &Path, dir: &Path, found: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<PathBuf> = entries.flatten().map(|entry| entry.path()).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            files(root, &path, found);
        } else if let Ok(name) = path.strip_prefix(root) {
            let name: Vec<String> = (name.components())
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            found.push((name.join("/"), path));
        }
    }
}

/// Writes `dashboard.rs`: the dashboard `bun run build` made, `web/dist`,
/// each file's bytes kept in the binary, so a hub built here runs from any
/// directory on any machine. None where it isn't built yet: such a hub
/// serves this checkout's `web/dist` once it is.
fn dashboard() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let dist = manifest.join("../web/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let mut found = Vec::new();
    if dist.join("index.html").exists() {
        files(&dist, &dist, &mut found);
    }
    let rows: String = (found.iter())
        .map(|(name, path)| {
            format!(
                "    ({name:?}, include_bytes!({:?})),\n",
                path.display().to_string()
            )
        })
        .collect();
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap_or_default()).join("dashboard.rs");
    let table = format!("pub static FILES: &[(&str, &[u8])] = &[\n{rows}];\n");
    if let Err(error) = std::fs::write(&out, table) {
        panic!("can't write {}: {error}", out.display());
    }
}

fn main() {
    println!("cargo:rustc-env=CROSS_AGENT_TARGET={}", std::env::var("TARGET").expect("cargo sets TARGET"));
    dashboard();
    let git = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
    };
    let build = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=CROSS_AGENT_BUILD={build}");
    // Built again when the checkout moves to another commit.
    if let Some(dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={dir}/HEAD");
        println!("cargo:rerun-if-changed={dir}/refs/heads");
    }
}
