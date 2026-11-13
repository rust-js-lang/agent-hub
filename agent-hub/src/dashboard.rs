//! Where the hub's dashboard is served from: the files kept in this binary
//! (`build.rs`, of a `web/dist` built before it), so a downloaded hub needs
//! no checkout beside it; or a directory, this checkout's `web/dist`, of a
//! hub built before its dashboard was.

use std::path::PathBuf;

include!(concat!(env!("OUT_DIR"), "/dashboard.rs"));

/// Where a hub serves its dashboard from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dashboard {
    /// The files this binary was built with.
    Embedded,
    /// A directory, read at each request.
    Dir(PathBuf),
}

impl From<PathBuf> for Dashboard {
    fn from(dir: PathBuf) -> Dashboard {
        Dashboard::Dir(dir)
    }
}

/// Whether this binary was built with a dashboard to serve.
pub fn embedded() -> bool {
    find(FILES, "").is_some()
}

/// The file of `files` a request for `path` is answered with, and its
/// content type: `index.html` of the dashboard's own address.
pub fn find<'a>(files: &[(&str, &'a [u8])], path: &str) -> Option<(&'static str, &'a [u8])> {
    let path = match path.trim_start_matches('/') {
        "" => "index.html",
        path => path,
    };
    (files.iter())
        .find(|(name, _)| *name == path)
        .map(|(name, bytes)| (content_type(name), *bytes))
}

/// What a browser is told `name` is, by its extension.
fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILES: &[(&str, &[u8])] = &[
        ("index.html", b"<html>"),
        ("assets/index-1a2b.js", b"js"),
        ("assets/index-3c4d.css", b"css"),
    ];

    #[test]
    fn the_dashboards_own_address_is_its_index() {
        let html = Some(("text/html; charset=utf-8", &b"<html>"[..]));
        assert_eq!(find(FILES, ""), html);
        assert_eq!(find(FILES, "/"), html);
        assert_eq!(find(FILES, "/index.html"), html);
    }

    #[test]
    fn a_file_is_found_by_its_path_with_its_type() {
        assert_eq!(
            find(FILES, "/assets/index-1a2b.js"),
            Some(("text/javascript; charset=utf-8", &b"js"[..]))
        );
        assert_eq!(
            find(FILES, "assets/index-3c4d.css"),
            Some(("text/css; charset=utf-8", &b"css"[..]))
        );
    }

    #[test]
    fn what_isnt_a_file_isnt_found() {
        assert_eq!(find(FILES, "/assets"), None);
        assert_eq!(find(FILES, "/../Cargo.toml"), None);
        assert_eq!(find(&[], ""), None);
    }
}
