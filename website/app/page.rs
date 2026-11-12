//! Next.js App Router entry. Every Rust route is a module of this crate.
#[path = "../lib/content.rs"]
pub mod content;
#[path = "../components/docs.rs"]
pub mod guide;
#[path = "../components/home.rs"]
pub mod home;
pub mod layout;
#[path = "not-found.rs"]
pub mod not_found;
#[path = "../components/shell.rs"]
pub mod shell;
#[path = "../styles/mod.rs"]
pub mod styles;
#[path = "../components/ui.rs"]
pub mod ui;
pub mod docs {
    pub mod page;
}
#[path = "docs/[slug]/page.rs"]
pub mod guide_page;

use react::{JSX, jsx};

pub fn HomePage() -> JSX::Element {
    jsx! { <home::Home /> }
}

js::export_default!(HomePage);
