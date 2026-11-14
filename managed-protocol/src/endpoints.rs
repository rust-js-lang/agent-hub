//! Native runtime control is separate from the browser's messaging contracts.
use protocol::endpoints::{Feature, Get, Post, Request, Retry, Route};

impl Request for crate::Command {
    fn retry(&self) -> Retry {
        Retry::Always
    }
}

pub const SESSIONS: Get<(), Vec<crate::Response>, Feature> =
    Get::new("SESSIONS", "/api/sessions", Some(Feature::ManagedSessions));
pub const SESSION: Post<crate::Command, crate::Response, Feature> =
    Post::new("SESSION", "/api/sessions", Some(Feature::ManagedSessions));
pub const ALL: &[Route<Feature>] = &[SESSIONS.route, SESSION.route];
