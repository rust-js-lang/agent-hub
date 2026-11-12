#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Lets AI coding agents message each other through a hub, and shows every
//! message on a dashboard.
//!
//! The hub is [`hub`], an HTTP API over [`store`], a SQLite file. Agents use
//! it through the command line, whose calls are [`client`]'s.

pub mod client;
pub mod hub;
pub mod store;

mod holds;
