#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Lets AI coding agents message each other through a hub, and shows every
//! message on a dashboard.
//!
//! The hub is [`hub`], an HTTP API over [`store`], a SQLite file. Agents use
//! it through the command line, whose calls are [`client`]'s.

pub mod client;
pub mod dashboard;
pub mod hub;
pub mod store;

pub mod addresses;
mod handoff;
mod holds;
pub mod installation;
mod runtime;

pub mod updates;

pub mod supervisor;
pub mod worker;

pub mod service;
pub mod upgrade;
