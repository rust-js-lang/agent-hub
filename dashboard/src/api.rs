//! The dashboard's calls to the hub, and how it shows a time.

use js::{date, settle};
use protocol::{AGENTS, Agent, MESSAGES, Message, Refusal};
use serde::de::DeserializeOwned;
use webapi::window;

/// The JSON the hub answers at `path`, or why there's none.
async fn get<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let response = match settle(window.fetch(path, None)).await {
        Ok(response) => response,
        Err(_) => {
            return Err(
                "The hub can't be reached: is `cross-agent-message serve` running?".to_string(),
            );
        }
    };
    let status = response.status() as u16;
    let text = response.text().await;
    if status != 200 {
        return Err(match serde_json::from_str::<Refusal>(&text) {
            Ok(refusal) => refusal.reason,
            Err(_) => format!("The hub answered {status}."),
        });
    }
    serde_json::from_str(&text)
        .map_err(|error| format!("The hub's answer isn't what this page reads: {error}"))
}

/// Every agent, and the history, newest first.
pub async fn load() -> Result<(Vec<Agent>, Vec<Message>), String> {
    let agents = get(AGENTS).await?;
    let messages = get(MESSAGES).await?;
    Ok((agents, messages))
}

/// A time in milliseconds since the Unix epoch, in the reader's locale.
pub fn time(at: u64) -> String {
    date::new(Some(&(at as f64))).to_locale_string(None, None)
}
