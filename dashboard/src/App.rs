//! The dashboard: every agent the hub knows, and every message, live.
//! Written with rust-js's `react` crate, which compiles to JSX. Each
//! component is a module of its own, as each is a file in a React app.

mod agent_list;
mod api;
mod message_list;

use agent_list::AgentList;
use js::spawn;
use message_list::MessageList;
use protocol::{Agent, EVENTS, Message};
use react::{JSX, jsx, use_effect, use_state};
use webapi::{Event, EventTargetExt, MessageEvent, event_source, events};

pub fn App() -> JSX::Element {
    let (agents, set_agents) = use_state(Vec::<Agent>::new);
    let (messages, set_messages) = use_state(Vec::<Message>::new);
    let (selected, set_selected) = use_state(|| None::<String>);
    let (live, set_live) = use_state(|| false);
    let (problem, set_problem) = use_state(|| None::<String>);

    // Every event says something changed: fetch the agents and the history
    // again, so the page is the hub's own answer, whatever it missed.
    use_effect(
        move || {
            let refresh = move || {
                spawn(Box::new(async move {
                    match api::load().await {
                        Ok((agents, messages)) => {
                            set_agents.set(agents);
                            set_messages.set(messages);
                            set_problem.set(None);
                        }
                        Err(reason) => set_problem.set(Some(reason)),
                    }
                }))
            };
            let source = event_source::new(EVENTS, None);
            source.add_event_listener(
                events::Open,
                move |_: &Event| {
                    set_live.set(true);
                    refresh();
                },
                None,
            );
            source.add_event_listener(events::Message, move |_: &MessageEvent| refresh(), None);
            source.add_event_listener(events::Error, move |_: &Event| set_live.set(false), None);
            move || source.close()
        },
        Some(&()),
    );

    let shown: Vec<&Message> = messages
        .iter()
        .filter(|message| {
            selected
                .as_ref()
                .is_none_or(|name| message.from == *name || message.to == *name)
        })
        .collect();
    jsx! {
        <div className="page">
            <header>
                <h1>{"Cross-agent messages"}</h1>
                <span className={if *live { "live on" } else { "live off" }}>
                    {if *live { "Live" } else { "Connecting…" }}
                </span>
            </header>
            {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
            <div className="layout">
                <AgentList agents={agents} selected={selected.as_deref()} onSelect={set_selected} />
                <MessageList messages={shown} />
            </div>
        </div>
    }
}
