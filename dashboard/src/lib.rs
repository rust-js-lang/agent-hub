//! The dashboard: every agent the hub knows, and every message, live.
//! Written with rust-js's `react` crate, which compiles to JSX.

mod api;

use js::spawn;
use protocol::{Agent, EVENTS, Message};
use react::{Dispatch, JSX, SetStateAction, jsx, use_effect, use_state};
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

pub struct AgentListProps<'a> {
    pub agents: &'a [Agent],
    pub selected: Option<&'a str>,
    pub on_select: &'static Dispatch<SetStateAction<Option<String>>>,
}

pub fn AgentList(
    AgentListProps {
        agents,
        selected,
        on_select,
    }: AgentListProps,
) -> JSX::Element {
    let class = |chosen: bool| if chosen { "agent chosen" } else { "agent" };
    jsx! {
        <nav className="agents" aria-label="Agents">
            <h2>{"Agents"}</h2>
            <button type="button" className={class(selected.is_none())} onClick={move |_| on_select.set(None)}>
                <span className="name">{"Everyone"}</span>
                <span className="meta">{format!("{} agents", agents.len())}</span>
            </button>
            {agents
                .iter()
                .map(|agent| {
                    let name = agent.name.clone();
                    let chosen = selected == Some(agent.name.as_str());
                    let state = if agent.watching {
                        "watching".to_string()
                    } else {
                        format!("seen {}", api::time(agent.last_seen))
                    };
                    jsx! {
                        <button
                            key={agent.name.clone()}
                            type="button"
                            className={class(chosen)}
                            onClick={move |_| on_select.set(Some(name.clone()))}>
                            <span className="name">{agent.name.clone()}</span>
                            <span className="meta">{format!("{} · {state}", agent.machine)}</span>
                            {(agent.unread > 0).then(|| jsx! {
                                <span className="unread">{format!("{} unread", agent.unread)}</span>
                            })}
                        </button>
                    }
                })
                .collect::<Vec<_>>()}
        </nav>
    }
}

pub struct MessageListProps<'a> {
    pub messages: Vec<&'a Message>,
}

pub fn MessageList(MessageListProps { messages }: MessageListProps) -> JSX::Element {
    if messages.is_empty() {
        return jsx! { <section className="messages empty">{"No messages yet."}</section> };
    }
    jsx! {
        <section className="messages" aria-label="Messages">
            {messages
                .iter()
                .map(|message| {
                    let read = match message.read_at {
                        Some(at) => format!("read {}", api::time(at)),
                        None => "unread".to_string(),
                    };
                    let class = if message.read_at.is_some() { "message" } else { "message unread" };
                    jsx! {
                        <article key={message.id} className={class}>
                            <div className="route">
                                <span className="from">{message.from.clone()}</span>
                                <span className="arrow">{"→"}</span>
                                <span className="to">{message.to.clone()}</span>
                                <span className="when">{api::time(message.sent_at)}</span>
                            </div>
                            <p className="body">{message.body.clone()}</p>
                            <div className="state">{format!("#{} · {read}", message.id)}</div>
                        </article>
                    }
                })
                .collect::<Vec<_>>()}
        </section>
    }
}
