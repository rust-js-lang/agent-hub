//! The dashboard: every agent the hub knows, and every message, live.
//! Written with rust-js's `react` crate, which compiles to JSX. Each
//! component is a module of its own, as each is a file in a React app.

mod agent_list;
mod api;
mod compose;
mod message_list;

use agent_list::AgentList;
use compose::Compose;
use js::{clear_interval, date, set_interval, spawn};
use message_list::MessageList;
use protocol::{AddressState, Agent, Channel, EVENTS, ListenAddress, Message};
use react::{JSX, jsx, use_effect, use_ref, use_state};
use webapi::{Event, EventTargetExt, MessageEvent, event_source, events};

pub fn App() -> JSX::Element {
    let (agents, set_agents) = use_state(Vec::<Agent>::new);
    let (channels, set_channels) = use_state(Vec::<Channel>::new);
    let (messages, set_messages) = use_state(Vec::<Message>::new);
    let (selected, set_selected) = use_state(|| None::<String>);
    let (live, set_live) = use_state(|| false);
    let (problem, set_problem) = use_state(|| None::<String>);
    let build = use_ref(None::<String>);
    let activity = use_ref(date::now());
    let (upgraded, set_upgraded) = use_state(|| false);
    let (addresses, set_addresses) = use_state(Vec::<ListenAddress>::new);
    // The person, where they signed in on this device: they write here.
    let (me, set_me) = use_state(api::me);

    use_effect(move || {
        let refresh = move || spawn(Box::new(async move {
            if let Ok(info) = api::hub().await { set_addresses.set(info.addresses); }
        }));
        refresh();
        let timer = set_interval(Box::new(refresh), Some(15_000.0));
        move || clear_interval(Some(timer))
    }, Some(&()));

    // Every event says something changed: fetch the agents and the history
    // again, so the page is the hub's own answer, whatever it missed.
    use_effect(
        move || {
            let refresh = move || {
                spawn(Box::new(async move {
                    match api::load().await {
                        Ok((agents, channels, messages)) => {
                            set_agents.set(agents);
                            set_channels.set(channels);
                            set_messages.set(messages);
                            set_problem.set(None);
                        }
                        Err(reason) => set_problem.set(Some(reason)),
                    }
                }))
            };
            let source =
                event_source::new(&format!("{EVENTS}?types={}", protocol::EVENT_TYPES), None);
            source.add_event_listener(
                events::Open,
                move |_: &Event| {
                    set_live.set(true);
                    spawn(Box::new(async move {
                        if let Ok(info) = api::hub().await {
                            if build
                                .current()
                                .as_ref()
                                .is_some_and(|old| *old != info.build)
                            {
                                set_upgraded.set(true);
                            } else {
                                build.set_current(Some(info.build));
                            }
                        }
                    }));
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

    use_effect(
        move || {
            let timer = set_interval(
                Box::new(move || {
                    if *upgraded
                        && api::draft().is_empty()
                        && (webapi::window.document().hidden()
                            || date::now() - activity.current() >= 30_000.0)
                    {
                        webapi::window.location().reload();
                    }
                }),
                Some(1000.0),
            );
            move || clear_interval(Some(timer))
        },
        Some(&(*upgraded,)),
    );

    // What's sent to the person is read once it's on their page: asked
    // again of each change, as the hub's answer changes with it.
    let unread = me.as_ref().is_some_and(|me| {
        messages
            .iter()
            .any(|message| message.to == me.name && message.read_at.is_none())
    });
    use_effect(
        move || {
            if let (true, Some(me)) = (unread, me.clone()) {
                spawn(Box::new(async move {
                    let _ = api::read(&me).await;
                }));
            }
        },
        Some(&(unread,)),
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
        <div className="page" onInput={move |_| activity.set_current(date::now())} onKeyDown={move |_| activity.set_current(date::now())} onPointerDown={move |_| activity.set_current(date::now())} onPointerMove={move |_| activity.set_current(date::now())}>
            <header>
                <h1>{"Cross-agent messages"}</h1>
                <span className={if *live { "live on" } else { "live off" }}>
                    {if *live { "Live" } else { "Connecting…" }}
                </span>
            </header>
            {(!addresses.is_empty()).then(|| jsx! {
                <ul className="listener-status" aria-label="Hub addresses">
                    {addresses.iter().map(|address| jsx! {
                        <li key={address.address.clone()} className={if address.state == AddressState::Blocked { "blocked" } else { "" }}>
                            <code>{address.address.clone()}</code>{" · "}
                            {match address.state { AddressState::Ready => "ready", AddressState::Pending => "pending", AddressState::Blocked => "blocked", AddressState::Unknown => "unknown" }}
                            {address.remote_seen.then(|| " · seen from another machine")}
                            {(address.state == AddressState::Blocked).then(|| address.reason.clone()).flatten().map(|reason| jsx! { <span className="listener-reason">{reason}</span> })}
                        </li>
                    }).collect::<Vec<_>>()}
                </ul>
            })}
            {(*upgraded).then(|| jsx! {
                <p className="upgrade-notice" role="status">{"The hub was upgraded: "}
                    <button type="button" className="link" onClick={move |_| webapi::window.location().reload()}>{"reload"}</button>
                </p>
            })}
            {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
            <div className="layout">
                <AgentList agents={agents} selected={selected.as_deref()} onSelect={set_selected} me={me.as_ref()} />
                <div className="talk">
                    <Compose
                        agents={agents}
                        channels={channels}
                        selected={selected.as_deref()}
                        me={me.as_ref()}
                        onMe={set_me} />
                    <MessageList messages={shown} />
                </div>
            </div>
        </div>
    }
}
