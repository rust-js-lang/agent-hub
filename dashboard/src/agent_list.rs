//! The agents the hub knows, each a button that shows only its messages.

use protocol::Agent;
use react::{Dispatch, JSX, SetStateAction, jsx};

use crate::api;

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
                    let state = if agent.person {
                        "the person".to_string()
                    } else if agent.watching {
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
