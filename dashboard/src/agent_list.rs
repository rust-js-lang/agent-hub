//! The agents the hub knows, each a button that shows only its messages.

use js::spawn;
use protocol::Agent;
use react::{Dispatch, JSX, SetStateAction, jsx, use_state};

use crate::api;

pub struct AgentListProps<'a> {
    pub agents: &'a [Agent],
    pub selected: Option<&'a str>,
    pub me: Option<&'a api::Me>,
    pub on_select: &'static Dispatch<SetStateAction<Option<String>>>,
}

pub fn AgentList(
    AgentListProps {
        agents,
        selected,
        on_select,
        me,
    }: AgentListProps,
) -> JSX::Element {
    let (problem, set_problem) = use_state(|| None::<String>);
    let class = |chosen: bool| if chosen { "agent chosen" } else { "agent" };
    let row = |agent: &Agent| {
        let name = agent.name.clone();
        let retire_name = agent.name.clone();
        let chosen = selected == Some(agent.name.as_str());
        let state = if agent.person {
            "the person".to_string()
        } else if agent.watching {
            "watching".to_string()
        } else {
            format!("seen {}", api::time(agent.last_seen))
        };
        let me = me.cloned();
        jsx! {
            <div key={agent.name.clone()} className="agent-row">
                <button type="button" className={class(chosen)} onClick={move |_| on_select.set(Some(name.clone()))}>
                    <span className="name">{agent.name.clone()}</span>
                    <span className="meta">{format!("{} · {state}", agent.machine)}</span>
                    {(agent.unread > 0).then(|| jsx! { <span className="unread">{format!("{} unread", agent.unread)}</span> })}
                </button>
                {(agent.away && me.is_some()).then(|| jsx! {
                    <button type="button" className="retire" aria-label={format!("Retire {}", agent.name)} onClick={move |_| {
                        let name = retire_name.clone();
                        let me = me.clone();
                        spawn(Box::new(async move {
                            if let Some(me) = me {
                                match api::retire(&me, &name).await {
                                    Ok(()) => set_problem.set(None),
                                    Err(reason) => set_problem.set(Some(reason)),
                                }
                            }
                        }));
                    }}>{"Retire"}</button>
                })}
            </div>
        }
    };
    let away = agents.iter().filter(|agent| agent.away).count();
    jsx! {
        <nav className="agents" aria-label="Agents">
            <h2>{"Agents"}</h2>
            <button type="button" className={class(selected.is_none())} onClick={move |_| on_select.set(None)}>
                <span className="name">{"Everyone"}</span>
                <span className="meta">{format!("{} agents", agents.len())}</span>
            </button>
            {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
            {agents.iter().filter(|agent| !agent.away).map(row).collect::<Vec<_>>()}
            {(away > 0).then(|| jsx! {
                <details className="away-agents">
                    <summary>{format!("Away ({away})")}</summary>
                    {agents.iter().filter(|agent| agent.away).map(row).collect::<Vec<_>>()}
                </details>
            })}
        </nav>
    }
}
