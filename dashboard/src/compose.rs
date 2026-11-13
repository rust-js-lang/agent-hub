//! Where the person the agents work for writes to them: signed in once on
//! a device with the hub's key, then a recipient, a message and Send.

use js::spawn;
use protocol::{Agent, Channel};
use react::event::{ChangeEvent, MouseEvent};
use react::{JSX, jsx, use_state};

use crate::api::{self, Me};

/// The key as it's typed, its digits in groups of four, `4821-7730-1182`:
/// a phone's number keys have no dash, so the page writes them.
fn grouped(typed: &str) -> String {
    let digits: Vec<char> = typed
        .chars()
        .filter(|c| c.is_ascii_digit())
        .take(12)
        .collect();
    let groups: Vec<String> = digits
        .chunks(4)
        .map(|group| group.iter().collect())
        .collect();
    groups.join("-")
}

pub struct ComposeProps<'a> {
    pub agents: &'a [Agent],
    pub channels: &'a [Channel],
    /// The agent whose messages are shown, the one to write to unless
    /// another is picked.
    pub selected: Option<&'a str>,
    pub me: Option<&'a Me>,
    pub on_me: &'static react::Dispatch<react::SetStateAction<Option<Me>>>,
}

pub fn Compose(
    ComposeProps {
        agents,
        channels,
        selected,
        me,
        on_me,
    }: ComposeProps,
) -> JSX::Element {
    let (name, set_name) = use_state(String::new);
    let (key, set_key) = use_state(String::new);
    let (picked, set_picked) = use_state(String::new);
    let (body, set_body) = use_state(api::draft);
    let (problem, set_problem) = use_state(|| None::<String>);

    let Some(me) = me else {
        let typed_name = name.clone();
        let typed_key = key.clone();
        let sign_in = move |_: &MouseEvent<_>| {
            let name = typed_name.trim().to_string();
            let key = typed_key.trim().to_string();
            spawn(Box::new(async move {
                match api::sign_in(&name, &key).await {
                    Ok(me) => {
                        on_me.set(Some(me));
                        set_problem.set(None);
                    }
                    Err(reason) => set_problem.set(Some(reason)),
                }
            }))
        };
        return jsx! {
            <section className="compose" aria-label="Write to the agents">
                <h2>{"Write to the agents"}</h2>
                <p className="hint">
                    {"Sign in once on this device, with a name to write as and the hub's key, twelve digits: "}
                    <code>{"agent-hub key"}</code>
                    {" prints it on the hub's machine."}
                </p>
                <div className="fields">
                    <input
                        aria-label="Your name"
                        placeholder="Your name"
                        autoCapitalize="none"
                        autoCorrect="off"
                        value={name.clone()}
                        onChange={move |e: &ChangeEvent<_>| set_name.set(e.value())} />
                    <input
                        aria-label="The hub's key"
                        placeholder="The hub's key, 12 digits"
                        inputMode="numeric"
                        value={key.clone()}
                        onChange={move |e: &ChangeEvent<_>| set_key.set(grouped(&e.value()))} />
                    <button type="button" className="send" onClick={sign_in}>{"Sign in"}</button>
                </div>
                {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
            </section>
        };
    };

    // Whom it goes to: the one picked here, else the agent whose messages
    // are shown, else the first channel, everyone. A channel is written
    // with its `#`; one with no agent in it, and the person, aren't offered.
    let others: Vec<&Agent> = agents.iter().filter(|agent| !agent.person).collect();
    let groups: Vec<(String, String)> = channels
        .iter()
        .filter(|channel| !channel.members.is_empty())
        .map(|channel| {
            let agents = match channel.members.len() {
                1 => "1 agent".to_string(),
                count => format!("{count} agents"),
            };
            (
                format!("#{}", channel.name),
                format!("#{} · {agents}", channel.name),
            )
        })
        .collect();
    let known = |to: &str| {
        others.iter().any(|agent| agent.name == to) || groups.iter().any(|(name, _)| name == to)
    };
    let to = match (picked.as_str(), selected) {
        (picked, _) if known(picked) => picked.to_string(),
        (_, Some(selected)) if known(selected) => selected.to_string(),
        _ => match groups.first() {
            Some((name, _)) => name.clone(),
            None => others
                .first()
                .map(|agent| agent.name.clone())
                .unwrap_or_default(),
        },
    };
    let ready = !to.is_empty() && !body.trim().is_empty();
    let send = {
        let (me, to, body) = (me.clone(), to.clone(), body.clone());
        move |_: &MouseEvent<_>| {
            let (me, to, body) = (me.clone(), to.clone(), body.clone());
            spawn(Box::new(async move {
                match api::send(&me, &to, &body).await {
                    Ok(_) => {
                        api::set_draft("");
                        set_body.set(String::new());
                        set_problem.set(None);
                    }
                    Err(reason) => set_problem.set(Some(reason)),
                }
            }))
        }
    };
    let sign_out = move |_: &MouseEvent<_>| {
        api::sign_out();
        on_me.set(None);
    };
    jsx! {
        <section className="compose" aria-label="Write to the agents">
            <h2>{"Write to the agents"}</h2>
            <p className="hint">
                {format!("Writing as {}. ", me.name)}
                <button type="button" className="link" onClick={sign_out}>{"Sign out"}</button>
            </p>
            <div className="fields">
                <select
                    aria-label="To"
                    value={to.clone()}
                    onChange={move |e: &ChangeEvent<_>| set_picked.set(e.value())}>
                    {groups
                        .iter()
                        .map(|(name, label)| jsx! {
                            <option key={name.clone()} value={name.clone()}>{label.clone()}</option>
                        })
                        .collect::<Vec<_>>()}
                    {others
                        .iter()
                        .map(|agent| jsx! {
                            <option key={agent.name.clone()} value={agent.name.clone()}>{agent.name.clone()}</option>
                        })
                        .collect::<Vec<_>>()}
                </select>
                <textarea
                    aria-label="Message"
                    placeholder="Message"
                    rows={3}
                    value={body.clone()}
                    onChange={move |e: &ChangeEvent<_>| { let value = e.value(); api::set_draft(&value); set_body.set(value); }} />
                <button type="button" className="send" disabled={!ready} onClick={send}>{"Send"}</button>
            </div>
            {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
        </section>
    }
}
