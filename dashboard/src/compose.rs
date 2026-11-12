//! Where the person the agents work for writes to them: signed in once on
//! a device with the hub's key, then a recipient, a message and Send.

use js::spawn;
use protocol::Agent;
use react::event::{ChangeEvent, MouseEvent};
use react::{JSX, jsx, use_state};

use crate::api::{self, Me};

pub struct ComposeProps<'a> {
    pub agents: &'a [Agent],
    /// The agent whose messages are shown, the one to write to unless
    /// another is picked.
    pub selected: Option<&'a str>,
    pub me: Option<&'a Me>,
    pub on_me: &'static react::Dispatch<react::SetStateAction<Option<Me>>>,
}

pub fn Compose(
    ComposeProps {
        agents,
        selected,
        me,
        on_me,
    }: ComposeProps,
) -> JSX::Element {
    let (name, set_name) = use_state(String::new);
    let (key, set_key) = use_state(String::new);
    let (picked, set_picked) = use_state(String::new);
    let (body, set_body) = use_state(String::new);
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
                    {"Sign in once on this device, with a name to write as and the hub's key: "}
                    <code>{"cross-agent-message key"}</code>
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
                        placeholder="The hub's key"
                        inputMode="numeric"
                        value={key.clone()}
                        onChange={move |e: &ChangeEvent<_>| set_key.set(e.value())} />
                    <button type="button" className="send" onClick={sign_in}>{"Sign in"}</button>
                </div>
                {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
            </section>
        };
    };

    // Whom it goes to: the one picked here, else the one whose messages
    // are shown, else the first there is. Never the person themself.
    let others: Vec<&Agent> = agents.iter().filter(|agent| !agent.person).collect();
    let known = |name: &str| others.iter().any(|agent| agent.name == name);
    let to = match (picked.as_str(), selected) {
        (picked, _) if known(picked) => picked.to_string(),
        (_, Some(selected)) if known(selected) => selected.to_string(),
        _ => others
            .first()
            .map(|agent| agent.name.clone())
            .unwrap_or_default(),
    };
    let ready = !to.is_empty() && !body.trim().is_empty();
    let send = {
        let (me, to, body) = (me.clone(), to.clone(), body.clone());
        move |_: &MouseEvent<_>| {
            let (me, to, body) = (me.clone(), to.clone(), body.clone());
            spawn(Box::new(async move {
                match api::send(&me, &to, &body).await {
                    Ok(_) => {
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
                    onChange={move |e: &ChangeEvent<_>| set_body.set(e.value())} />
                <button type="button" className="send" disabled={!ready} onClick={send}>{"Send"}</button>
            </div>
            {problem.clone().map(|reason| jsx! { <p className="problem" role="alert">{reason}</p> })}
        </section>
    }
}
