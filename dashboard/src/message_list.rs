//! Messages, newest first: who sent each to whom, when, and whether it's read.

use protocol::Message;
use react::{JSX, jsx};

use crate::api;

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
                    let class = match (message.read_at.is_some(), message.person) {
                        (true, false) => "message",
                        (false, false) => "message unread",
                        (true, true) => "message person",
                        (false, true) => "message person unread",
                    };
                    jsx! {
                        <article key={message.id} className={class}>
                            <div className="route">
                                <span className="from">{message.from.clone()}</span>
                                {message.person.then(|| jsx! { <span className="badge">{"the person"}</span> })}
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
