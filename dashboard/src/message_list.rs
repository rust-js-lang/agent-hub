//! Messages, newest first: who sent each to whom, when, and whether it's
//! read. One sent to a channel is shown once, with how many of the agents
//! it went to have read it.

use protocol::Message;
use react::{JSX, jsx};

use crate::api;

pub struct MessageListProps<'a> {
    pub messages: Vec<&'a Message>,
}

/// A message as it's shown: one sent to an agent, or every copy of one
/// sent to a channel, its recipients' together.
struct Shown<'a> {
    first: &'a Message,
    copies: usize,
    read: usize,
    away: usize,
    active_read: usize,
}

/// `messages` as they're shown: the copies of one sent to a channel, which
/// share its sender, channel, time and words, as one.
fn shown<'a>(messages: &[&'a Message]) -> Vec<Shown<'a>> {
    let mut shown: Vec<Shown<'a>> = Vec::new();
    for &message in messages {
        let read = usize::from(message.read_at.is_some());
        let same = shown.iter_mut().find(|other| {
            message.channel.is_some()
                && other.first.channel == message.channel
                && other.first.from == message.from
                && other.first.sent_at == message.sent_at
                && other.first.body == message.body
        });
        match same {
            Some(other) => {
                other.copies += 1;
                other.read += read;
                other.active_read += usize::from(message.read_at.is_some() && !message.away);
                other.away += usize::from(message.away);
            }
            None => shown.push(Shown {
                first: message,
                copies: 1,
                read,
                away: usize::from(message.away),
                active_read: usize::from(message.read_at.is_some() && !message.away),
            }),
        }
    }
    shown
}

pub fn MessageList(MessageListProps { messages }: MessageListProps) -> JSX::Element {
    if messages.is_empty() {
        return jsx! { <section className="messages empty">{"No messages yet."}</section> };
    }
    jsx! {
        <section className="messages" aria-label="Messages">
            {shown(&messages)
                .iter()
                .map(|group| {
                    let message = group.first;
                    let (to, state) = match (&message.channel, message.read_at) {
                        (Some(channel), _) => (
                            format!("#{channel}"),
                            if group.away > 0 {
                                format!("read by {} of {}, {} away", group.active_read, group.copies - group.away, group.away)
                            } else { format!("read by {} of {}", group.read, group.copies) },
                        ),
                        (None, Some(at)) => (message.to.clone(), format!("read {}", api::time(at))),
                        (None, None) => (message.to.clone(), "unread".to_string()),
                    };
                    let class = match (group.active_read == group.copies - group.away, message.person) {
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
                                <span className="to">{to}</span>
                                <span className="when">{api::time(message.sent_at)}</span>
                            </div>
                            <p className="body">{message.body.clone()}</p>
                            <div className="state">{format!("#{} · {state}", message.id)}</div>
                        </article>
                    }
                })
                .collect::<Vec<_>>()}
        </section>
    }
}
