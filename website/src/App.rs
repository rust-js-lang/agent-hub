//! The public landing page and documentation, independently built from the hub.
mod content;
mod docs;
mod home;
mod ui;

use react::event::MouseEvent;
use react::{JSX, jsx, use_effect, use_ref, use_state};
use webapi::{EventTargetExt, HashChangeEvent, events, window};

fn route() -> String {
    window.location().hash()
}

pub fn App() -> JSX::Element {
    let (page, set_page) = use_state(route);
    let content = use_ref::<Option<&'static webapi::HTMLElement>>(None);
    use_effect(
        move || {
            let listener = webapi::listener(move |_: &HashChangeEvent| {
                set_page.set(route());
            });
            window.add_event_listener(events::Hashchange, listener, None);
            move || window.remove_event_listener(events::Hashchange, listener, None)
        },
        Some(&()),
    );
    let is_docs = page.starts_with("#docs");
    jsx! {
        <>
            <a className="skip-link" href="#content" onClick={move |e: &MouseEvent<_>| {
                e.prevent_default();
                if let Some(target) = content.current() { target.focus(None); }
            }}>{"Skip to content"}</a>
            <header className="site-header">
                <div className="nav-wrap">
                    <a className="brand" href="#" aria-label="cross-agent-message home">
                        <span className="brand-mark" aria-hidden="true"><i /><i /></span>
                        <span>{"cross-agent-message"}<span className="brand-dot">{"."}</span></span>
                    </a>
                    <nav aria-label="Main navigation">
                        <a className={if is_docs { "nav-link active" } else { "nav-link" }} href="#docs/introduction">{"Documentation"}</a>
                        <a className="nav-github" href="https://github.com/rust-js-lang/cross-agent-message">{"GitHub"}<span aria-hidden="true">{" ↗"}</span></a>
                    </nav>
                </div>
            </header>
            <div id="content" ref={content} tabIndex={-1}>
            {if is_docs {
                jsx! { <docs::Docs route={page.clone()} /> }
            } else {
                jsx! { <home::Home /> }
            }}
            </div>
            <footer className="site-footer">
                <div><a className="footer-brand" href="#">{"cross-agent-message"}</a><p>{"Independent agents. Shared context."}</p></div>
                <div><span>{"Built with Rust. Made for collaboration."}</span><a href="https://github.com/rust-js-lang/cross-agent-message">{"Source code ↗"}</a></div>
            </footer>
        </>
    }
}
