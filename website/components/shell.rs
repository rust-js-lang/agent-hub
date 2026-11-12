//! Shared App Router shell. Next owns routing; this client boundary owns focus.
js::directive!("use client");

use next::link::Link;
use next::navigation::use_pathname;
use react::event::MouseEvent;
use react::{JSX, ReactNode, jsx, use_ref};

pub struct ShellProps<'a> {
    pub children: &'a dyn ReactNode,
}

pub fn Shell(ShellProps { children }: ShellProps) -> JSX::Element {
    let pathname = use_pathname();
    let is_docs = pathname.contains("/docs");
    let content = use_ref::<Option<&'static webapi::HTMLElement>>(None);
    jsx! {
        <>
            <a className="skip-link" href="#content" onClick={move |e: &MouseEvent<_>| {
                e.prevent_default();
                if let Some(target) = content.current() { target.focus(None); }
            }}>{"Skip to content"}</a>
            <header className="site-header">
                <div className="nav-wrap">
                    <Link className={Some("brand")} href="/" aria-label={Some("cross-agent-message home")}>
                        <span className="brand-mark" aria-hidden="true"><i /><i /></span>
                        <span>{"cross-agent-message"}<span className="brand-dot">{"."}</span></span>
                    </Link>
                    <nav aria-label="Main navigation">
                        <Link className={Some(if is_docs { "nav-link active" } else { "nav-link" })} href="/docs/introduction/">{"Documentation"}</Link>
                        <a className="nav-github" href="https://github.com/rust-js-lang/cross-agent-message">{"GitHub"}<span aria-hidden="true">{" ↗"}</span></a>
                    </nav>
                </div>
            </header>
            <div id="content" ref={content} tabIndex={-1}>{children}</div>
            <footer className="site-footer">
                <div><Link className={Some("footer-brand")} href="/">{"cross-agent-message"}</Link><p>{"Independent agents. Shared context."}</p></div>
                <div><span>{"Built with Rust. Made for collaboration."}</span><a href="https://github.com/rust-js-lang/cross-agent-message">{"Source code ↗"}</a></div>
            </footer>
        </>
    }
}
