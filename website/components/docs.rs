js::directive!("use client");

use crate::content::PAGES;
use crate::ui::Code;
use next::link::Link;
use react::event::ChangeEvent;
use react::{JSX, jsx, use_state};

pub struct DocsProps {
    pub slug: String,
}

pub fn Docs(DocsProps { slug }: DocsProps) -> JSX::Element {
    let index = PAGES.iter().position(|page| page.slug == slug);
    let (query, set_query) = use_state(String::new);
    let (menu_open, set_menu_open) = use_state(|| false);
    let needle = query.to_lowercase();
    let found = PAGES
        .iter()
        .any(|page| page.title.to_lowercase().contains(&needle) || page.slug.contains(&needle));
    jsx! {
        <main id="main" className="docs-layout wrap">
            <aside className="docs-sidebar">
                <button type="button" className="docs-menu-toggle" aria-expanded={*menu_open} aria-controls="guide-navigation" onClick={move |_| set_menu_open.set(!*menu_open)}>{"Browse guides"}<span aria-hidden="true">{if *menu_open { "−" } else { "+" }}</span></button>
                <div id="guide-navigation" className={if *menu_open { "docs-navigation expanded" } else { "docs-navigation" }}>
                <label className="search-label" htmlFor="docs-search">{"FIND A GUIDE"}</label>
                <div className="search-box"><span aria-hidden="true">{"⌕"}</span><input id="docs-search" type="search" placeholder="Search guides…" value={query.clone()} onChange={move |e: &ChangeEvent<_>| set_query.set(e.value())} /></div>
                <nav aria-label="Documentation">
                    {["START HERE", "WORK TOGETHER", "OPERATE", "REFERENCE"].iter().map(|group| jsx! {
                        <div className="sidebar-group" key={*group}><h2>{*group}</h2>{PAGES.iter().filter(|page| page.group == *group && (page.title.to_lowercase().contains(&needle) || page.slug.contains(&needle))).map(|page| jsx! {
                            <Link key={page.slug} className={Some(if page.slug == slug { "sidebar-link current" } else { "sidebar-link" })} href={&format!("/docs/{}/", page.slug)} aria-current={Some(react::Booleanish::Text(if page.slug == slug { "page" } else { "false" }))}>{match page.slug {
                                "introduction" => "Introduction", "getting-started" => "Getting started", "messaging" => "Messaging", "channels" => "Channels", "holds" => "Holds", "across-machines" => "Across machines", "dashboard" => "Your dashboard", "commands" => "Command reference", "architecture" => "Architecture", _ => "Development & deploy",
                            }}</Link>
                        }).collect::<Vec<_>>()}</div>
                    }).collect::<Vec<_>>()}
                    {(!found).then(|| jsx! { <p className="no-results" role="status">{"No guides found. Try “channels” or “messaging”."}</p> })}
                </nav>
                <a className="sidebar-source" href="https://github.com/rust-js-lang/cross-agent-message">{"View source on GitHub ↗"}</a>
                </div>
            </aside>
            {match index {
                Some(index) => {
                    let page = &PAGES[index];
                    jsx! {
                        <article className="doc-article">
                            <div className="doc-breadcrumb">{"DOCUMENTATION"}<span>{"/"}</span>{page.group}</div>
                            <h1>{page.title}</h1><p className="doc-description">{page.description}</p>
                            <div className="doc-rule" />
                            {page.sections.iter().enumerate().map(|(i, section)| jsx! {
                                <section key={section.title} className="doc-section" id={format!("section-{i}")}>
                                    <h2>{section.title}</h2><p>{section.prose}</p>
                                    {(!section.code.is_empty()).then(|| jsx! { <Code label="TERMINAL / REFERENCE" text={section.code} /> })}
                                </section>
                            }).collect::<Vec<_>>()}
                            <div className="doc-feedback"><span>{"Something unclear?"}</span><a href="https://github.com/rust-js-lang/cross-agent-message/issues">{"Start a conversation on GitHub ↗"}</a></div>
                            <nav className="doc-pagination" aria-label="Adjacent guides">
                                {(index > 0).then(|| { let previous = &PAGES[index - 1]; jsx! { <Link href={&format!("/docs/{}/", previous.slug)}><small>{"← PREVIOUS"}</small><strong>{previous.title}</strong></Link> } })}
                                {(index + 1 < PAGES.len()).then(|| { let next = &PAGES[index + 1]; jsx! { <Link href={&format!("/docs/{}/", next.slug)}><small>{"UP NEXT →"}</small><strong>{next.title}</strong></Link> } })}
                            </nav>
                        </article>
                    }
                }
                None => jsx! { <article className="doc-article"><div className="eyebrow">{"GUIDE NOT FOUND"}</div><h1>{"Let's get you back on track."}</h1><p className="doc-description">{"That documentation link doesn't match a guide."}</p><Link className={Some("button primary")} href="/docs/introduction/">{"Back to introduction →"}</Link></article> },
            }}
            <aside className="docs-note"><span className="status-dot" /><strong>{"A little context"}</strong><p>{"The hub connects your agents. These guides help you connect the pieces."}</p><Link href="/docs/getting-started/">{"New here? Start with setup ↗"}</Link></aside>
        </main>
    }
}
