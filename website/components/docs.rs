js::directive!("use client");

use crate::styles::STYLES;

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
        <main id="main" className={format!("{} {}", STYLES.docs_layout, STYLES.wrap)}>
            <aside className={STYLES.docs_sidebar}>
                <button type="button" className={STYLES.docs_menu_toggle} aria-expanded={*menu_open} aria-controls="guide-navigation" onClick={move |_| set_menu_open.set(!*menu_open)}>{"Browse guides"}<span aria-hidden="true">{if *menu_open { "−" } else { "+" }}</span></button>
                <div id="guide-navigation" className={if *menu_open { format!("{} {}", STYLES.docs_navigation, STYLES.expanded) } else { STYLES.docs_navigation.to_owned() }}>
                <label className={STYLES.search_label} htmlFor="docs-search">{"FIND A GUIDE"}</label>
                <div className={STYLES.search_box}><span aria-hidden="true">{"⌕"}</span><input id="docs-search" type="search" placeholder="Search guides…" value={query.clone()} onChange={move |e: &ChangeEvent<_>| set_query.set(e.value())} /></div>
                <nav aria-label="Documentation">
                    {["START HERE", "WORK TOGETHER", "OPERATE", "REFERENCE"].iter().map(|group| jsx! {
                        <div className={STYLES.sidebar_group} key={*group}><h2>{*group}</h2>{PAGES.iter().filter(|page| page.group == *group && (page.title.to_lowercase().contains(&needle) || page.slug.contains(&needle))).map(|page| jsx! {
                            <Link key={page.slug} className={Some((if page.slug == slug { format!("{} {}", STYLES.sidebar_link, STYLES.current) } else { STYLES.sidebar_link.to_owned() }).as_str())} href={&format!("/docs/{}/", page.slug)} aria-current={Some(react::Booleanish::Text(if page.slug == slug { "page" } else { "false" }))}>{match page.slug {
                                "introduction" => "Introduction", "getting-started" => "Getting started", "messaging" => "Messaging", "channels" => "Channels", "holds" => "Holds", "across-machines" => "Across machines", "dashboard" => "Your dashboard", "commands" => "Command reference", "architecture" => "Architecture", _ => "Development & deploy",
                            }}</Link>
                        }).collect::<Vec<_>>()}</div>
                    }).collect::<Vec<_>>()}
                    {(!found).then(|| jsx! { <p className={STYLES.no_results} role="status">{"No guides found. Try “channels” or “messaging”."}</p> })}
                </nav>
                <a className={STYLES.sidebar_source} href="https://github.com/rust-js-lang/agent-hub">{"View source on GitHub ↗"}</a>
                </div>
            </aside>
            {match index {
                Some(index) => {
                    let page = &PAGES[index];
                    jsx! {
                        <article data-testid="doc-article" className={STYLES.doc_article}>
                            <div className={STYLES.doc_breadcrumb}>{"DOCUMENTATION"}<span>{"/"}</span>{page.group}</div>
                            <h1>{page.title}</h1><p className={STYLES.doc_description}>{page.description}</p>
                            <div className={STYLES.doc_rule} />
                            {page.sections.iter().enumerate().map(|(i, section)| jsx! {
                                <section key={section.title} className={STYLES.doc_section} id={format!("section-{i}")}>
                                    <h2>{section.title}</h2><p>{section.prose}</p>
                                    {(!section.code.is_empty()).then(|| jsx! { <Code label="TERMINAL / REFERENCE" text={section.code} /> })}
                                </section>
                            }).collect::<Vec<_>>()}
                            <div className={STYLES.doc_feedback}><span>{"Something unclear?"}</span><a href="https://github.com/rust-js-lang/agent-hub/issues">{"Start a conversation on GitHub ↗"}</a></div>
                            <nav className={STYLES.doc_pagination} aria-label="Adjacent guides">
                                {(index > 0).then(|| { let previous = &PAGES[index - 1]; jsx! { <Link href={&format!("/docs/{}/", previous.slug)}><small>{"← PREVIOUS"}</small><strong>{previous.title}</strong></Link> } })}
                                {(index + 1 < PAGES.len()).then(|| { let next = &PAGES[index + 1]; jsx! { <Link href={&format!("/docs/{}/", next.slug)}><small>{"UP NEXT →"}</small><strong>{next.title}</strong></Link> } })}
                            </nav>
                        </article>
                    }
                }
                None => jsx! { <article data-testid="doc-article" className={STYLES.doc_article}><div className={STYLES.eyebrow}>{"GUIDE NOT FOUND"}</div><h1>{"Let's get you back on track."}</h1><p className={STYLES.doc_description}>{"That documentation link doesn't match a guide."}</p><Link className={Some(&format!("{} {}", STYLES.button, STYLES.primary))} href="/docs/introduction/">{"Back to introduction →"}</Link></article> },
            }}
            <aside className={STYLES.docs_note}><span className={STYLES.status_dot} /><strong>{"A little context"}</strong><p>{"The hub connects your agents. These guides help you connect the pieces."}</p><Link href="/docs/getting-started/">{"New here? Start with setup ↗"}</Link></aside>
        </main>
    }
}
