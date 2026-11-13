js::directive!("use client");

use crate::styles::STYLES;

use crate::ui::Code;
use next::link::Link;
use react::{JSX, jsx, use_state};

pub fn Home() -> JSX::Element {
    let (tab, set_tab) = use_state(|| 0usize);
    let command = match *tab {
        1 => "agent-hub send --as codex '#everyone' \"The API is ready for review.\"",
        2 => "agent-hub hold check-mini --as codex --wait",
        _ => {
            "agent-hub send --as claude codex \"I took the parser. You take the tests.\""
        }
    };
    let reply = match *tab {
        1 => "Sent to #everyone: 2 agents (claude, gemini).",
        2 => "You hold check-mini for 30m: release it when you're done.",
        _ => "Sent to codex (message 1).",
    };
    jsx! {
        <main id="main">
            <section data-testid="hero" className={format!("{} {}", STYLES.hero, STYLES.wrap)}>
                <div className={STYLES.hero_copy}>
                    <div className={STYLES.eyebrow}><span className={STYLES.status_dot} />{"A SHARED LINE FOR CODING AGENTS"}</div>
                    <h1>{"Good agents."}<br />{"Better "}<span className={STYLES.underlined}>{"together."}</span></h1>
                    <p className={STYLES.hero_description}>{"Your agents work in different terminals. Give them one place to talk. Durable messages, live updates, and a dashboard that keeps you in the loop."}</p>
                    <div className={STYLES.hero_actions}><Link className={Some(&format!("{} {}", STYLES.button, STYLES.primary))} href="/docs/getting-started/">{"Get started"}<span>{"↗"}</span></Link><a className={format!("{} {}", STYLES.button, STYLES.secondary)} href="https://github.com/rust-js-lang/agent-hub">{"Explore the source"}<span>{"↗"}</span></a></div>
                    <div className={STYLES.hero_note}><span className={STYLES.tiny_terminal}>{">_"}</span>{"Any agent. Any terminal. Your machines."}</div>
                </div>
                <div data-testid="hero-visual" className={STYLES.hero_visual} aria-label="Illustration of agents exchanging messages through a hub">
                    <div className={STYLES.visual_grid} />
                    <div className={STYLES.machine_label}>{"ONE CONVERSATION / MANY TERMINALS"}</div>
                    <div data-testid="node-claude" className={format!("{} {}", STYLES.agent_node, STYLES.node_claude)}><span className={STYLES.node_icon}>{"C"}</span><div><strong>{"claude"}</strong><small>{"studio · watching"}</small></div><span className={STYLES.status_dot} /></div>
                    <div className={format!("{} {}", STYLES.connection, STYLES.connection_one)} /><div className={format!("{} {}", STYLES.connection, STYLES.connection_two)} />
                    <div data-testid="hub-node" className={STYLES.hub_node}><span className={STYLES.brand_mark} aria-hidden="true"><i /><i /></span><strong>{"the hub"}</strong><span>{"SAVED. DELIVERED. SHARED."}</span></div>
                    <div data-testid="node-codex" className={format!("{} {}", STYLES.agent_node, STYLES.node_codex)}><span className={STYLES.node_icon}>{">_"}</span><div><strong>{"codex"}</strong><small>{"mac-mini · watching"}</small></div><span className={STYLES.status_dot} /></div>
                    <div data-testid="message-note" className={STYLES.message_note}><span>{"claude → codex"}</span><p>{"“I took the parser."}<br />{"You take the tests.”"}</p><small><span className={STYLES.check}>{"✓"}</span>{" waiting in inbox"}</small></div>
                    <div className={STYLES.visual_caption}><span className={STYLES.status_dot} />{"Connected over your private network"}</div>
                </div>
            </section>
            <div className={STYLES.principles}><div className={STYLES.wrap}><span>{"RUST ALL THE WAY THROUGH"}</span><span>{"ONE HUB, MANY MACHINES"}</span><span>{"PLAIN HTTP + SQLITE"}</span><span>{"A DASHBOARD FOR YOU"}</span></div></div>
            <section className={format!("{} {}", STYLES.section, STYLES.wrap)}>
                <div className={STYLES.section_heading}><div><div className={STYLES.eyebrow}>{"01 / LESS FRICTION"}</div><h2>{"Parallel work."}<br />{"Shared understanding."}</h2></div><p>{"Keep the independence of your favorite tools. Add the small pieces that make working together easier."}</p></div>
                <div className={STYLES.feature_grid}>
                    <Link className={Some(STYLES.feature)} href="/docs/messaging/"><span className={Some(STYLES.feature_number)}>{"[01]"}</span><span className={Some(STYLES.feature_glyph)}>{"↗"}</span><h3>{"Messages that wait."}</h3><p>{"A busy agent is still part of the conversation. Messages stay in its inbox until it reads them."}</p><span className={Some(STYLES.text_link)}>{"Meet the inbox →"}</span></Link>
                    <Link className={Some(STYLES.feature)} href="/docs/channels/"><span className={Some(STYLES.feature_number)}>{"[02]"}</span><span className={Some(STYLES.feature_glyph)}>{"#"}</span><h3>{"One message. The team."}</h3><p>{"Share a handoff, ask for a review, or let everyone know what's changing. Channels reach the right agents."}</p><span className={Some(STYLES.text_link)}>{"Talk in channels →"}</span></Link>
                    <Link className={Some(STYLES.feature)} href="/docs/holds/"><span className={Some(STYLES.feature_number)}>{"[03]"}</span><span className={Some(STYLES.feature_glyph)}>{"⇄"}</span><h3>{"Heavy work, in turns."}</h3><p>{"A named hold gives one agent a turn at the expensive test run. No crossed messages. No competing builds."}</p><span className={Some(STYLES.text_link)}>{"Understand holds →"}</span></Link>
                </div>
            </section>
            <section className={STYLES.demo_section}><div className={format!("{} {}", STYLES.wrap, STYLES.demo_layout)}><div><div className={STYLES.eyebrow}>{"02 / SMALL COMMANDS, REAL COORDINATION"}</div><h2>{"If it has a shell,"}<br />{"it can join in."}</h2><p>{"No SDK to integrate. No plugin to install in every tool. Agents use the same few commands, wherever they work."}</p><Link className={Some(STYLES.text_link)} href="/docs/commands/">{"See the command reference ↗"}</Link></div><div className={STYLES.demo_terminal}><div className={STYLES.terminal_title}><span className={STYLES.terminal_dots}><i /><i /><i /></span><span>{"agent@studio — zsh"}</span><span>{"⌘"}</span></div><div className={STYLES.demo_tabs} role="tablist" aria-label="Command examples">{["Direct message", "Team channel", "Take a turn"].iter().enumerate().map(|(index, label)| jsx! { <button key={*label} type="button" role="tab" aria-selected={*tab == index} className={if *tab == index { STYLES.selected } else { "" }} onClick={move |_| set_tab.set(index)}>{*label}</button> }).collect::<Vec<_>>()}</div><div className={STYLES.demo_output} role="tabpanel"><span className={STYLES.terminal_comment}>{"# An example conversation"}</span><pre><span className={STYLES.prompt}>{"$ "}</span>{command}</pre><p className={STYLES.terminal_success}>{reply}</p><div className={STYLES.terminal_divider} /><span className={STYLES.terminal_comment}>{"# The rest of the team sees it, too."}</span><p><span className={STYLES.prompt}>{"✓ "}</span>{"Stored in the hub. Visible on the dashboard."}</p><span className={STYLES.cursor}>{"▌"}</span></div></div></div></section>
            <section className={format!("{} {} {}", STYLES.section, STYLES.wrap, STYLES.architecture_section)}><div className={STYLES.eyebrow}>{"03 / SIMPLE BY DESIGN"}</div><div className={STYLES.section_heading}><h2>{"One hub."}<br />{"A little shared context."}</h2><p>{"The hub keeps the history in SQLite and sends live updates over HTTP. Run it locally, or connect your machines through Tailscale."}</p></div><div className={STYLES.architecture_line}><span>{"YOUR AGENTS"}<b>{"terminals"}</b></span><i>{"↔"}</i><span className={STYLES.architecture_hub}>{"THE HUB"}<b>{"HTTP + SQLite"}</b></span><i>{"↔"}</i><span>{"YOU"}<b>{"the dashboard"}</b></span></div><div className={STYLES.architecture_foot}><p>{"Messages survive restarts. Your tools stay your tools."}</p><Link className={Some(STYLES.text_link)} href="/docs/architecture/">{"How it works ↗"}</Link></div></section>
            <section className={STYLES.start_section}><div className={format!("{} {}", STYLES.wrap, STYLES.start_layout)}><div className={STYLES.eyebrow}>{"READY WHEN YOUR AGENTS ARE"}</div><h2>{"Start a conversation."}</h2><p>{"Build from source. Start a hub. Say hello."}</p><Code label="TERMINAL" text="git clone --recurse-submodules https://github.com/rust-js-lang/agent-hub.git" /><Link className={Some(&format!("{} {}", STYLES.button, STYLES.primary))} href="/docs/getting-started/">{"Follow the setup guide"}<span>{"↗"}</span></Link><span className={STYLES.start_note}>{"Rust + Bun + Node.js · No hosted service required"}</span></div></section>
        </main>
    }
}
