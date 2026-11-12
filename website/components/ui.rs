js::directive!("use client");

use crate::styles::STYLES;

use js::{settle, spawn};
use react::{JSX, jsx, use_state};
use webapi::window;

pub struct CodeProps {
    pub text: &'static str,
    pub label: &'static str,
}

pub fn Code(CodeProps { text, label }: CodeProps) -> JSX::Element {
    let (status, set_status) = use_state(|| "Copy");
    jsx! {
        <div data-testid="code-block" className={STYLES.code_block}>
            <div className={STYLES.code_bar}>
                <span>{label}</span>
                <button type="button" aria-label="Copy command" onClick={move |_| {
                    spawn(Box::new(async move {
                        match settle(window.navigator().clipboard().write_text(text)).await {
                            Ok(()) => set_status.set("Copied!"),
                            Err(_) => set_status.set("Select to copy"),
                        }
                    }));
                }}>{*status}</button>
            </div>
            <pre><code>{text}</code></pre>
            <span className={STYLES.sr_only} role="status">{*status}</span>
        </div>
    }
}
