use crate::styles::STYLES;
use next::link::Link;
use react::{JSX, jsx};

pub fn NotFound() -> JSX::Element {
    jsx! {
        <main id="main" className={format!("{} {}", STYLES.wrap, STYLES.section)}>
            <div className={STYLES.eyebrow}>{"GUIDE NOT FOUND"}</div>
            <h1>{"Let's get you back on track."}</h1>
            <p className={STYLES.doc_description}>{"That documentation link doesn't match a guide."}</p>
            <Link className={Some(&format!("{} {}", STYLES.button, STYLES.primary))} href="/docs/introduction/">{"Back to introduction →"}</Link>
        </main>
    }
}

js::export_default!(NotFound);
