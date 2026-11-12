use next::link::Link;
use react::{JSX, jsx};

pub fn NotFound() -> JSX::Element {
    jsx! {
        <main id="main" className="wrap section">
            <div className="eyebrow">{"GUIDE NOT FOUND"}</div>
            <h1>{"Let's get you back on track."}</h1>
            <p className="doc-description">{"That documentation link doesn't match a guide."}</p>
            <Link className={Some("button primary")} href="/docs/introduction/">{"Back to introduction →"}</Link>
        </main>
    }
}

js::export_default!(NotFound);
