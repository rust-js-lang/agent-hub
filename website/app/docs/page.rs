use crate::guide::Docs;
use react::{JSX, jsx};

pub fn Documentation() -> JSX::Element {
    jsx! { <Docs slug={"introduction".to_string()} /> }
}

js::export_default!(Documentation);
