use crate::content::PAGES;
use crate::guide::Docs;
use next::Metadata;
use next::metadata::Title;
use next::navigation::not_found;
use react::{JSX, jsx};

pub struct Params {
    pub slug: String,
}

pub struct GuideProps {
    pub params: js::Promise<Params>,
}

#[cfg_attr(rust_js, rust_js::name = "dynamicParams")]
pub static DYNAMIC_PARAMS: bool = false;

pub fn generate_static_params() -> Vec<Params> {
    PAGES
        .iter()
        .map(|page| Params {
            slug: page.slug.to_string(),
        })
        .collect()
}

pub async fn generate_metadata(GuideProps { params }: GuideProps) -> Metadata<'static> {
    let Params { slug } = params.await;
    let Some(page) = PAGES.iter().find(|page| page.slug == slug) else {
        not_found()
    };
    Metadata {
        title: Some(Title::Str(page.title)),
        description: Some(page.description),
        ..Default::default()
    }
}

pub async fn GuidePage(GuideProps { params }: GuideProps) -> JSX::Element {
    let Params { slug } = params.await;
    if !PAGES.iter().any(|page| page.slug == slug) {
        not_found();
    }
    jsx! { <Docs key={slug.clone()} slug={slug} /> }
}

js::export_default!(GuidePage);
