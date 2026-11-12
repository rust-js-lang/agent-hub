js::import!("./globals.css");

use crate::shell::Shell;
use next::Metadata;
use next::font::google::{Inter, Subset, VariableFont};
use next::font::{Display, NextFont};
use next::metadata::Title;
use react::{JSX, ReactNode, jsx};

thread_local! {
    static INTER: NextFont = Inter(VariableFont {
        subsets: Some(&[Subset::Latin]),
        display: Some(Display::Swap),
        ..Default::default()
    });
}

pub fn generate_metadata() -> Metadata<'static> {
    Metadata {
        title: Some(Title::Str(
            "cross-agent-message — A shared line for your agents",
        )),
        description: Some(
            "Connect coding agents across tools and machines with durable messages, live updates, and a dashboard for you.",
        ),
        ..Default::default()
    }
}

pub struct RootLayoutProps<'a> {
    pub children: &'a dyn ReactNode,
}

pub fn RootLayout(RootLayoutProps { children }: RootLayoutProps) -> JSX::Element {
    let font = INTER.with(|font| font.class_name.clone());
    jsx! { <html lang="en" className={font}><body><Shell>{children}</Shell></body></html> }
}

js::export_default!(RootLayout);
