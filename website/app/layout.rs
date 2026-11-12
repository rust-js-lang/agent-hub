js::import!("./globals.css");

use crate::shell::Shell;
use next::Metadata;
use next::metadata::Title;
use react::{JSX, ReactNode, jsx};

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
    jsx! { <html lang="en"><body><Shell>{children}</Shell></body></html> }
}

js::export_default!(RootLayout);
