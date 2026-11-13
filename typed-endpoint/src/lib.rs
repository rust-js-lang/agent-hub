//! Typed HTTP endpoint contracts shared by clients and server adapters.
//! Application endpoints, feature values and wire models remain application-owned.
//!
//! ```
//! use typed_endpoint::{Get, Post};
//! const READ: Get<(), String> = Get::new("READ", "/value", None);
//! const WRITE: Post<String, String> = Post::new("WRITE", "/value", None);
//! assert_eq!(READ.url(&()), "/value");
//! assert_eq!(WRITE.url(), "/value");
//! ```
//!
//! Methods, request types and response types cannot be interchanged:
//! ```compile_fail
//! use typed_endpoint::{Get, Post};
//! const WRITE: Post<u32, String> = Post::new("WRITE", "/value", None);
//! let _: &Get<u32, String> = &WRITE;
//! ```
//! ```compile_fail
//! use typed_endpoint::Post;
//! const WRITE: Post<u32, String> = Post::new("WRITE", "/value", None);
//! fn call<B, R>(_: &Post<B, R>, _: B) -> R { unimplemented!() }
//! let _: String = call(&WRITE, "wrong body");
//! ```
//! ```compile_fail
//! use typed_endpoint::Post;
//! const WRITE: Post<u32, String> = Post::new("WRITE", "/value", None);
//! fn call<B, R>(_: &Post<B, R>, _: B) -> R { unimplemented!() }
//! let _: u32 = call(&WRITE, 1);
//! ```

#[cfg(feature = "axum")]
pub mod axum;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Json,
    Stream,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route<F = ()> {
    pub name: &'static str,
    pub method: Method,
    pub path: &'static str,
    pub kind: Kind,
    pub needs: Option<F>,
}

macro_rules! endpoint_kind {
    ($kind:ident, $method:ident, $wire:ident) => {
        pub struct $kind<Req, Res, F = ()> {
            pub route: Route<F>,
            types: std::marker::PhantomData<fn(Req) -> Res>,
        }

        impl<Req, Res, F> $kind<Req, Res, F> {
            pub const fn new(name: &'static str, path: &'static str, needs: Option<F>) -> Self {
                Self {
                    route: Route {
                        name,
                        method: Method::$method,
                        path,
                        kind: Kind::$wire,
                        needs,
                    },
                    types: std::marker::PhantomData,
                }
            }
        }
    };
}

endpoint_kind!(Get, Get, Json);
endpoint_kind!(Post, Post, Json);
endpoint_kind!(Stream, Get, Stream);
endpoint_kind!(Download, Get, Download);

macro_rules! query_url {
    ($($kind:ident),*) => { $(
        impl<Q: Query, Res, F> $kind<Q, Res, F> {
            pub fn url(&self, query: &Q) -> String { url(self.route.path, query) }
        }
    )* };
}
query_url!(Get, Stream, Download);

impl<B, Res, F> Post<B, Res, F> {
    pub fn url(&self) -> &'static str {
        self.route.path
    }
}

pub trait Query: Sized {
    fn pairs(&self) -> Vec<(&'static str, String)>;
    fn parse(pairs: &[(String, String)]) -> Result<Self, String>;

    /// Streams carry their protocol here because EventSource cannot set a
    /// request header. Ordinary endpoint queries make no protocol claim.
    fn protocol(&self) -> Option<u32> {
        None
    }
}

impl Query for () {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }

    fn parse(_: &[(String, String)]) -> Result<Self, String> {
        Ok(())
    }
}

fn encode(text: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for &byte in text.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 15) as usize] as char);
        }
    }
    encoded
}

pub fn url(path: &str, query: &impl Query) -> String {
    let pairs = query.pairs();
    let mut result = path.to_string();
    for (index, (key, value)) in pairs.iter().enumerate() {
        result.push(if index == 0 { '?' } else { '&' });
        result.push_str(&encode(key));
        result.push('=');
        result.push_str(&encode(value));
    }
    result
}
