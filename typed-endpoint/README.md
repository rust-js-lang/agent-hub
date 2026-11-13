# typed-endpoint

Shared Rust HTTP endpoint contracts with optional Axum server adapters. The
default build has no dependencies and can be compiled to JavaScript by rust-js.
Enable the `axum` feature in the server; keep it disabled in browser consumers.
This is an internal workspace crate, not a published package.

```rust
use typed_endpoint::{Get, Post};

const READ: Get<(), String> = Get::new("READ", "/value", None);
const WRITE: Post<String, String> = Post::new("WRITE", "/value", None);

assert_eq!(READ.url(&()), "/value");
assert_eq!(WRITE.url(), "/value");
```

An endpoint fixes its method, request and response types. `Get`, `Stream` and
`Download` use a `Query`; `Post` carries a body. The optional third type parameter
is the application's feature/capability type (`Copy` for the server adapter);
the library does not define or enforce features. Paths are static Axum paths and query pairs are percent-encoded
as UTF-8, preserving order and empty values. Query parsers receive decoded pairs;
defaults, duplicate handling and validation belong to the application.

With `features = ["axum"]`, `axum::Routes<State, Error, Capability>` registers
typed handlers. The state can be any Axum-compatible state, and the application
supplies an error implementing `TransportError` and `IntoResponse`. Extractor
failures keep their HTTP status and message but use that application's response
shape. Business errors pass through unchanged. GET/POST body and response types
are checked against the handler signature; downloads preserve the handler's
headers and bytes, and streams encode each item as JSON SSE with keep-alives.
`post_empty` deliberately ignores the body, accepting both no body and JSON null.

Duplicate method/path registration is rejected. `get_variants` supports two
contracts on one path only when exactly one query parser accepts the request;
ambiguous or invalid queries return the application-shaped HTTP 400 error.
`with_stream_validation` installs application-owned header/query protocol
validation before a stream handler runs. Without it, streams have no additional
version check. Ordinary routes can use application middleware for version checks.

The crate owns no endpoint catalog, wire data model, refusal format, protocol
version, authentication, feature negotiation, retry policy, nonce generation or
HTTP client. Those remain in the application and its native/browser transports.
It does not depend on the messaging protocol, hub, SQLite or process supervisor.

The independent service in `tests/adapters.rs` uses its own state, capability,
request/response and JSON errors. It checks all transports, malformed requests,
query variants, registration guards and stream validation before side effects.
Documentation tests pair a valid handler with compile-time refusals for wrong
methods, bodies and response types. The hub retains its original route and
native/browser client contract tests to verify the extraction preserves its wire.
