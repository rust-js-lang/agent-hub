# process-supervisor

A reusable Unix process supervisor, extracted from agent-hub. Link it
into a service's own executable. It does not require a separate daemon, SQLite,
Axum, the hub protocol, or the dashboard. The supported deployment target is
currently macOS Apple Silicon; this extraction does not establish support for
other operating systems.

| Library owns | Application owns |
| --- | --- |
| Listener binding, inheritance and lifetime | Worker entry point and request handling |
| Stable installed executable, lock, immutable recovery images and atomic activation | Candidate identity, target and application protocol validation |
| Drain/resume/retire protocol and correlation IDs | Finish requests and release exclusive state before reporting drained |
| Parked worker lifetime, serving commands, EOF and internal Resume replies | Finish serving when the generation emits Stop |
| Per-address first acceptance, readiness validation and retirement grace | Full service/state probes for each address; report acceptance timestamps |
| Exposure deadline, safe admission, rollback and process reaping | Disposable-state migration/write/read checks before touching live state |
| Private IPC directory safety and namespacing | Stable namespace, CLI/environment conventions and announcements |

`Application` is the integration boundary. Its serializable settings are
flattened into `Config<Application>` alongside process configuration. `Config.db`
is the canonical application state anchor used to name configuration, IPC and
recovery files; the library never opens it as a database. An application can use
a non-database state path. Settings must not reuse the process configuration's
field names. Keep the application name and runtime namespace stable across
versions.

An adapter supplies:

1. `inspect`: bounded identity/compatibility validation of a candidate.
2. `preflight`: bounded checks against disposable state while the current worker
   remains serving. Never restore a backup over acknowledged live state.
3. `probe`: bounded full requests to the actual service and its state. Successful
   binds or TCP connects alone do not prove readiness.
4. `worker_command`: configure the application executable's private worker
   arguments/environment. The engine installs listener FDs starting at 3 and
   the lifecycle FD immediately after them; it owns `pre_exec` FD setup.
5. `runtime`: stable private IPC namespace, unique to that application.
6. `announce`: application-specific notification of automatic rollback.

The worker takes ownership of inherited descriptors with `worker::Inherited`
and calls `worker::run` with negotiated `worker::Options` and a callback receiving
a `worker::Generation` handle. The handle creates tracked listeners, owns
`accepting`/`accepted`/`ready` framing and negotiation, answers Resume internally,
and exposes Quiesce/Stop events through async or synchronous waits. EOF becomes
Stop. Both the hub and the TCP example use this same implementation.

The application callback owns its runtime, request tasks and exclusive state.
Before calling `Generation::quiesced(id)`, it must finish requests, stop every
accept loop, and destroy all state owners. The handle verifies the request ID
and gathers timestamps from stopped library listeners; applications cannot
construct quiescence metadata themselves. Only after the callback returns and
its runtime has dropped does the library send `Drained` and park the process.
On resume the callback starts a new generation in the same process. While
parked, the worker cannot accept or reopen application state; channel EOF or a
park timeout exits instead. The application must also observe channel EOF while
serving. The handle supports cancellation-safe command reads without consuming
a queued resume across a runtime teardown. `next_event_observing` lets an
application supply its latest service/address observations for an internal
Resume reply; the library still owns the reply and lifecycle metadata.

Use `worker::run_checked` in an application's conformance fixtures to supply a
PID-based check that no exclusive resource remains held after each generation
returns quiesced. A failed check prevents `Drained` from being sent. The hub's
process tests independently inspect SQLite handle ownership after drain.
The check runs inside the admission budget. Keep a production check cheap;
an expensive check can consume the 500 ms scheduling margin and defer upgrades.

Before activation, candidates may write only durable changes the previous
worker can still read. This obligation also applies after activation when
automatic recovery may restore the previous binary. Irreversible migrations
need a separate compatibility/rollout design; activation alone does not make
them safe. Applications should construct identity with `BuildInfo::new` so the
linked library supplies its own lifecycle capability.

The existing lifecycle wire format, phase budgets, five-second retirement grace
and ten-second exposure admission policy are retained. These limits are fixed
to the hub's validated policy and are not a configurable library API yet.
Cold supervisor
replacement and crashed/non-cooperating candidates remain exceptions to the
cooperative zero-failure guarantee. Timeouts cannot force arbitrary application
code to release state safely; an application that ignores the contract cannot
claim that guarantee. This is an internal workspace library for now, not a
published crate or a new release of the hub.

## A second service

`examples/echo.rs` is a plain TCP service using this boundary. It has no hub or
SQLite dependency. Build once, copy it into its own stable installation, then
run from that path:

```sh
cargo build --release -p process-supervisor --example echo
mkdir -p /tmp/supervisor-echo-demo
cp target/release/examples/echo /tmp/supervisor-echo-demo/echo
/tmp/supervisor-echo-demo/echo serve /tmp/supervisor-echo-demo/state 127.0.0.1:17878
```

From another terminal:

```sh
nc 127.0.0.1 17878
# hello echo-v1
target/release/examples/echo upgrade /tmp/supervisor-echo-demo/state target/release/examples/echo
target/release/examples/echo stop /tmp/supervisor-echo-demo/state
```

The example demonstrates wiring and handoff, not a remote firewall acceptance
claim. An application adopting this library needs its own state-ownership,
compatibility, rollback and deployment acceptance tests. The hub retains its
real-process tests in `agent-hub/tests`; protocol, timing, filesystem
safety and late-drain tests live with this library.
