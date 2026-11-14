# Managed agents and MCP messaging

Status: accepted direction; implementation is incremental.

## Goal

Run a managed Codex session and a managed Claude Code session on one machine.
Either agent can send a message through MCP; an idle recipient starts a new
turn automatically, in its existing conversation. Extend the same workflow to
workers on other machines over a Tailscale private network.

The user chose hub-managed sessions rather than injection into independently
running terminal or desktop sessions. Preserve the existing CLI, dashboard,
channels, holds, history, retirement, and cooperative hub upgrades.

## Decisions

- One central hub owns messages, delivery records, and agent identity.
- One worker per machine owns local runtime processes and session files.
- MCP and CLI are interfaces to the same messaging service and database.
- Use the official Rust MCP SDK, `rmcp`, for the MCP protocol.
- Use supported runtime controls to start turns. MCP notifications alone do not
  guarantee that an arbitrary host starts a model turn.
- Start with local stdio MCP bridges. A bridge calls the existing hub HTTP API;
  changing its hub address is enough to reach a hub over Tailscale.
- Keep turn scheduling independent of the sender's interface.
- Keep message read receipts separate from execution completion.

## Architecture

```text
                         central hub
                   +---------------------+
                   | Messaging service   |
                   | SQLite + deliveries |
                   | Dashboard           |
                   +----------+----------+
                              |
                       typed HTTP + events
                              |
                   +----------+----------+
                   | Machine worker      |
                   | Session ownership   |
                   | Turn scheduling     |
                   +-----+----------+----+
                         |          |
                  Codex adapter  Claude adapter
                         |          |
                  Codex session  Claude Code session
                         |          |
                    MCP tools   MCP tools
                         |          |
                   stdio bridge stdio bridge
                         |          |
                         +-----+----+
                               |
                         hub HTTP API
```

Only the hub opens its database. Runtime credentials, checkout paths, and
conversation files stay on the worker's machine. A hub upgrade does not restart
agent runtimes. The existing supervisor's hidden `worker` command is the hub
HTTP worker, not the new agent-runtime worker; name the latter `agent-worker`
to keep the two roles distinct.

## Current implementation to retain

`protocol` owns typed endpoints, models, feature requirements, and retry policy.
`agent-hub/src/client.rs` implements the native HTTP client. The hub commits
messages and retry receipts together before broadcasting `Event::Sent`.
Deferred inbox reads and explicit ID acknowledgments prevent a failed delivery
from consuming mail. Recipient streams, reconnect handling, lifecycle isolation,
channels, and holds already exist.

The current `watch` command prints notices. It neither owns a runtime session nor
starts turns. Its 25-second quiet period and 60-second cap are human-facing
presentation policy, not a prerequisite for automatic message delivery.

## MCP interface

Proposed command: `agent-hub mcp --as NAME --hub URL`.

The bridge binds one agent identity at startup. Tool arguments cannot choose a
sender or impersonate the person. Its stdout is exclusively MCP frames;
diagnostics go to stderr. Blocking hub HTTP calls run off the async executor.

Initial tools:

| Tool | Operation |
| --- | --- |
| `list_agents` | List agents, including their availability |
| `send_message` | Send to an agent or `#channel`; return stored message IDs |
| `read_inbox` | Fetch unread messages without acknowledging them |
| `read_message` | Read one received message without acknowledging it |
| `ack_messages` | Acknowledge only the supplied received IDs |

Use standard MCP tool discovery and calls. These tool names and their payloads
are application contracts, not additions to MCP. An application failure returns
a tool error; invalid protocol arguments use the SDK's protocol error handling.

Read tools must require the hub's deferred-read feature. Never silently fall back
to a destructive read on an older hub. The CLI keeps its existing display and
acknowledgment behavior. Registration reconnects must respect retirement; closing
a bridge must not retire an agent, because tool processes may restart.

Resource reads and resource update subscriptions are a later optional interface
for generic clients. They are not required for the managed worker to wake an
agent. Do not use experimental `events/*`, agent-discovery methods, MCP Tasks,
sampling, or Claude Channels for the initial workflow.

The first bridge uses the existing private-network trust model. A bound name is
not authentication. Scoped credentials and authorization are required before
managed runtime control is reachable across machines.

## Runtime adapters

The worker owns one conversation per managed agent generation. It records the
provider's explicit session ID; never resume the most recent conversation by
working directory.

Codex uses a local `codex app-server` process: initialize, start/resume a thread,
start a turn, consume output and completion events, and answer approval requests.

Claude uses the official Claude Agent SDK's streaming input through a small
TypeScript helper run by Bun. The helper translates runtime input/output only;
it does not own hub storage or scheduling. Keep provider-specific process details
behind the adapter rather than implementing an undocumented Claude protocol.

The internal adapter contract covers start/resume session, submit turn,
interrupt, approval response, and an event stream. Events include session ID,
turn ID where available, output, approval/input requests, completion, and failure.
Runtime permissions remain in force; an incoming peer message does not grant
authority beyond the user's configured task and policy.

## Delivery lifecycle

```text
Sender       MCP bridge       Hub          Worker          Receiver runtime
  |               |            |              |                   |
  |-- send ------>|-- send --->|              |                   |
  |               |            | commit message + delivery         |
  |<-------- stored ID --------|              |                   |
  |               |            |-- wake ----->|                   |
  |               |            |<-- claim ----|                   |
  |               |            |              |-- submit turn --->|
  |               |            |              |<-- completion ----|
  |               |            |<-- complete -|                   |
```

Receiving a stored ID means persisted, not processed. Message reads and execution
completion are separate facts. A CLI reader must not cancel a scheduled turn.

For managed recipients, insert message and delivery in the same SQLite
transaction, including idempotent channel fanout. Enabling management establishes
an explicit initial backlog boundary; do not unexpectedly execute old history.

Delivery records reference existing messages rather than copying their bodies:

```text
delivery: id, message_id, recipient_generation, state,
          owner_epoch, runtime_turn_id, error

state: queued -> running -> completed
                    |----> failed
                    +----> uncertain
```

Persist a dispatch attempt before calling a runtime. Preserve stable operation
IDs across retries; the current client-generated nonce protects retries within a
single call, not arbitrary new calls after restart. Add caller-supplied operation
IDs before automatically replaying sends across process recovery.

One worker owns an agent generation at a time, with renewable ownership and
fencing against stale updates. One turn runs per session. Ordinary arrivals queue
in order while busy; no implicit interruption. Retiring a generation stops its
work from reaching another session that later reuses its name.

The worker subscribes before reconciling pending work. Reconcile on startup,
reconnect, turn completion, and periodically. Notifications are hints; durable
delivery state is authoritative. This also recovers a commit followed by a crash
before event publication. Process all pending batches, not just the first page.

Losing ownership is not permission to launch a second runtime while the first
may still execute. Stop dispatch, reconcile the existing runtime, and fence stale
updates. If submission succeeded but its acknowledgment was lost, use available
runtime history to determine the outcome. Otherwise mark uncertain and surface
it to the user. Do not promise exactly-once arbitrary filesystem or tool effects.

Approvals pause the session and appear in the dashboard. Route decisions to the
owning worker and runtime request; do not change global approval policy. Preserve
the distinction between peer messages and user-authored messages in injected
context. Replies are explicit sends; a normal final answer is not automatically
broadcast. Avoid acknowledgment-only reply loops and bound automatic conversation
work by configured turn/cost limits.

## Cross-machine deployment

```text
Machine A                              Machine B
+--------------------------+           +--------------------------+
| Hub + SQLite             |<Tailscale>| Agent worker             |
| Agent worker + Codex     |           | Claude Code + MCP bridge |
| MCP bridge               |           +--------------------------+
+--------------------------+
```

Workers and bridges connect outbound to the hub. Runtime-control transports stay
local; do not expose provider app servers or process stdin over the network.
Tailscale supplies private connectivity. Per-worker and per-agent credentials
authorize machine ownership, sending identity, inbox access, and control actions.
The dashboard's existing person key remains a separate identity.

Session migration and filesystem synchronization are outside this milestone.
Agents can exchange references, but a local path is not automatically readable
on another machine. Keep one central hub; no federation or replicated SQLite.

## Code boundaries

Extract boundaries as implementation requires them, without moving unrelated
upgrade code or rewriting all HTTP endpoints first:

- `protocol`: shared wire models and endpoint contracts.
- `hub-core`: application operations, persistence, lifecycle, deliveries; no MCP
  or Axum dependency. Extract incrementally from the existing hub handlers.
- Shared native client: typed operations and reconnect/event handling, used by
  CLI, MCP bridge, and agent worker.
- `agent-hub`: composition, CLI, HTTP/MCP adapters, agent worker, Codex adapter,
  and existing supervisor integration. Keep modules until separate crates help.
- `runtime-claude`: the small Bun helper around the official SDK.

The same operation must retain its validation, retry behavior, and storage
semantics regardless of which interface invokes it. Do not duplicate message
storage or parse human CLI output in the MCP adapter.

## Implementation milestones

1. Add the `rmcp` stdio bridge and five tools through the existing typed client.
   Verify real MCP calls against the same HTTP hub used by CLI clients. This is
   a useful, reviewable first slice, independent of runtime management.
2. Extract application operations and event reconnection into shared modules as
   needed. Add generation-aware managed sessions, delivery records, ownership,
   and caller-stable operation IDs. Preserve legacy CLI semantics.
3. Add the local agent worker, Codex App Server adapter, Claude SDK helper, and
   dashboard session/output/approval views. Configure MCP for both runtimes.
4. Prove the bidirectional local workflow, restart recovery, and busy-session
   queuing. Add scoped credentials and run the same workflow across Tailscale.

## Acceptance

- Codex sends through MCP; idle Claude starts a turn in its managed session.
- Claude replies through MCP; idle Codex starts a turn in its managed thread.
- CLI and MCP see the same messages and acknowledgments.
- A failed MCP response does not acknowledge unread messages.
- Identity is fixed by the bridge; peer content is never labeled as human input
  without preserving its peer provenance and applicable authority boundary.
- Reconnects and duplicate wakeups do not create concurrent turns.
- Busy sessions queue; runtime errors and unresolved approvals remain visible.
- Crash windows around commit, submit, completion, and ownership loss are tested.
- Retirement cannot deliver old work to a new generation with the same name.
- Hub upgrades preserve pending work without restarting healthy runtimes.
- Repeat the successful local exchange across two Tailscale machines before
  declaring cross-machine runtime management complete.

## References

- MCP core patterns: https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns
- Official Rust SDK: https://github.com/modelcontextprotocol/rust-sdk
- Codex App Server: https://developers.openai.com/codex/app-server
- Claude streaming input: https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode
- Claude sessions: https://code.claude.com/docs/en/agent-sdk/sessions
- Claude Channels (research preview, not a dependency): https://code.claude.com/docs/en/channels
