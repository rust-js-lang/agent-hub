# Claude runtime helper

This private Bun package adapts the official Claude Agent SDK to the native
agent worker. It owns one persistent streaming-input query. Scheduling, messages,
leases, and recovery remain in the Rust hub and worker.

```sh
bun install --frozen-lockfile
bun run check
bun test
```

The worker starts `bun main.ts` with piped stdin/stdout. Each line is JSON:

- Input: `init`, then serial `turn` commands, `response`, or `interrupt`.
- Output: `sessionReady`, `started`, `output`, `approval`, `approvalResolved`,
  `finished`, or `fatal`.

`init` carries the working directory, optional explicit session ID and model,
instructions, and the worker-configured MCP bridge. `turn` carries the stored
message ID and its sender-classified prompt. A result ends one turn, while the
input stream remains open for the next message. Restart uses `resume` with the
saved ID, never a search for the latest conversation.

Stdout is reserved for this protocol; SDK diagnostics go to stderr. Normal user,
project, and local settings apply. Messaging MCP tools are allowed for the
requested collaboration; other permission decisions use the SDK callback and
wait for the person's response through the hub. Account limits remain normal
runtime failures. See the root README for configuration and recovery commands.
