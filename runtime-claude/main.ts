import {
  query,
  type Options,
  type PermissionResult,
  type SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
import { createInterface } from "node:readline";

type Init = {
  type: "init";
  cwd: string;
  sessionId?: string;
  model?: string;
  instructions: string;
  bridge: { command: string; args: string[] };
};

type Command = Init
  | { type: "turn"; messageId: number; prompt: string }
  | { type: "response"; id: string; response: PermissionResult }
  | { type: "interrupt" };

// The SDK consumes this iterator throughout one persistent conversation. Ending
// stdin closes it; a result message ends a turn, not this input iterator.
export class InputQueue<T> implements AsyncIterable<T> {
  private values: T[] = [];
  private wake: (() => void) | undefined;
  private closed = false;

  push(value: T) {
    if (this.closed) throw new Error("Input is closed");
    this.values.push(value);
    this.wake?.();
  }

  close() {
    this.closed = true;
    this.wake?.();
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      if (this.values.length) yield this.values.shift()!;
      else if (this.closed) break;
      else await new Promise<void>((resolve) => { this.wake = resolve; });
    }
  }
}

export async function run(
  lines: AsyncIterable<string>,
  emit: (value: object) => void,
  createQuery: typeof query = query,
) {
  const input = new InputQueue<SDKUserMessage>();
  const decisions = new Map<string, (decision: PermissionResult) => void>();
  let init: Init | undefined;
  let stream: ReturnType<typeof query> | undefined;
  let outputTask: Promise<void> | undefined;
  let active: { id: number; started: boolean } | undefined;

  const started = () => {
    if (active && !active.started) {
      active.started = true;
      emit({ type: "started", messageId: active.id, turnId: `message-${active.id}` });
    }
  };

  const start = () => {
    if (!init) throw new Error("Initialize before submitting input");
    const options: Options = {
      cwd: init.cwd,
      resume: init.sessionId,
      model: init.model,
      executable: "bun",
      settingSources: ["user", "project", "local"],
      permissionMode: "default",
      allowedTools: ["mcp__agent_hub__*"],
      mcpServers: { agent_hub: init.bridge },
      systemPrompt: { type: "preset", preset: "claude_code", append: init.instructions },
      stderr: (text) => process.stderr.write(text),
      canUseTool: async (toolName, toolInput, context) => {
        if (!active) return { behavior: "deny", message: "No active hub delivery" };
        const id = `${active.id}:${crypto.randomUUID()}`;
        const result = new Promise<PermissionResult>((resolve) => {
          const finish = (decision: PermissionResult) => {
            context.signal.removeEventListener("abort", abort);
            decisions.delete(id);
            emit({ type: "approvalResolved", id });
            resolve(decision);
          };
          const abort = () => finish({ behavior: "deny", message: "Request cancelled" });
          decisions.set(id, finish);
          context.signal.addEventListener("abort", abort, { once: true });
          if (context.signal.aborted) abort();
        });
        if (decisions.has(id)) emit({
          type: "approval", messageId: active.id, id,
          request: { kind: "claude-tool", toolName, input: toolInput },
        });
        return result;
      },
    };
    stream = createQuery({ prompt: input, options });
    outputTask = (async () => {
      try {
        for await (const message of stream!) {
          if (message.type === "system" && message.subtype === "init") {
            emit({ type: "sessionReady", sessionId: message.session_id });
          } else if (message.type === "assistant" && active) {
            started();
            for (const block of message.message.content) {
              if (block.type === "text") emit({ type: "output", messageId: active.id, text: block.text });
            }
          } else if (message.type === "result" && active) {
            started();
            const success = message.subtype === "success" && !message.is_error;
            emit({
              type: "finished", messageId: active.id, state: success ? "completed" : "failed",
              error: success ? null : message.subtype === "success" ? message.result : message.errors.join("\n"),
            });
            active = undefined;
          }
        }
        if (active) throw new Error("Claude ended without a result for its active turn");
      } catch (error) {
        emit({ type: "fatal", error: String(error) });
      }
    })();
  };

  try {
    for await (const line of lines) {
      if (!line.trim()) continue;
      const command = JSON.parse(line) as Command;
      switch (command.type) {
        case "init":
          if (init) throw new Error("Already initialized");
          init = command;
          break;
        case "turn":
          if (active) throw new Error("Only one turn may run at a time");
          if (!stream) start();
          active = { id: command.messageId, started: false };
          input.push({ type: "user", message: { role: "user", content: command.prompt }, parent_tool_use_id: null });
          break;
        case "response": {
          const decision = command.response;
          if (!decision || !["allow", "deny"].includes(decision.behavior)) throw new Error("Expected an allow/deny permission response");
          if (decision.behavior === "deny" && typeof decision.message !== "string") throw new Error("A denial needs a message");
          decisions.get(command.id)?.(decision);
          break;
        }
        case "interrupt":
          await stream?.interrupt();
          break;
        default:
          throw new Error("Unknown runtime command");
      }
    }
  } finally {
    for (const resolve of [...decisions.values()]) resolve({ behavior: "deny", message: "Worker disconnected" });
    input.close();
    stream?.close();
    await outputTask;
  }
}

if (import.meta.main) {
  try {
    await run(createInterface({ input: process.stdin }), (value) => console.log(JSON.stringify(value)));
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
