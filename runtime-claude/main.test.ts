import { describe, expect, test } from "bun:test";
import { query, type Options, type SDKMessage, type SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { InputQueue, run } from "./main";

type QueryArgs = Parameters<typeof query>[0];

function factory(
  receive: (input: SDKUserMessage, options: Options) => Promise<void> = async () => {},
  fail = false,
) {
  const calls: QueryArgs[] = [];
  const create = ((args: QueryArgs) => {
    calls.push(args);
    const messages = (async function* () {
      yield { type: "system", subtype: "init", session_id: "conversation-1" } as SDKMessage;
      for await (const message of args.prompt as AsyncIterable<SDKUserMessage>) {
        await receive(message,args.options!);
        yield { type: "assistant", message: { content: [{ type: "text", text: "Working" }] } } as SDKMessage;
        yield { type: "result", subtype: "success", is_error: fail, result: fail ? "API error" : "Done" } as SDKMessage;
      }
    })();
    return Object.assign(messages,{ close() {}, async interrupt() {} });
  }) as typeof query;
  return { calls, create };
}

const init = {
  type: "init", cwd: "/tmp", sessionId: "conversation-1", instructions: "Assigned project task",
  bridge: {command:"agent-hub",args:["mcp","--as","claude"]},
};

describe("Claude runtime adapter", () => {
  test("keeps one query alive across turns and resumes the explicit session", async () => {
    const commands = new InputQueue<string>();
    const sdk = factory();
    const events: any[] = [];
    commands.push(JSON.stringify(init));
    commands.push(JSON.stringify({type:"turn",messageId:1,prompt:"first"}));
    await run(commands,(event: any) => {
      events.push(event);
      if (event.type === "finished") {
        if (event.messageId === 1) commands.push(JSON.stringify({type:"turn",messageId:2,prompt:"second"}));
        else commands.close();
      }
    },sdk.create);
    expect(sdk.calls).toHaveLength(1);
    expect(sdk.calls[0]!.options!.resume).toBe("conversation-1");
    expect(sdk.calls[0]!.options!.permissionMode).toBe("default");
    expect(sdk.calls[0]!.options!.allowedTools).toEqual(["mcp__agent_hub__*"]);
    expect(events.filter(event => event.type === "finished").map(event => event.messageId)).toEqual([1,2]);
    expect(events.some(event => event.type === "fatal")).toBe(false);
  });

  test("permission requests wait for an explicit host decision", async () => {
    const commands = new InputQueue<string>();
    let decision: unknown;
    const sdk = factory(async (_, options) => {
      decision = await options.canUseTool!("Bash",{command:"a command"},{signal:new AbortController().signal,toolUseID:"tool-1",requestId:"request-1"});
    });
    commands.push(JSON.stringify(init));
    commands.push(JSON.stringify({type:"turn",messageId:7,prompt:"work"}));
    const events: any[] = [];
    await run(commands,(event: any) => {
      events.push(event);
      if (event.type === "approval") commands.push(JSON.stringify({type:"response",id:event.id,response:{behavior:"deny",message:"User declined"}}));
      if (event.type === "finished") commands.close();
    },sdk.create);
    expect(decision).toEqual({behavior:"deny",message:"User declined"});
    expect(events.filter(event => event.type === "approval")).toHaveLength(1);
    expect(events.filter(event => event.type === "approvalResolved")).toHaveLength(1);
  });

  test("a success subtype carrying an API error is a failed turn", async () => {
    const commands = new InputQueue<string>();
    commands.push(JSON.stringify(init));
    commands.push(JSON.stringify({type:"turn",messageId:1,prompt:"work"}));
    let result: any;
    await run(commands,(event: any) => {
      if (event.type === "finished") { result=event; commands.close(); }
    },factory(undefined,true).create);
    expect(result.state).toBe("failed");
    expect(result.error).toBe("API error");
  });
});
