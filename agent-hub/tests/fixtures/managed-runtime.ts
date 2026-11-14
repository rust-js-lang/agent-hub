// Deterministic provider fixture. It uses the real MCP bridge for peer messages.
import { createInterface } from "node:readline";
import { appendFileSync } from "node:fs";
import { Readable } from "node:stream";

const [role, trace] = process.argv.slice(2);
const emit = (value: object) => console.log(JSON.stringify(value));
const record = (value: object) => appendFileSync(trace!, JSON.stringify({role,...value}) + "\n");
let bridge: {command: string; args: string[]} | undefined;
let session = "";
let busy = false;
let sequence = 0;
const decisions = new Map<string, () => void>();

class Mcp {
  private child: ReturnType<typeof Bun.spawn>;
  private pending = new Map<number, {resolve(value: any): void; reject(reason: unknown): void}>();
  private id = 0;

  constructor(config: {command: string; args: string[]}) {
    this.child = Bun.spawn([config.command,...config.args], {stdin:"pipe",stdout:"pipe",stderr:"inherit"});
    void (async () => {
      for await (const line of createInterface({input:Readable.fromWeb(this.child.stdout as any)})) {
        const message = JSON.parse(line);
        if (message.id !== undefined) {
          const pending = this.pending.get(message.id);
          this.pending.delete(message.id);
          if (message.error) pending?.reject(message.error);
          else pending?.resolve(message.result);
        }
      }
      for (const pending of this.pending.values()) pending.reject(Error("MCP bridge exited"));
    })();
  }

  async request(method: string, params: object) {
    const id = ++this.id;
    const result = new Promise<any>((resolve,reject) => this.pending.set(id,{resolve,reject}));
    (this.child.stdin as any).write(JSON.stringify({jsonrpc:"2.0",id,method,params}) + "\n");
    return result;
  }

  async send(to: string, body: string) {
    await this.request("initialize", {protocolVersion:"2025-11-25",capabilities:{},clientInfo:{name:"fixture",version:"1"}});
    (this.child.stdin as any).write(JSON.stringify({jsonrpc:"2.0",method:"notifications/initialized"}) + "\n");
    const result = await this.request("tools/call", {name:"send_message",arguments:{to,body}});
    if (result.isError) throw Error(JSON.stringify(result));
    this.child.kill();
  }
}

async function turn(id: number, prompt: string) {
  if (busy) throw Error("Concurrent turns");
  busy = true;
  const body = JSON.parse(prompt.slice(prompt.indexOf("\n")+1)).body as string;
  const turnId = `turn-${++sequence}`;
  record({event:"start",id,session,body});
  if (role === "codex") {
    emit({id, result:{turn:{id:turnId,status:"inProgress"}}});
  } else {
    emit({type:"started",messageId:id,turnId});
  }
  if (body === "crash") process.exit(17);
  if (body.startsWith("slow")) await Bun.sleep(1500);
  if (body === "approval") {
    const key = role === "codex" ? "900" : `approval-${id}`;
    const wait = new Promise<void>((resolve) => decisions.set(key,resolve));
    if (role === "codex") emit({id:900,method:"item/commandExecution/requestApproval",params:{command:"fixture command"}});
    else emit({type:"approval",messageId:id,id:key,request:{kind:"claude-tool",toolName:"Bash",input:{command:"fixture command"}}});
    await wait;
  }
  if (body.startsWith("send:")) {
    const [,to,...words] = body.split(":");
    await new Mcp(bridge!).send(to!,words.join(":"));
  }
  record({event:"finish",id,session,body});
  busy = false;
  if (role === "codex") {
    emit({method:"item/agentMessage/delta",params:{threadId:session,delta:`Processed ${body}`}});
    emit({method:"item/completed",params:{threadId:session,turnId,item:{type:"agentMessage",id:`item-${turnId}`,text:`Processed ${body}`}}});
    emit({method:"turn/completed",params:{threadId:session,turn:{id:turnId,status:"completed"}}});
  } else {
    emit({type:"output",messageId:id,text:`Processed ${body}`});
    emit({type:"finished",messageId:id,state:"completed",error:null});
  }
}

for await (const line of createInterface({input:process.stdin})) {
  const message = JSON.parse(line);
  if (role === "codex") {
    if (message.method === "initialize") emit({id:message.id,result:{}});
    else if (message.method === "thread/start" || message.method === "thread/resume") {
      session = message.params.threadId ?? crypto.randomUUID();
      bridge = message.params.config["mcp_servers.agent_hub"];
      record({event:"session",method:message.method,session});
      emit({id:message.id,result:{thread:{id:session}}});
    } else if (message.method === "turn/start") {
      const prompt = message.params.input[0].text;
      // The provider RPC ID and hub message ID are different. Keep the RPC reply
      // correlated, while the worker associates events with its active delivery.
      void turn(message.id,prompt).catch((error) => {console.error(error);process.exit(1);});
    } else if (message.id === 900 && message.result) {
      record({event:"decision",response:message.result});
      decisions.get("900")?.();
      decisions.delete("900");
    }
  } else {
    if (message.type === "init") {
      session = message.sessionId ?? crypto.randomUUID();
      bridge = message.bridge;
      record({event:"session",method:message.sessionId ? "resume" : "start",session});
      emit({type:"sessionReady",sessionId:session});
    } else if (message.type === "turn") {
      void turn(message.messageId,message.prompt).catch((error) => {console.error(error);process.exit(1);});
    } else if (message.type === "response") {
      record({event:"decision",response:message.response});
      decisions.get(message.id)?.();
      decisions.delete(message.id);
      emit({type:"approvalResolved",id:message.id});
    }
  }
}
