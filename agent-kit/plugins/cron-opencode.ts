// 会话内定时任务插件：通过 cron_add / cron_list / cron_remove 工具
// 寤虹珛瀹氭椂浠诲姟锛涘埌鐐瑰悗浠诲姟浠ョ敤鎴锋秷鎭敞鍏ヤ細璇?鈥斺€?绌洪棽鏃剁珛鍗冲紑鏂拌疆娆℃墽琛岋紝
// 蹇欑鏃朵互 followUp 鎺掗槦锛屼笉鎵撴柇褰撳墠宸ヤ綔銆?// 鎸佷箙鍖栵細浠诲姟闅?appendEntry 鍐欒繘浼氳瘽鏂囦欢锛岄噸鍚?/ 鍒囧垎鏀悗鑷姩鎭㈠锛?//         鎭㈠鏃跺凡杩囨湡鐨?nextAt 浼氬湪棣栦釜 tick 琛ヨ窇涓€娆★紙閿欒繃鐨勪换鍔¤窇涓€娆★紝涓嶅爢绉級銆?// 瀹氭椂鍣細蹇呴』鐢?ctx.setInterval锛堟墭绠★級鈥斺€?鍥炶皟鎶涢敊鍙鏃ュ織锛屼笉浼氭嫋鍨暣涓細璇濓紝
//         且会话关闭时自动清理。raw setInterval 的未捕获异常会以 uncaughtException
//         缁堢粨鏁翠釜杩涚▼锛岀姝娇鐢ㄣ€?// 鎵嬪姩鏌ョ湅锛?cron锛涘垹闄ゆ湰鏂囦欢鍗冲嵏杞斤紙闇€閲嶅惎浼氳瘽鐢熸晥锛夈€?
// ---- 涓?opencode 杩愯鏃剁殑缁撴瀯鍖栬竟鐣岋細鍙０鏄庢湰鎵╁睍瀹為檯鐢ㄥ埌鐨勬柟娉?----
interface CronUi {
  notify?: (text: string, level?: string) => void;
}

interface CronCtx {
  ui?: CronUi;
  isIdle?: () => boolean;
  setInterval?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  sessionManager?: { getBranch?: () => unknown[] };
}

type ZodNode = { optional: () => ZodNode };
interface CronPi {
  zod: {
    string: () => ZodNode;
    number: () => ZodNode;
    object: (shape: Record<string, ZodNode>) => unknown;
  };
  exec?: (command: string, args: string[], opts?: { cwd?: string }) => Promise<{ stdout: string; stderr: string; code: number }>;
  sendUserMessage: (
    content: string,
    options?: { deliverAs?: "followUp" | "steer" | "nextTurn" | "aside" },
  ) => Promise<unknown> | unknown;
  appendEntry: (customType: string, data: unknown) => unknown;
  registerTool: (tool: Record<string, unknown>) => unknown;
  registerCommand?: (
    name: string,
    command: { description: string; handler: (args: unknown, ctx: CronCtx) => Promise<void> },
  ) => unknown;
  on: (event: string, handler: (event: unknown, ctx: CronCtx) => Promise<void>) => unknown;
  logger?: { warn?: (msg: string) => void; error?: (msg: string) => void; info?: (msg: string) => void };
}

// ---- 数据模型 ----
type JobKind = "every" | "daily" | "once";

interface CronJob {
  id: string;
  name: string;
  prompt: string;
  kind: JobKind;
  everySeconds?: number; // kind=every
  at?: string; // kind=daily锛?HH:MM" 鏈満鏃跺尯
  nextAt: number; // epoch ms
  createdAt: number;
  onBusy?: "queue" | "cancel"; // 会话忙碌时：queue 排队（默认）/ cancel 取消本次触发
  condition?: string; // "__TOKEN__ # <agent_id> # <endpoint>"锛氬埌鐐瑰厛 get_messages 鍒?count>0锛岀┖鍒欒烦杩囨湰娆′笉杩?LLM
  tokenFile?: string; // 明文 token 文件路径，读首行
}

const ENTRY_TYPE = "local.cron.jobs.v1";
const TICK_MS = 5_000; // 鍒扮偣妫€鏌ョ矑搴︼紱瀹為檯瑙﹀彂鏈€澶氭櫄涓€涓?tick
const MIN_SECONDS = 5; // 鍏佽鐨勬渶灏忛棿闅旓紱涓?tick 绮掑害涓€鑷达紝鍐嶅皬浼氳 5s tick 鍚炴帀涓斿彧鐑?token
const MAX_JOBS = 32;
const MAX_PROMPT = 4_000;
const MAX_NAME = 80;

// "HH:MM" -> 涓嬩竴娆¤Е杈炬椂鍒伙紙鏈満鏃跺尯锛夛紱宸茶繃浠婃棩璇ョ偣鍒欓『寤朵竴澶┿€?export function nextDailyAt(at: string, from: number = Date.now()): number {
  const m = /^(\d{1,2}):(\d{2})$/.exec(at.trim());
  const h = m ? Number(m[1]) : NaN;
  const min = m ? Number(m[2]) : NaN;
  if (!m || !(h >= 0 && h < 24) || !(min >= 0 && min < 60)) {
    throw new Error(`daily_at 闇€涓?"HH:MM"锛?4 灏忔椂鍒讹級锛屾敹鍒帮細${at}`);
  }
  const d = new Date(from);
  d.setHours(h, min, 0, 0);
  if (d.getTime() <= from) d.setDate(d.getDate() + 1);
  return d.getTime();
}

// 鍔ㄦ€侀敭璇诲彇锛歚in` 鏀剁獎瀵归潪甯搁噺閿笉鐢熸晥锛屾澶勬敹鍙ｄ负鍞竴鍑哄彛锛堝瓧娈甸€愪竴缁?asStr/asNum 鏍￠獙锛夈€?function field(raw: unknown, key: string): unknown {
  return raw && typeof raw === "object" ? (raw as Record<string, unknown>)[key] : undefined;
}

function asStr(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function asNum(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function validateSchedule(raw: unknown): { kind: JobKind; everySeconds?: number; at?: string; nextAt: number } {
  const every = asNum(field(raw, "every_seconds"));
  const once = asNum(field(raw, "once_in_seconds"));
  const daily = asStr(field(raw, "daily_at"));
  const given = [every, daily, once].filter((v) => v !== undefined && v !== "");
  if (given.length !== 1) {
    throw new Error("every_seconds / daily_at / once_in_seconds 三选一，必填其一");
  }
  if (every !== undefined) {
    if (every < MIN_SECONDS) throw new Error(`every_seconds 鏈€灏?${MIN_SECONDS} 绉抈);
    return { kind: "every", everySeconds: every, nextAt: Date.now() + every * 1000 };
  }
  if (once !== undefined) {
    if (once < MIN_SECONDS) throw new Error(`once_in_seconds 鏈€灏?${MIN_SECONDS} 绉抈);
    return { kind: "once", nextAt: Date.now() + once * 1000 };
  }
  // 鍓嶄袱涓垎鏀湭杩斿洖 鈬?鍞竴鎻愪緵鐨勮皟搴﹀瓧娈垫槸 daily_at
  return { kind: "daily", at: daily, nextAt: nextDailyAt(daily) };
}

function formatJob(j: CronJob): string {
  const schedule =
    j.kind === "every"
      ? `姣?${j.everySeconds}s`
      : j.kind === "daily"
        ? `每天 ${j.at}`
        : "涓€娆℃€?;
  const at = new Date(j.nextAt).toLocaleString();
  return `- [${j.id}] ${j.name}锛?{schedule}锛?{j.onBusy === "cancel" ? "蹇欐椂鍙栨秷" : "蹇欐椂鎺掗槦"}锛屼笅娆?${at}锛夛細${j.prompt}`;
}

export default function cron(pi: CronPi) {
  // 鐘舵€佸叏閮ㄦ斁鍦ㄥ伐鍘傞棴鍖呭唴锛氭瘡涓細璇濆悇鑷姞杞芥墿灞曞疄渚嬶紝浜掍笉涓叉壈銆?  let jobs = new Map<string, CronJob>();
  let ctxRef: CronCtx | null = null;
  let timer: unknown = null;
  let idSeq = 0;

  function persist(): void {
    pi.appendEntry(ENTRY_TYPE, { version: 1, jobs: [...jobs.values()] });
  }

  function restore(ctx: CronCtx): void {
    const branch = ctx.sessionManager?.getBranch?.() ?? [];
    let latest: { version: number; jobs: CronJob[] } | undefined;
    for (const entry of branch) {
      const e = entry as { type?: string; customType?: string; data?: unknown };
      if (e.type === "custom" && e.customType === ENTRY_TYPE) {
        latest = e.data as { version: number; jobs: CronJob[] };
      }
    }
    jobs = new Map((latest?.jobs ?? []).filter((j) => j && j.id && typeof j.prompt === "string").map((j) => [j.id, j]));
  }

  // MCP 鏉′欢闂細condition 褰㈠ "__TOKEN__ # <agent_id> # <endpoint>"銆?  // 鍒扮偣鍏堢敤 exec 璧?curl 璋?harbor get_messages锛屽垽 count>0 鎵嶆斁琛屾敞鍏ワ紱
  // count=0 / 瑙ｆ瀽澶辫触 / 缂轰緷璧?鈫?fail-closed 璺宠繃鏈锛堜笉杩?LLM锛夛紝鍙『寤?nextAt銆?  async function conditionGate(job: CronJob): Promise<{ go: boolean; detail?: string }> {
    const spec = (job.condition ?? "").trim();
    if (!spec) return { go: true }; // 鏃?condition锛氳€佽涓猴紝鏃犳潯浠舵敞鍏?    if (!pi.exec) return { go: false, detail: "pi.exec 涓嶅彲鐢紝璺宠繃鏈" };
    // condition 妯℃澘褰㈠锛?    //   __TOKEN__ placeholder + " # " + agent_id + " # " + endpoint
    // 鎸?" # " 涓夋鍒囧垎锛屾瀬绠€鑰屾槑纭?    const parts = spec.split(" # ");
    if (parts.length !== 3) {
      return { go: false, detail: `condition 闇€涓?"__TOKEN__ # <agent_id> # <endpoint>"锛屾敹鍒?${spec}` };
    }
    const [_tokenPh, agentId, endpoint] = parts.map((p) => p.trim());
    let token = "";
    try {
      const tok = await pi.exec("head", ["-n1", job.tokenFile ?? ""]);
      if (tok.code !== 0) return { go: false, detail: `璇?token 澶辫触: ${tok.stderr.trim()}` };
      token = tok.stdout.trim();
    } catch (e) {
      return { go: false, detail: `璇?token 鏂囦欢澶辫触: ${String(e)}` };
    }
    if (!token) return { go: false, detail: "token 为空" };

    // 1) 建立 MCP streamable-http 会话拿到 mcp-session-id
    const initRes = await pi.exec("curl", [
      "-s", "-D", "-", "-o", "/dev/null", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-d", JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "initialize",
        params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "omp-cron", version: "1" } },
      }),
    ]);
    const sidMatch = /mcp-session-id:\s*(\S+)/i.exec(initRes.stdout);
    if (!sidMatch) return { go: false, detail: "initialize 鏈繑鍥?mcp-session-id" };
    const mcpSessionId = sidMatch[1];

    // 2) notifications/initialized（可选，多数 server 不强求）
    await pi.exec("curl", ["-s", "-o", "/dev/null", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-H", `mcp-session-id: ${mcpSessionId}`,
      "-d", JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
    ]);

    // 3) tools/call get_messages：判 count>0 决定是否唤醒
    const call = await pi.exec("curl", ["-s", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-H", `mcp-session-id: ${mcpSessionId}`,
      "-d", JSON.stringify({
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "get_messages", arguments: { agent_id: agentId, token, unread_only: true, limit: 20 } },
      }),
    ]);
    // 浠?SSE data: 琛屽彇鏈€鍚庝竴涓?data: 鐨?JSON
    const dataLines = call.stdout.split("\n").filter((l) => l.startsWith("data:"));
    if (dataLines.length === 0) return { go: false, detail: "tools/call 鏃?data 琛? };
    const payload = JSON.parse(dataLines[dataLines.length - 1].slice(5).trim());
    const contentArr = payload?.result?.content as Array<{ text?: string }> | undefined;
    const text = (contentArr ?? []).map((c) => c.text ?? "").join("");
    const inner = JSON.parse(text); // {messages:[...], count:N}
    const count = Number(inner.count);
    pi.logger?.info?.(`cron 任务 ${job.id} condition count=${count}`);
    return { go: count > 0, detail: `count=${count}` };
  }

  // 鍒扮偣瑙﹀彂锛氫竴娆℃€т换鍔＄Щ闄わ紝鍛ㄦ湡浠诲姟浠庡綋鍓嶆椂鍒婚『寤讹紙閿欒繃鍙ˉ璺戜竴娆★級銆?  // 蹇欑鏃舵寜 on_busy 鍒嗘祦锛歲ueue=followUp 鎺掗槦锛堥粯璁わ級锛沜ancel=鍙栨秷鏈瑙﹀彂
  async function fireDue(): Promise<void> {
    const now = Date.now();
    for (const job of [...jobs.values()]) {
      if (job.nextAt > now) continue;
      if (job.kind === "once") jobs.delete(job.id);
      else
        job.nextAt =
          job.kind === "daily" ? nextDailyAt(job.at as string, now) : now + (job.everySeconds as number) * 1000;
      persist();
      const busy = !ctxRef?.isIdle?.();
      // 鏉′欢闂ㄦ斁鏈€鍓嶏細count<=0 鐩存帴闈欓粯璺宠繃锛屼笉杩?LLM锛屼篃涓嶅彈 busy 褰卞搷
      const gate = await conditionGate(job);
      if (!gate.go) {
        pi.logger?.warn?.(`cron 浠诲姟 ${job.id} 鏉′欢鏈懡涓紝璺宠繃鏈锛?{gate.detail ?? ""}锛屼笅娆?${new Date(job.nextAt).toLocaleString()}锛塦);
        continue;
      }
      if (busy && job.onBusy === "cancel") {
        pi.logger?.warn?.(`cron 浠诲姟 ${job.id} 蹇欐椂鍙栨秷鏈瑙﹀彂锛堜笅娆?${new Date(job.nextAt).toLocaleString()}锛塦);
        continue;
      }
      const text = `鈴?瀹氭椂浠诲姟 [${job.name}] 瑙﹀彂锛岃鎵ц锛歕n${job.prompt}`;
      try {
        if (!busy) await pi.sendUserMessage(text);
        else await pi.sendUserMessage(text, { deliverAs: "followUp" });
      } catch (e) {
        pi.logger?.error?.(`cron 任务 ${job.id} 注入失败: ${String(e).slice(0, 200)}`);
      }
    }
  }

  const z = pi.zod;
  const textContent = (text: string) => ({ content: [{ type: "text" as const, text }] });

  pi.registerTool({
    name: "cron_add",
    label: "新建定时任务",
    description:
      `在当前会话建立定时任务，到点后把 prompt 作为用户消息注入会话驱动执行。` +
      `every_seconds / daily_at锛?HH:MM"锛屾湰鏈烘椂鍖猴級/ once_in_seconds 涓夐€変竴锛沗 +
      `鏈€灏忛棿闅?${MIN_SECONDS} 绉掞紝浠诲姟闅忎細璇濇寔涔呭寲銆俙 +
      `on_busy：会话忙碌时策略，queue=排队等空闲（默认），cancel=取消本次触发。` +
      `condition锛堝彲閫夛級锛?__TOKEN__ # <agent_id> # <endpoint>" 涓夋锛屽埌鐐瑰厛 get_messages 鍒?count>0 鎵嶆敞鍏ャ€乧ount=0 闈欓粯璺宠繃鏈涓嶈繘 LLM锛沗 +
      `tokenFile锛堝彲閫夛級锛氭槑鏂?token 鏂囦欢璺緞锛堣棣栬锛夛紝娉ㄥ叆 condition 鐨?__TOKEN__ 鍗犱綅銆俙,
    parameters: z.object({
      name: z.string(),
      prompt: z.string(),
      every_seconds: z.number().optional(),
      daily_at: z.string().optional(),
      once_in_seconds: z.number().optional(),
      on_busy: z.string().optional(),
      condition: z.string().optional(),
      tokenFile: z.string().optional(),
    }),
    async execute(_id: string, params: unknown) {
      try {
        if (jobs.size >= MAX_JOBS) return textContent(`瀹氭椂浠诲姟宸茶揪涓婇檺锛?{MAX_JOBS}锛夛紝璇峰厛 cron_remove銆俙);
        const name = asStr(field(params, "name")).trim().slice(0, MAX_NAME);
        const prompt = asStr(field(params, "prompt")).trim().slice(0, MAX_PROMPT);
        if (!name || !prompt) return textContent("name 鍜?prompt 鍧囧繀濉笖涓嶈兘涓虹┖銆?);
        const onBusy = asStr(field(params, "on_busy"));
        if (onBusy !== "" && onBusy !== "queue" && onBusy !== "cancel") {
          return textContent(`on_busy 浠呮敮鎸?"queue"锛堟帓闃燂級鎴?"cancel"锛堝彇娑堟湰娆★級锛屾敹鍒帮細${onBusy}`);
        }
        const condition = asStr(field(params, "condition")).trim();
        const tokenFile = asStr(field(params, "tokenFile")).trim();
        const sched = validateSchedule(params);
        const job: CronJob = {
          id: `${Date.now().toString(36)}-${(idSeq++).toString(36)}`,
          name,
          prompt,
          kind: sched.kind,
          everySeconds: sched.everySeconds,
          at: sched.at,
          nextAt: sched.nextAt,
          onBusy: onBusy === "" ? undefined : onBusy === "cancel" ? "cancel" : "queue",
          condition: condition || undefined,
          tokenFile: tokenFile || undefined,
          createdAt: Date.now(),
        };
        jobs.set(job.id, job);
        persist();
        return { ...textContent(`已建立定时任务：\n${formatJob(job)}`), details: { job } };
      } catch (e) {
        return textContent(`寤虹珛澶辫触锛?{e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  pi.registerTool({
    name: "cron_list",
    label: "列出定时任务",
    description: "鍒楀嚭褰撳墠浼氳瘽鐨勫叏閮ㄥ畾鏃朵换鍔★紙鍚笅娆¤Е鍙戞椂闂达級銆?,
    parameters: z.object({}),
    async execute() {
      const list = [...jobs.values()].sort((a, b) => a.nextAt - b.nextAt);
      return textContent(list.length ? list.map(formatJob).join("\n") : "褰撳墠娌℃湁瀹氭椂浠诲姟銆?);
    },
  });

  pi.registerTool({
    name: "cron_remove",
    label: "删除定时任务",
    description: "鎸?id 鍒犻櫎涓€涓畾鏃朵换鍔°€?,
    parameters: z.object({ id: z.string() }),
    async execute(_id: string, params: unknown) {
      const id = asStr(field(params, "id"));
      const gone = jobs.get(id);
      if (!id || !gone) return textContent(`鏈壘鍒板畾鏃朵换鍔?${id}锛屽彲鐢?cron_list 鏌ヨ銆俙);
      jobs.delete(id);
      persist();
      return textContent(`宸插垹闄わ細${gone.name}锛?{id}锛塦);
    },
  });

  pi.registerCommand?.("cron", {
    description: "鏌ョ湅褰撳墠浼氳瘽鐨勫畾鏃朵换鍔?,
    handler: async (_args: unknown, ctx: CronCtx) => {
      const list = [...jobs.values()].sort((a, b) => a.nextAt - b.nextAt);
      ctx.ui?.notify?.(list.length ? list.map(formatJob).join("\n") : "褰撳墠娌℃湁瀹氭椂浠诲姟銆?, "info");
    },
  });

  // 鍔犺浇鏃舵満鍗虫敞鍐屾湡锛涘畾鏃跺櫒鍙兘鍦ㄨ繍琛屾湡锛坰ession_start 涔嬪悗锛夊惎鍔ㄣ€?  pi.on("session_start", async (_event: unknown, ctx: CronCtx) => {
    ctxRef = ctx;
    restore(ctx); // 涓婃杩愯鐣欎笅鐨勪换鍔″湪姝ゅ娲伙紱杩囨湡鐨?nextAt 棣栦釜 tick 琛ヨ窇涓€娆?    timer = ctx.setInterval?.(() => {
      void fireDue();
    }, TICK_MS);
  });

  // 鍒囧垎鏀?/ 鍒囨爲瑙嗗浘鍚庝互褰撳墠鍒嗘敮涓哄噯閲嶅缓浠诲姟琛ㄣ€?  pi.on("session_branch", async (_event: unknown, ctx: CronCtx) => {
    restore(ctx);
  });
  pi.on("session_tree", async (_event: unknown, ctx: CronCtx) => {
    restore(ctx);
  });

  pi.on("session_shutdown", async () => {
    // 鎵樼瀹氭椂鍣ㄦ湰浼氶殢浼氳瘽鑷姩娓呯悊锛涜繖閲屾樉寮忔竻涓€娆″苟閲婃斁寮曠敤銆?    if (timer !== null) ctxRef?.clearTimer?.(timer);
    timer = null;
    ctxRef = null;
  });
}
