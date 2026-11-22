// The conversations of the agents opened lately, and of every live agent (its process attached), kept between visits:
// a chat opened on one shows at once. The live agents' are loaded ahead, in the background, one after another while
// the page is idle. Each is its session's snapshot (api.session) plus what the session's stream (session:<sid>) brought
// since. While no chat shows it, its stream is still followed, quietly (it doesn't count as watched: desktop
// notifications still come), so that a live agent's conversation is as current as if it had stayed open. Where the
// stream may have missed something (the socket reconnected, an older server that can't follow quietly, its transcript
// written to elsewhere), it is shown as it was and loaded again; what is unchanged keeps its objects, and what shows it
// doesn't render again.
import { useCallback, useSyncExternalStore } from "react";
import { api, Goal, Item, LiveState, SessionRow, SessionSettings } from "../api";
import { socket } from "../ws";
import { useApp } from "../store";
import { useOutbox } from "./outbox";

type ResultItem = Extract<Item, { kind: "tool_result" }>;
type PermItem = Extract<Item, { kind: "permission" }>;

export interface ChatState {
  items: Item[];
  seen: Set<string>;
  results: Record<string, ResultItem>;
  pending: Record<string, PermItem>;
  resolved: Set<string>;
  streaming: string;
  state: LiveState | null;
  mode: string | null;
  model: string | null;
  effort: string | null; // the thread's effort when nothing overrides it, if the harness says (Codex)
  usage: { used: number | null; max: number | null } | null;
  goal: Goal | null; // the thread's goal (Codex)
}

export const empty = (): ChatState => ({
  items: [], seen: new Set(), results: {}, pending: {}, resolved: new Set(), streaming: "", state: null, mode: null, model: null, effort: null, usage: null,
  goal: null,
});

/** The chat with `its` applied, in order. What they change is copied once, not once per item: a long conversation's
 *  snapshot, a frame's worth of live items. */
export function applyAll(st: ChatState, its: Item[]): ChatState {
  const next: ChatState = { ...st };
  let items: Item[] | null = null, seen: Set<string> | null = null, resolved: Set<string> | null = null;
  let results: ChatState["results"] | null = null, pending: ChatState["pending"] | null = null;
  let changed = false; // an item seen before changes nothing: the same state, and no render
  for (const it of its) {
    switch (it.kind) {
      case "delta":
        next.streaming += it.text;
        break;
      case "state":
        next.state = it.state;
        if (it.state !== "running") next.streaming = "";
        break;
      case "usage":
        next.usage = { used: it.used, max: it.max };
        break;
      case "goal":
        next.goal = it.goal;
        break;
      case "tool_result":
        (results ??= { ...st.results })[it.id] = it;
        break;
      case "permission":
        if ((resolved ?? st.resolved).has(it.id)) continue;
        (pending ??= { ...st.pending })[it.id] = it;
        break;
      case "permission_resolved":
        delete (pending ??= { ...st.pending })[it.id];
        (resolved ??= new Set(st.resolved)).add(it.id);
        break;
      default: {
        if ((seen ?? st.seen).has(it.id)) {
          // harnesses may re-send a tool call once its input is final (upsert)
          const i = it.kind === "tool" ? (items ?? st.items).findIndex((x) => x.kind === "tool" && x.id === it.id) : -1;
          if (i < 0) continue;
          (items ??= [...st.items])[i] = it;
          break;
        }
        (seen ??= new Set(st.seen)).add(it.id);
        (items ??= [...st.items]).push(it);
        if (it.kind === "text") next.streaming = "";
        if (it.kind === "system" && it.subtype === "init") {
          next.mode = it.permission_mode ?? next.mode;
          next.model = it.model ?? next.model;
          next.effort = it.effort ?? next.effort;
        }
      }
    }
    changed = true;
  }
  if (!changed) return st;
  if (items) next.items = items;
  if (seen) next.seen = seen;
  if (results) next.results = results;
  if (pending) next.pending = pending;
  if (resolved) next.resolved = resolved;
  return next;
}

/** What a chat shows of its session. */
export interface Chat {
  st: ChatState;
  row: SessionRow | null; // the session's row as loaded with it (the store's list leaves archived ones out)
  settings: SessionSettings;
  loading: boolean; // its first load is under way: nothing to show yet
}

interface Slot {
  key: string;
  pid: string;
  sid: string;
  chat: Chat; // a new object for every change: what the chats showing it compare
  listeners: Set<() => void>; // the chats showing it
  ready: boolean; // the stream's items go in as they come; else they wait for the load under way (buffer)
  stale: boolean; // the stream may have missed some: loaded again when it is shown
  failed: boolean; // its last load failed: not tried again in the background (when it is shown, it is)
  buffer: Item[];
  queued: Item[]; // live items go in once a frame, together: a burst of them, or a reply streamed token by token
  frame: number;
  loads: number; // loads asked for: the answer to one asked for again since is dropped
  pending: Promise<void>; // the last load (settled once it has landed or failed)
  stream: ((msg: any) => void) | null; // its handler on the session's topic, while it follows it
  unsub: (() => void) | null;
  used: number; // when it was last shown or asked for (performance.now): the least recent go first
  rowAt: number | null; // its row's updated_at as loaded: a later one in the list means its transcript was written to
}

const KEEP = 8; // conversations kept besides those shown, kept hidden or of a live agent: the least lately shown go
const WARM = 12; // live agents' conversations loaded ahead
const ATTACHED = new Set<LiveState>(["starting", "running", "waiting", "idle"]); // its process runs (idle: until closed)
const slots = new Map<string, Slot>();
const kept = new Set<string>(); // those that chats kept hidden (App's ChatColumn) show: never dropped
const NO_SETTINGS: SessionSettings = { model: "", effort: "", mode: "" };
const keyOf = (pid: string, sid: string) => `${pid}/${sid}`;
/** The server can be followed quietly (else the chats it has open are followed, and those only). */
const quietOK = () => !!useApp.getState().meta?.features?.includes("quiet_subscriptions");

function set(s: Slot, chat: Chat) {
  s.chat = chat;
  for (const f of s.listeners) f();
}

function open(pid: string, sid: string): Slot {
  const key = keyOf(pid, sid);
  let s = slots.get(key);
  if (!s) {
    s = {
      key, pid, sid, chat: { st: empty(), row: null, settings: NO_SETTINGS, loading: true }, listeners: new Set(), ready: false, stale: false, failed: false,
      buffer: [], queued: [], frame: 0, loads: 0, pending: Promise.resolve(), stream: null, unsub: null, used: performance.now(), rowAt: null,
    };
    slots.set(key, s);
    follow(s);
    load(s);
    trim();
  }
  return s;
}

function follow(s: Slot) {
  const stream = (msg: any) => {
    if (msg.type === "settings") return set(s, { ...s.chat, settings: msg.settings });
    if (msg.type === "reload") {
      // a message was changed: the conversation is the one up to it, from here on (new items wait for it)
      if (s.listeners.size) load(s);
      else s.stale = true;
      return;
    }
    if (msg.type !== "item" || s.stale) return;
    if (!s.ready) return void s.buffer.push(msg.item);
    s.queued.push(msg.item);
    if (s.queued.length >= 1000) flush(s); // a hidden page gets no frames until it is shown
    else s.frame ||= requestAnimationFrame(() => flush(s));
  };
  s.stream = stream;
  s.unsub = socket.subscribe(`session:${s.sid}`, stream, !s.listeners.size);
}

function unfollow(s: Slot) {
  s.unsub?.();
  s.unsub = s.stream = null;
  cancelAnimationFrame(s.frame);
  s.frame = 0;
  s.queued = [];
}

function flush(s: Slot) {
  cancelAnimationFrame(s.frame);
  s.frame = 0;
  const its = s.queued;
  s.queued = [];
  const st = applyAll(s.chat.st, its);
  if (st !== s.chat.st) set(s, { ...s.chat, st });
}

function load(s: Slot): Promise<void> {
  const n = ++s.loads;
  s.ready = false;
  s.stale = false;
  // what came before this is asked for, the snapshot has; what comes while it is on its way waits for it
  s.buffer = [];
  cancelAnimationFrame(s.frame);
  s.frame = 0;
  s.queued = [];
  const asked = performance.now();
  return (s.pending = api
    .session(s.pid, s.sid)
    .then((snap) => {
      if (n !== s.loads || slots.get(s.key) !== s) return; // asked for again since, or dropped
      const buffer = s.buffer;
      const st = applyAll(empty(), [...snap.items, ...buffer]);
      // drop permissions the server no longer considers pending
      const pend = new Set(snap.pending);
      st.pending = Object.fromEntries(Object.entries(st.pending).filter(([k]) => pend.has(k) || buffer.some((b) => b.kind === "permission" && b.id === k)));
      st.state = snap.live ? snap.state : null;
      if (!st.model && snap.settings?.model) st.model = snap.settings.model;
      s.ready = true;
      s.failed = false;
      s.buffer = [];
      s.rowAt = snap.session?.updated_at ?? null;
      set(s, { st: reuse(s.chat.st, st), row: snap.session, settings: snap.settings ?? s.chat.settings, loading: false });
      useOutbox.getState().settle(s.sid, st.seen, asked); // the messages on their way it has
    })
    .catch((e) => {
      if (n !== s.loads || slots.get(s.key) !== s) return;
      s.stale = s.failed = true; // loaded again when next shown
      if (s.listeners.size) useApp.getState().toast(String(e.message || e), "error");
      set(s, { ...s.chat, loading: false });
    }));
}

/** A chat showing `s` from now on: `cb` hears of its changes. */
function listen(s: Slot, cb: () => void): () => void {
  s.listeners.add(cb);
  s.used = performance.now();
  if (!s.stream) follow(s); // (not followed while hidden: an older server)
  else socket.setQuiet(`session:${s.sid}`, s.stream, false);
  const row = useApp.getState().sessions.find((x) => x.id === s.sid);
  // a live agent's stream says all; one that isn't live may have been carried on elsewhere (its CLI, in a terminal)
  if (s.stale || (!attached(s) && !s.chat.loading && row && s.rowAt !== null && row.updated_at > s.rowAt)) load(s);
  return () => {
    s.listeners.delete(cb);
    if (s.listeners.size) return;
    s.used = performance.now();
    if (!quietOK()) {
      unfollow(s);
      s.stale = true;
    } else if (s.stream) socket.setQuiet(`session:${s.sid}`, s.stream, true);
    trim();
  };
}

/** Its agent runs (in Atrium: a process attached, busy or idle until it is closed). */
function attached(s: Slot): boolean {
  const state = s.chat.st.state ?? useApp.getState().sessions.find((r) => r.id === s.sid)?.live ?? null;
  return !!state && ATTACHED.has(state);
}

function drop(s: Slot) {
  unfollow(s);
  slots.delete(s.key);
}

/** What no chat shows or keeps hidden goes if it is another project's; of the rest, beyond KEEP, the least lately shown
 *  of those whose agent doesn't run. */
function trim() {
  const pid = useApp.getState().project?.id;
  const idle = [...slots.values()].filter((s) => !s.listeners.size && !kept.has(s.key));
  for (const s of idle) if (s.pid !== pid) drop(s);
  const spare = idle.filter((s) => s.pid === pid && !attached(s)).sort((a, b) => a.used - b.used);
  for (const s of spare.slice(0, Math.max(0, spare.length - KEEP))) drop(s);
}

let warming = false;
const later = (f: () => void) => (typeof requestIdleCallback === "function" ? requestIdleCallback(f, { timeout: 2000 }) : setTimeout(f, 300));

/** Loads the next live agent's conversation that isn't kept yet (or may have missed something), up to WARM, then the
 *  one after it once the page is idle again: opening any of them then shows it at once. */
function warm() {
  if (warming || document.visibilityState !== "visible" || !quietOK()) return;
  const { project, sessions } = useApp.getState();
  if (!project) return;
  const live = sessions.filter((r) => r.role === "worker" && !r.archived && !!r.live && ATTACHED.has(r.live)).slice(0, WARM);
  const s = live.map((r) => slots.get(keyOf(project.id, r.id))).find((x) => x && x.stale && !x.failed && !x.listeners.size);
  const next = live.find((r) => !slots.has(keyOf(project.id, r.id)));
  if (!s && !next) return;
  warming = true;
  void (s ? load(s) : open(project.id, next!.id).pending).finally(() => {
    warming = false;
    later(warm);
  });
}

/** The conversation of `sid`, kept up with while it is kept (and loaded first if it isn't). */
export function useChat(pid: string, sid: string): Chat {
  const s = open(pid, sid);
  const subscribe = useCallback((cb: () => void) => listen(s, cb), [s]);
  return useSyncExternalStore(subscribe, () => s.chat);
}

/** Changes what a chat shows of its session (the mode just set, …). */
export function updateChat(pid: string, sid: string, fn: (c: Chat) => Chat) {
  const s = slots.get(keyOf(pid, sid));
  if (s) set(s, fn(s.chat));
}

/** Loads `sid`'s conversation ahead of its chat (a finger or a button going down on it). */
export function prefetchChat(pid: string, sid: string) {
  const s = slots.get(keyOf(pid, sid));
  if (!s) {
    if (quietOK()) open(pid, sid);
  } else {
    s.used = performance.now();
    if (s.stale && !s.listeners.size && s.stream) load(s);
  }
}

/** The sessions whose chats are kept hidden (App's ChatColumn): their conversations stay. */
export function keepChats(pid: string, sids: string[]) {
  kept.clear();
  for (const sid of sids) kept.add(keyOf(pid, sid));
  trim();
}

// the stream may have missed items while the socket was down: shown chats load again, the others when shown
socket.onReconnect(() => {
  for (const s of slots.values()) {
    if (s.listeners.size) load(s);
    else s.stale = true;
  }
});

// another project (its conversations go, as its chats close), or agents that started or stopped: kept as they should be
useApp.subscribe((now, before) => {
  if (now.project === before.project && now.sessions === before.sessions) return;
  trim();
  later(warm);
});
document.addEventListener("visibilitychange", () => later(warm));

/** `next` with what is unchanged from `prev` as the same objects (all of `prev`, if nothing changed): a conversation
 *  loaded again renders only what changed. */
function reuse(prev: ChatState, next: ChatState): ChatState {
  if (!prev.items.length) return next;
  const old = new Map<string, Item>();
  for (const it of prev.items) if ("id" in it) old.set(it.id, it);
  let sameItems = prev.items.length === next.items.length;
  const items = next.items.map((it, i) => {
    const o = "id" in it ? old.get(it.id) : undefined;
    const keep = o && equal(o, it) ? o : it;
    if (keep !== prev.items[i]) sameItems = false;
    return keep;
  });
  let sameResults = Object.keys(prev.results).length === Object.keys(next.results).length;
  const results: ChatState["results"] = {};
  for (const [k, r] of Object.entries(next.results)) {
    const o = prev.results[k];
    results[k] = o && equal(o, r) ? o : r;
    if (results[k] !== o) sameResults = false;
  }
  const out: ChatState = {
    ...next,
    items: sameItems ? prev.items : items,
    seen: sameItems ? prev.seen : next.seen,
    results: sameResults ? prev.results : results,
    pending: equal(prev.pending, next.pending) ? prev.pending : next.pending,
    resolved: equal([...prev.resolved], [...next.resolved]) ? prev.resolved : next.resolved,
    usage: equal(prev.usage, next.usage) ? prev.usage : next.usage,
    goal: equal(prev.goal, next.goal) ? prev.goal : next.goal,
  };
  return (Object.keys(out) as (keyof ChatState)[]).every((k) => out[k] === prev[k]) ? prev : out;
}

/** Whether two values read from JSON are the same. */
function equal(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null || Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    const y = b as unknown[];
    if (a.length !== y.length) return false;
    for (let i = 0; i < a.length; i++) if (!equal(a[i], y[i])) return false;
    return true;
  }
  const x = a as Record<string, unknown>, y = b as Record<string, unknown>;
  const ks = Object.keys(x);
  if (ks.length !== Object.keys(y).length) return false;
  for (const k of ks) if (!(k in y) || !equal(x[k], y[k])) return false;
  return true;
}
