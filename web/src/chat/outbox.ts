// Messages on their way to an agent. Sending waits on the server — an agent closed while idle starts again first, a
// changed message starts its process over — so the chat shows a message the moment it is sent, and the composer is
// free for the next one meanwhile. The server's own copy of the message comes back on the session's stream under the
// id given here, and takes its place. A message that fails goes back where it was written, and so do those after it.
// What hasn't gone yet is kept in localStorage as well, under this page with a heartbeat: a page reloaded, closed or
// dropped by the phone meanwhile leaves it there, and a later page puts back in their reply boxes the messages their
// agent never got (reclaim, as their chats load).
import { create } from "zustand";
import type { Attachment, Upload } from "../api";
import { useDrafts } from "../drafts";
import { socket } from "../ws";
import type { PendingUpload } from "./Composer";

export interface Outgoing {
  id: string; // its chat item's id (the server keeps it)
  text: string;
  uploads: Upload[];
  command?: boolean; // one of the harness's slash commands, as far as the chat knows them
  move?: boolean; // the agent is moved first (its worktree was removed): until that is done, nothing else moves it
  edit?: string; // the message it replaces: until the server has gone back there, the chat shows what came before it
  sent?: number; // when the server took it (performance.now()): its item was on the stream by then
}

interface Outbox {
  bySession: Record<string, Outgoing[]>;
  add: (sid: string, o: Outgoing) => void;
  sent: (sid: string, id: string) => void;
  /** Those that are in the chat now (`seen`: its item ids), or that went before `since` (a load of the chat asked
   *  for then has them, under another id if the agent was started again since: its history's). */
  settle: (sid: string, seen: Set<string>, since?: number) => void;
  drop: (sid: string, ids: string[]) => void;
}

const NONE: Outgoing[] = [];

export const useOutbox = create<Outbox>((set) => {
  const change = (sid: string, fn: (xs: Outgoing[]) => Outgoing[]) =>
    set((s) => {
      const xs = s.bySession[sid] ?? NONE;
      const next = fn(xs);
      if (next === xs) return s;
      const bySession = { ...s.bySession, [sid]: next };
      if (!next.length) delete bySession[sid];
      return { bySession };
    });
  return {
    bySession: {},
    add: (sid, o) => change(sid, (xs) => [...xs, o]),
    sent: (sid, id) => change(sid, (xs) => (xs.some((o) => o.id === id) ? xs.map((o) => (o.id === id ? { ...o, sent: performance.now() } : o)) : xs)),
    settle: (sid, seen, since) =>
      change(sid, (xs) => {
        const left = xs.filter((o) => !seen.has(o.id) && !(since !== undefined && o.sent !== undefined && o.sent < since));
        return left.length === xs.length ? xs : left;
      }),
    drop: (sid, ids) =>
      change(sid, (xs) => {
        const left = xs.filter((o) => !ids.includes(o.id));
        return left.length === xs.length ? xs : left;
      }),
  };
});

export const useOutgoing = (sid: string) => useOutbox((s) => s.bySession[sid]) ?? NONE;

/** A new message id, as the server makes them. (Not crypto.randomUUID: a page served over plain http, a phone on the
 *  LAN, has none.) */
export function newMessageId(): string {
  return "u-" + Array.from(crypto.getRandomValues(new Uint8Array(6)), (b) => b.toString(16).padStart(2, "0")).join("");
}

/** How a message on its way shows its attachments: as the server will. */
export const sentAttachments = (uploads: Upload[]): Attachment[] => uploads.map((u) => ({ name: u.name, mime: u.mime, path: u.path, url: u.url }));

export interface Returns {
  error: (e: unknown) => void; // it failed (those after it don't go either: they come back too, without one)
  back?: () => boolean; // its own way back (a changed message: its editor); false, or none: the reply box
}

interface Job extends Returns {
  o: Outgoing;
  send: () => Promise<{ id?: string }>; // the answer names the id the message's item got
}

const queues = new Map<string, Job[]>();

/** Show `o` in its chat and `send` it, after the session's messages still on their way: they reach the agent in the
 *  order they were sent. If it fails, those after it don't go either, and each goes back: `back`, else the reply box,
 *  in order (as one text: a slash command doesn't wait behind a message, nor a message behind one, see ChatView). */
export function post(sid: string, o: Outgoing, send: Job["send"], returns: Returns) {
  const q = queues.get(sid);
  if (q) q.push({ o, send, ...returns });
  else queues.set(sid, [{ o, send, ...returns }]);
  useOutbox.getState().add(sid, o); // (with its job queued: kept as the one on its way, if it is)
  if (!q) void pump(sid);
}

async function pump(sid: string) {
  const q = queues.get(sid)!;
  try {
    while (q.length) {
      const job = q[0]!;
      try {
        const got = await job.send();
        q.shift();
        // an item under another id (a server from before they were kept, or this one was taken) is shown as the server's
        if (got?.id === job.o.id) useOutbox.getState().sent(sid, job.o.id);
        else useOutbox.getState().drop(sid, [job.o.id]);
      } catch (e) {
        if (leaving) return; // the page goes, and its requests with it (one may have arrived): kept for the next (reclaim)
        const jobs = q.splice(0); // it, and those after it: none went
        useOutbox.getState().drop(sid, jobs.map((j) => j.o.id));
        job.error(e);
        backToComposer(sid, jobs.filter((j) => !j.back?.()).map((j) => j.o));
      }
    }
  } finally {
    queues.delete(sid); // whatever happened: the next message starts the session's queue again
  }
}

// ---- kept in localStorage ------------------------------------------------------------------------------------------

const KEY = "atrium.outbox.";
const MINE = KEY + socket.page;
const GONE = 15000; // a page that hasn't beaten for this long sends nothing more (one marked gone: pagehide, at once)

/** A message that hasn't gone yet; `going`: its request has left (it may still arrive). */
interface Left {
  sid: string;
  id: string;
  text: string;
  uploads: Upload[];
  going: boolean;
}

let beat = 0;
let leaving = false;
function keep(gone = false) {
  const left: Left[] = [];
  for (const [sid, xs] of Object.entries(useOutbox.getState().bySession))
    for (const o of xs) if (o.sent === undefined) left.push({ sid, id: o.id, text: o.text, uploads: o.uploads, going: queues.get(sid)?.[0]?.o.id === o.id });
  try {
    if (left.length) localStorage.setItem(MINE, JSON.stringify({ beat: Date.now(), gone, left }));
    else localStorage.removeItem(MINE);
  } catch {
    /* storage full: what goes is still in memory */
  }
  if (left.length && !gone) beat ||= window.setInterval(keep, 5000);
  else if (beat) {
    clearInterval(beat);
    beat = 0;
  }
}
useOutbox.subscribe(() => keep());
addEventListener("pagehide", () => {
  leaving = true;
  keep(true);
});
addEventListener("pageshow", (e) => {
  leaving = false;
  if (e.persisted) keep(); // back from the page cache: it sends them after all
});

/** Whether a page that is gone left messages behind that haven't been put back yet. */
export const leftBehind = () => Object.keys(localStorage).some((k) => k.startsWith(KEY) && k !== MINE);

/** The messages pages gone before they went left for `sid`: those its agent got (`seen`) are done, the others go back
 *  in its reply box (one whose request had left only once it had the time to arrive). How many went back. */
export function reclaim(sid: string, seen: Set<string>): number {
  const back: Left[] = [];
  const now = Date.now();
  for (const key of Object.keys(localStorage)) {
    if (!key.startsWith(KEY) || key === MINE) continue;
    let rec: { beat: number; gone?: boolean; left: Left[] };
    try {
      rec = JSON.parse(localStorage.getItem(key) || "");
    } catch {
      localStorage.removeItem(key);
      continue;
    }
    const silent = now - rec.beat > GONE;
    if (!rec.gone && !silent) continue; // its page still sends them
    const stay = rec.left.filter((m) => {
      if (m.sid !== sid) return true;
      if (seen.has(m.id)) return false;
      if (m.going && !silent) return true;
      back.push(m);
      return false;
    });
    if (stay.length === rec.left.length) continue;
    if (stay.length) localStorage.setItem(key, JSON.stringify({ ...rec, left: stay }));
    else localStorage.removeItem(key);
  }
  backToComposer(sid, back);
  return back.length;
}

/** Uploads as the composer holds them (an image previewed from its URL: the composer's own preview is gone). */
export const pendingUploads = (uploads: Upload[]): PendingUpload[] =>
  uploads.map((u) => ({ key: `back-${u.id}-${Math.random().toString(36).slice(2)}`, name: u.name, mime: u.mime, preview: u.mime.startsWith("image/") ? u.url : undefined, upload: u }));

/** Messages that didn't go, back in the session's composer, in order and ahead of what was typed there since. */
export function backToComposer(sid: string, msgs: { text: string; uploads: Upload[] }[]) {
  if (!msgs.length) return;
  useDrafts.getState().updateReply(sid, (d) => ({
    text: [...msgs.map((m) => m.text).filter(Boolean), d.text].filter(Boolean).join("\n\n"),
    attachments: [...pendingUploads(msgs.flatMap((m) => m.uploads)), ...d.attachments],
  }));
}
