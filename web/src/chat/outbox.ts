// Messages on their way to an agent. Sending waits on the server — an agent closed while idle starts again first, a
// changed message starts its process over — so the chat shows a message the moment it is sent, and the composer is
// free for the next one meanwhile. The server's own copy of the message comes back on the session's stream under the
// id given here, and takes its place. A message that fails goes back where it was written.
import { create } from "zustand";
import type { Attachment, Upload } from "../api";
import { useDrafts } from "../drafts";
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

interface Job {
  o: Outgoing;
  send: () => Promise<{ id?: string }>; // the answer names the id the message's item got
  fail: (e: unknown, later: Outgoing[]) => void;
}

const queues = new Map<string, Job[]>();

/** Show `o` in its chat and `send` it, after the session's messages still on their way: they reach the agent in the
 *  order they were sent. If it fails, those after it don't go either: `fail` gets them (`later`), to put back. */
export function post(sid: string, o: Outgoing, send: Job["send"], fail: Job["fail"]) {
  useOutbox.getState().add(sid, o);
  const q = queues.get(sid);
  if (q) return void q.push({ o, send, fail });
  queues.set(sid, [{ o, send, fail }]);
  void pump(sid);
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
        const later = q.splice(0).slice(1).map((j) => j.o);
        useOutbox.getState().drop(sid, [job.o.id, ...later.map((o) => o.id)]);
        job.fail(e, later);
      }
    }
  } finally {
    queues.delete(sid); // whatever happened: the next message starts the session's queue again
  }
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
