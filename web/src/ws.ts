// Single multiplexed websocket with topic subscriptions and auto-reconnect.
// Also reports whether the page is active (visible and focused): the server skips desktop
// notifications for sessions whose chat is open in an active page. A quiet subscription (a chat kept up with, not
// shown: chatStore) doesn't count as open.

import { api } from "./api";

type Handler = (msg: any) => void;

const pageActive = () => document.visibilityState === "visible" && document.hasFocus();

class Socket {
  private ws: WebSocket | null = null;
  private handlers = new Map<string, Map<Handler, boolean>>(); // topic -> its handlers, each quiet or not
  private backoff = 500;
  private reconnectListeners = new Set<() => void>();
  private everConnected = false;
  private active = pageActive();
  connected = false;
  /** This page, to the server: a chat isn't discarded under a page that has it open (store.ts). Not crypto.randomUUID,
   *  which needs https: a phone may reach Atrium over plain http. */
  readonly page = Math.random().toString(36).slice(2) + Date.now().toString(36);

  constructor() {
    this.connect();
    // read after the event settles: focus moving into an iframe still counts as this page
    const update = () => setTimeout(() => this.setActive(pageActive()));
    document.addEventListener("visibilitychange", update);
    window.addEventListener("focus", update);
    window.addEventListener("blur", update);
  }

  private url() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }

  private connect() {
    const ws = new WebSocket(this.url());
    this.ws = ws;
    let opened = false;
    ws.onopen = () => {
      opened = true;
      this.connected = true;
      this.backoff = 500;
      for (const topic of this.handlers.keys()) this.send(topic);
      ws.send(JSON.stringify({ op: "presence", active: this.active, page: this.page }));
      if (this.everConnected) this.reconnectListeners.forEach((f) => f());
      this.everConnected = true;
    };
    ws.onmessage = (ev) => {
      let msg: any;
      try {
        msg = JSON.parse(ev.data);
      } catch {
        return;
      }
      for (const h of this.handlers.get(msg.topic)?.keys() ?? []) h(msg);
    };
    ws.onclose = () => {
      this.connected = false;
      // refused: the server is down (restarting?) or the login is gone, which a request tells (see api.ts)
      if (!opened) void api.settings().catch(() => undefined);
      setTimeout(() => this.connect(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, 8000);
    };
  }

  /** A topic is quiet while all its handlers are. */
  private quiet(topic: string): boolean {
    return [...(this.handlers.get(topic)?.values() ?? [])].every(Boolean);
  }

  private send(topic: string) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ op: "sub", topic, ...(this.quiet(topic) && { quiet: true }) }));
  }

  subscribe(topic: string, handler: Handler, quiet = false): () => void {
    const hs = this.handlers.get(topic);
    const was = hs ? this.quiet(topic) : null;
    if (!hs) this.handlers.set(topic, new Map([[handler, quiet]]));
    else hs.set(handler, quiet);
    if (was !== this.quiet(topic)) this.send(topic);
    return () => {
      const s = this.handlers.get(topic);
      if (!s?.has(handler)) return;
      const before = this.quiet(topic);
      s.delete(handler);
      if (s.size === 0) {
        this.handlers.delete(topic);
        if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ op: "unsub", topic }));
      } else if (this.quiet(topic) !== before) this.send(topic);
    };
  }

  /** Makes `handler`'s subscription to `topic` quiet, or not any more. */
  setQuiet(topic: string, handler: Handler, quiet: boolean) {
    const hs = this.handlers.get(topic);
    if (!hs?.has(handler) || hs.get(handler) === quiet) return;
    const was = this.quiet(topic);
    hs.set(handler, quiet);
    if (this.quiet(topic) !== was) this.send(topic);
  }

  onReconnect(f: () => void): () => void {
    this.reconnectListeners.add(f);
    return () => this.reconnectListeners.delete(f);
  }

  private setActive(active: boolean) {
    if (active === this.active) return;
    this.active = active;
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ op: "presence", active }));
  }
}

export const socket = new Socket();
