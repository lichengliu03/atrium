import type { JSX } from "react";
import { useEffect, useState } from "react";
import { Goal as GoalIcon, Pause, Play, X } from "lucide-react";
import type { Goal } from "../api";
import { clsx } from "../util";
import { useTip } from "../components/ui";
import { fmtTokens } from "./Composer";

/** What each status is called on the bar. */
const LABEL: Record<Goal["status"], string> = {
  active: "Goal active",
  paused: "Goal paused",
  blocked: "Goal blocked",
  usage_limited: "Usage limit hit",
  budget_limited: "Budget used up",
  complete: "Goal achieved",
};

/** What it means, in the tip. */
const MEANING: Record<Goal["status"], string> = {
  active: "Not running now: Codex carries on with it after the agent's next turn, or ▶ now.",
  paused: "Paused: Codex starts no turns for it until it's resumed.",
  blocked: "The agent stopped: it says it can't get further without you. Answer it, or resume.",
  usage_limited: "Stopped at the account's usage limit. Resume once there is room again.",
  budget_limited: "Its token budget is used up: Codex stopped working on it.",
  complete: "The agent checked the objective against the work and reported it achieved.",
};

const RESUMABLE = new Set<Goal["status"]>(["active", "paused", "blocked", "usage_limited"]);

/** 12s, 5m, 1h 30m, 2d 3h 0m: a goal's time, counted as Codex's TUI does. */
export function fmtGoalTime(s: number): string {
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), h = Math.floor(m / 60);
  if (!h) return `${m}m`;
  if (h >= 24) return `${Math.floor(h / 24)}d ${h % 24}h ${m % 60}m`;
  return m % 60 ? `${h}h ${m % 60}m` : `${h}h`;
}

/** `on`, and for `ms` after it turns off: Codex ends one of the goal's turns and starts the next a moment later. */
function useLinger(on: boolean, ms: number): boolean {
  const [v, setV] = useState(on);
  useEffect(() => {
    if (on) {
      setV(true);
      return;
    }
    const t = window.setTimeout(() => setV(false), ms);
    return () => window.clearTimeout(t);
  }, [on, ms]);
  return v;
}

/** The thread's goal (Codex's /goal), over the composer: whether the agent is pursuing it, for how long and how many
 *  tokens (of its budget), and what it is; tapped, all of it. Its buttons send /goal commands (`onCommand`). */
export function GoalBar({ goal, running, onCommand }: { goal: Goal; running: boolean; onCommand: (cmd: string) => void }) {
  const busy = useLinger(running, 1500);
  const pursuing = goal.status === "active" && busy;
  // the time Codex counted, and the turn's own since then, second by second
  const [since, setSince] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => setSince(Date.now()), [goal, pursuing]);
  useEffect(() => {
    if (!pursuing) return;
    const t = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, [pursuing]);
  const time = goal.time_used + (pursuing ? Math.max(0, Math.floor((now - since) / 1000)) : 0);
  const budget = goal.token_budget;
  const pct = budget ? Math.min(100, Math.round((100 * goal.tokens_used) / budget)) : null;
  const [tipProps, tip] = useTip(
    <div className="goal-tip">
      <div className="k">{pursuing ? "Codex is working toward this goal:" : `${LABEL[goal.status]}:`}</div>
      <div className="goal-tip-text">{goal.objective}</div>
      <div>
        {fmtGoalTime(time)} · {fmtTokens(goal.tokens_used)} tokens{budget ? ` of ${fmtTokens(budget)} (${pct}%)` : ""}
      </div>
      <div className="k">{pursuing ? "It starts turns on its own until the goal is achieved." : MEANING[goal.status]}</div>
    </div>,
    { tap: true },
  );
  const button = (label: string, cmd: string, icon: JSX.Element) => (
    <button className="icon-btn sm" aria-label={label} data-tip={label} onClick={() => onCommand(cmd)}>
      {icon}
    </button>
  );
  return (
    <div className={clsx("goal-bar", goal.status, pursuing && "pursuing")}>
      <span className="goal-main" {...tipProps}>
        <GoalIcon size={14} className="goal-ico" aria-hidden />
        <span className="goal-state">{pursuing ? "Pursuing goal" : LABEL[goal.status]}</span>
        <span className="goal-usage">
          {fmtGoalTime(time)}
          {budget ? ` · ${fmtTokens(goal.tokens_used)} / ${fmtTokens(budget)}` : goal.tokens_used ? <span className="goal-tokens"> · {fmtTokens(goal.tokens_used)}</span> : null}
        </span>
        <span className="goal-text">{goal.objective}</span>
        {tip}
      </span>
      {goal.status === "active" && button("Pause the goal: no more turns of its own", "/goal pause", <Pause size={13} />)}
      {RESUMABLE.has(goal.status) && !pursuing && button("Resume the goal: Codex carries on with it now", "/goal resume", <Play size={13} />)}
      {button("Clear the goal", "/goal clear", <X size={14} />)}
      {pct !== null && <span className="goal-meter" style={{ width: `${pct}%` }} aria-hidden />}
    </div>
  );
}
