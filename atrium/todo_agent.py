"""TODO agents — the board's TODOs that are the human's, done by agents Atrium starts for them.

A TODO in the human's list (one they added, or an agent's suggestion they accepted) gets an agent of its own
``todo_agent.delay_seconds`` after it got there, ``todo_agent.max_concurrent`` of them at work at once, the others in
the order they came; a TODO's ▶ on the board starts one at once. In a git project the agent works in a worktree of its
own, off the branch the project directory has: it commits there and fast-forwards that branch to its work
(``todo_agent.merge``), then checks the TODO off. What only the human can do or decide, it prepares and leaves to them:
it reports waiting, and the TODO says it needs them.

Its agent is an ordinary one (role ``worker``: the board's tools, reviewed like any other) whose session meta names its
TODO (``todo``: the id, and where it merges). The rail shows them in a section of their own, and they are archived once
idle for ``todo_agent.archive_after_days``. The TODO keeps its agent in board.json as ``worker``: {session, name, state,
at, error, briefed}, state starting | working | waiting | done | stopped | failed | deleted. The board's switch
(``todo_auto``) turns this on or off per project.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import time
from typing import TYPE_CHECKING, Any

from . import gitops
from .board.prompts import TODO_AGENT_APPEND, TODO_FINISH_KEEP, TODO_FINISH_MERGE, TODO_WHERE_DIR, TODO_WHERE_WORKTREE
from .board.template import due_for_agent, in_list_since, is_suggestion
from .config import config
from .events import hub
from .harness import get_harness
from .harness.base import Attachment, Item
from .review import _write_materials
from .watch import transcript_watcher

if TYPE_CHECKING:
    from .project import Project
    from .sessions import LiveHandle, SessionManager

log = logging.getLogger("atrium.todo_agent")

BUSY = ("starting", "running", "waiting")
INTERRUPTED = {"aborted_streaming", "aborted_tools", "interrupted"}  # turns the human stopped
CHECK_EVERY = 60  # seconds: a settings change, an agent whose process died, are seen within this
ARCHIVE_FIRST, ARCHIVE_EVERY = 120, 600  # after the start, then every
NAME_CHARS = 40  # of the TODO's text in its agent's name
MAX_CARD_CHARS = 6_000  # of one of the suggesting agent's cards, as text, in its context file
MAX_CONTEXT_CHARS = 40_000


def _short(s: str, n: int) -> str:
    s = " ".join((s or "").split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _last_result(h: "LiveHandle") -> Item | None:
    return next((it for it in reversed(h.items) if it.get("kind") == "result"), None)


class TodoAgent:
    """One per project: starts an agent for each of the human's TODOs, follows where it leaves it, archives it."""

    def __init__(self, project: "Project", sessions: "SessionManager"):
        self.project = project
        self.sessions = sessions
        self._starting: set[str] = set()  # TODOs whose agent is being started
        self._stopping: set[str] = set()  # agents stopped because their TODO was deleted
        self._loop: asyncio.AbstractEventLoop | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._timer_at = 0.0
        self._task: asyncio.Task[None] | None = None
        board = project.board
        stale = [t for t in board.todos() if (t.get("worker") or {}).get("state") in ("starting", "working")]
        for t in stale:  # their turn died with the Atrium process before this one
            board.set_todo_worker(t["id"], {**t["worker"], "state": "stopped", "error": "Atrium restarted during its turn", "at": time.time()}, quiet=True)
        if stale:
            board.rerender()
        board.on_todo = self.wake

    def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._background(), name=f"todo-agent-{self.project.id}")
        self.wake()

    def stop(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self._loop = None
        self.project.board.on_todo = None

    def enabled(self) -> bool:
        return self.project.board.todo_auto()

    def _set(self, tid: str, **fields: Any) -> None:
        """Change what a TODO's ``worker`` says, as it is now (a turn may have ended while we waited on something)."""
        t = next((t for t in self.project.board.todos() if t["id"] == tid), None)
        if t is not None:
            self.project.board.set_todo_worker(tid, {**(t.get("worker") or {}), **fields})

    # ---- when to look --------------------------------------------------------
    def wake(self, delay: float = 0.5) -> None:
        """Look again soon at what to start or stop: a TODO changed (``BoardStore.on_todo``), an agent's turn ended."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._schedule, delay)

    def _schedule(self, delay: float) -> None:
        delay = max(0.0, delay)
        at = time.monotonic() + delay
        if self._timer and self._timer_at <= at:
            return  # an earlier look is coming
        if self._timer:
            self._timer.cancel()
        self._timer_at = at
        self._timer = asyncio.get_running_loop().call_later(delay, self._fire)

    def _fire(self) -> None:
        self._timer = None
        asyncio.get_running_loop().create_task(self._check_safe())

    async def _background(self) -> None:
        last_archive = time.monotonic() - ARCHIVE_EVERY + ARCHIVE_FIRST
        while True:
            await asyncio.sleep(CHECK_EVERY)
            await self._check_safe()
            if time.monotonic() - last_archive >= ARCHIVE_EVERY:
                last_archive = time.monotonic()
                try:
                    await self.archive_idle()
                except Exception:
                    log.exception("archiving the idle TODO agents of %s failed", self.project.name)

    async def _check_safe(self) -> None:
        try:
            await self._check()
        except Exception:
            log.exception("TODO agents of %s: check failed", self.project.name)

    async def _check(self) -> None:
        todos = self.project.board.todos()
        await self._stop_orphans()
        self._reconcile(todos)
        if not self.enabled():
            return
        delay = float(config.get("todo_agent.delay_seconds") or 0)
        limit = int(config.get("todo_agent.max_concurrent") or 0)
        busy = self._busy(todos)
        now = time.time()
        since = self.project.board.todo_auto_since()
        for t in sorted((t for t in todos if due_for_agent(t, since) and t["id"] not in self._starting), key=in_list_since):
            due = in_list_since(t) + delay
            if due > now:
                self._schedule(due - now + 0.5)
                return
            if limit and busy >= limit:
                return  # an agent's turn ending frees its place (turn_ended looks again)
            busy += 1
            self._starting.add(t["id"])  # before the task runs: a look in between doesn't start it twice
            asyncio.get_running_loop().create_task(self._start_safe(t["id"]))

    def _busy(self, todos: list[dict[str, Any]]) -> int:
        """TODO agents at work: being started, or in the middle of a turn (also on a TODO that is done: review fixes)."""
        n = len(self._starting)
        for t in todos:
            sid = (t.get("worker") or {}).get("session")
            h = self.sessions.handle(sid) if sid and t["id"] not in self._starting else None
            if h and h.state in BUSY:
                n += 1
        return n

    async def _stop_orphans(self) -> None:
        """An agent whose TODO was deleted while it worked on it stops (the human can carry on with it in its chat)."""
        removed = self.project.board.removed_todos
        if not removed:
            return
        meta = None
        for sid, h in list(self.sessions.live.items()):
            if h.project.id != self.project.id or h.role != "worker" or not h.alive or h.state not in BUSY or sid in self._stopping:
                continue
            meta = self.project.sessions_meta() if meta is None else meta
            tid = (meta.get(sid, {}).get("todo") or {}).get("id")
            if not tid or tid not in removed:
                continue
            self._stopping.add(sid)
            log.info("TODO %s was deleted: stopping its agent %s", tid, sid)
            try:
                await self.sessions.interrupt(sid)
            except Exception:
                log.exception("stopping %s failed", sid)
            self.project.board.log(sid, f"Stopped: TODO {tid} was deleted", source="atrium")

    def _reconcile(self, todos: list[dict[str, Any]]) -> None:
        """An agent at work whose process ended without its turn ending (it crashed, it was closed) has stopped."""
        for t in todos:
            w = t.get("worker") or {}
            if w.get("state") != "working" or t.get("done") or t["id"] in self._starting:
                continue
            h = self.sessions.live.get(w.get("session") or "")
            if h is None or not h.alive:
                self.project.board.set_todo_worker(t["id"], {**w, "state": "stopped", "error": "its process ended before its turn did", "at": time.time()})

    # ---- an agent's turn -------------------------------------------------------
    def turn_ended(self, sid: str) -> None:
        """``SessionManager.on_turn_end``: where a TODO's agent leaves it is where its TODO stands — checked off, or
        waiting for the human (it said so, or it stopped without checking it off)."""
        h = self.sessions.live.get(sid)
        if not h or h.role != "worker":
            return
        tid = (self.project.session_meta(sid).get("todo") or {}).get("id")
        if not tid:
            return
        board = self.project.board
        t = next((t for t in board.todos() if t["id"] == tid), None)
        w = (t or {}).get("worker") or {}
        if t is None or w.get("session") != sid:
            self.wake()  # its TODO was deleted, or another agent has it now: its place is free all the same
            return
        res = _last_result(h)
        error = None
        if t.get("done"):
            state = "done"
        elif res and res.get("terminal_reason") in INTERRUPTED:
            state, error = "stopped", "you stopped it"
        elif res and res.get("is_error"):
            state, error = "stopped", _short(res.get("text") or "its turn failed", 200)
        elif h.reported == "done":  # it said it is done but didn't check the TODO off: it is done
            board.update_todo(tid, done=True, by=w.get("name") or h.name)
            board.log(sid, f"✓ {t['text']}", kind="todo", source="atrium")
            state = "done"
        else:
            state = "waiting"
        board.set_todo_worker(tid, {**w, "state": state, "at": time.time(), "error": error})
        self.wake()

    # ---- the human's actions ---------------------------------------------------
    async def run(self, tid: str) -> dict[str, Any]:
        """A TODO's ▶ or ↻: its agent carries on, or a new one starts on it now — whatever the switch and the limit say."""
        board = self.project.board
        t = next((t for t in board.todos() if t["id"] == tid), None)
        if t is None:
            raise KeyError(tid)
        if t.get("done"):
            raise ValueError("That TODO is done")
        if is_suggestion(t):
            raise ValueError("Add it to your TODOs first")
        if tid in self._starting:
            return {"state": "starting"}
        w = t.get("worker") or {}
        sid = w.get("session")
        if sid and self.project.session_meta(sid):
            h = self.sessions.handle(sid)
            if h and h.state in BUSY:
                return {"state": "working", "session": sid}
            cwd, _ = await self.sessions.session_info(self.project, sid)
            if os.path.isdir(cwd):  # (its worktree removed: a new agent does it)
                self._starting.add(tid)  # while it is woken up (seconds, for a long conversation), it is no process yet
                try:
                    text, attachments = await self._brief(t) if not w.get("briefed") else (_carry_on(tid, w.get("error")), [])
                    self._set(tid, state="working", at=time.time(), error=None)
                    board.update_agent(sid, status="working", doing=_short(t["text"], 80), source="atrium")
                    self._stopping.discard(sid)
                    await self.sessions.send(self.project, sid, text, attachments)
                except Exception as e:
                    self._set(tid, state="stopped", at=time.time(), error=_short(str(e) or type(e).__name__, 200))
                    raise
                finally:
                    self._starting.discard(tid)
                self._set(tid, briefed=True)
                return {"state": "working", "session": sid}
        self._starting.add(tid)
        return await self._start(tid)

    def forget(self, sid: str) -> None:
        """The agent was deleted: its TODO, if it is still open, is left without one (↻ starts a new agent)."""
        for t in self.project.board.todos():
            w = t.get("worker") or {}
            if w.get("session") == sid:
                self.project.board.set_todo_worker(t["id"], {**w, "session": None, "state": w.get("state") if t.get("done") else "deleted", "at": time.time()})
        self._stopping.discard(sid)
        self.wake()

    # ---- starting an agent -----------------------------------------------------
    async def _start_safe(self, tid: str) -> None:
        try:
            await self._start(tid)
        except Exception:
            log.exception("starting the agent of TODO %s failed", tid)

    async def _start(self, tid: str) -> dict[str, Any]:
        """A new agent for the TODO (the caller has put it in ``_starting``): in a worktree of its own in a git project,
        the TODO its first message."""
        project, board = self.project, self.project.board
        created: dict[str, Any] | None = None
        try:
            t = next((t for t in board.todos() if t["id"] == tid), None)
            if t is None or t.get("done") or is_suggestion(t) or (t.get("worker") or {}).get("state") in ("starting", "working"):
                return {"skipped": "it has an agent, or isn't open any more"}
            board.set_todo_worker(tid, {"state": "starting", "at": time.time()})
            todo, cwd, created = await self._workspace(tid)
            name = f"TODO {tid} · {_short(t['text'], NAME_CHARS)}"
            text, attachments = await self._brief(t)
            sid = await self.sessions.create(project, name=name, prompt=None, cwd=cwd, harness_id=config.get("todo_agent.harness") or None,
                                             model=config.get("todo_agent.model") or None, effort=config.get("todo_agent.effort") or None,
                                             todo=todo)
            created = None  # its agent's worktree from now on
            worker = {"session": sid, "name": name, "state": "working", "at": time.time(), "branch": todo.get("branch")}
            board.set_todo_worker(tid, worker)
            board.update_agent(sid, status="working", progress=0, doing=_short(t["text"], 80), source="atrium")
            board.log(sid, f"Started on TODO {tid}" + (f" in worktree {todo['branch']}" if todo.get("branch") else ""), source="atrium")
            try:
                await self.sessions.send(project, sid, text, attachments)
            except Exception as e:  # the agent is there: ↻ sends it the TODO again
                self._set(tid, state="stopped", at=time.time(), error=_short(str(e) or type(e).__name__, 200))
                raise
            self._set(tid, briefed=True)
            return {"state": "working", "session": sid}
        except Exception as e:
            if created:  # nothing refers to it
                await gitops.run_git(project.path, "worktree", "remove", "--force", created["path"], check=False)
                await gitops.run_git(project.path, "branch", "-D", created["branch"], check=False)
            cur = next((x for x in board.todos() if x["id"] == tid), None)
            if cur and (cur.get("worker") or {}).get("state") == "starting":
                board.set_todo_worker(tid, {"state": "failed", "at": time.time(), "error": _short(str(e) or type(e).__name__, 200)})
            raise
        finally:
            self._starting.discard(tid)
            self.wake()

    async def _workspace(self, tid: str) -> tuple[dict[str, Any], str, dict[str, Any] | None]:
        """Where the agent works: a new worktree off what the project directory has checked out (in a git project with a
        commit), else the project directory. Returns its ``todo`` meta, its directory, and the worktree made for it."""
        path = self.project.path
        todo: dict[str, Any] = {"id": tid}
        root = gitops.repo_root(path)
        if not root or not await gitops.git_ok(root, "rev-parse", "--verify", "--quiet", "HEAD"):
            return todo, path, None
        root = os.path.realpath(root)
        base = (await gitops.run_git(root, "rev-parse", "--abbrev-ref", "HEAD", check=False)).strip()
        base = None if base in ("", "HEAD") else base  # a detached HEAD: nothing to merge into
        for n in range(1, 100):
            branch = f"todo/{tid}" + (f"-{n}" if n > 1 else "")
            dest = os.path.join(root, ".atrium", "worktrees", branch.replace("/", "-"))
            if not os.path.exists(dest) and not await gitops.git_ok(root, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"):
                break
        else:
            raise RuntimeError(f"no free branch name for TODO {tid}")
        created = await gitops.add_worktree(root, branch, dest=dest)
        transcript_watcher.add_project(self.project)  # its transcripts go to a directory of their own
        hub.publish(f"project:{self.project.id}", {"type": "git_changed"})
        sub = os.path.join(created["path"], os.path.relpath(os.path.realpath(path), root))  # a project in a subdirectory: the same one
        cwd = os.path.realpath(sub if os.path.isdir(sub) else created["path"])
        todo.update(branch=branch, base=base, root=root, merge=bool(config.get("todo_agent.merge")) and bool(base))
        return todo, cwd, created

    async def _brief(self, t: dict[str, Any]) -> tuple[str, list[Attachment]]:
        """The agent's first message: the TODO, where it came from, and the context of the agent that suggested it."""
        att = await self._context(t)
        lines = [f"[TODO {t['id']} · from the board] {t['text']}"]
        if t.get("why"):
            lines.append(f"Why: {t['why']}")
        who = t.get("agent") or "an agent"
        attached = " The attached file has what it was doing then." if att else ""
        if t.get("source") == "agent":
            lines.append(f"Suggested by {who}, and accepted by the human.{attached}")
        elif t.get("session_id"):
            lines.append(f"The human's own, noted down by {who}.{attached}")
        else:
            lines.append("Added by the human.")
        return "\n\n".join(lines), [att] if att else []

    async def _context(self, t: dict[str, Any]) -> Attachment | None:
        """A file with what the agent that put the TODO on the board was doing then: what it had been asked, what it
        replied, its status and cards. None when there is no such agent, or nothing is known of it."""
        sid = t.get("session_id")
        if not sid:
            return None
        board = self.project.board.agent(sid) or {}
        name = t.get("agent") or board.get("name") or "the agent"
        cwd = hid = None
        items: list[Item] = []
        try:
            if self.project.session_meta(sid) or await self.sessions.find_summary(self.project, sid):  # (deleted since: its card is left)
                cwd, hid = await self.sessions.session_info(self.project, sid)
                h = self.sessions.handle(sid)
                items = list(h.items) if h else await get_harness(hid).load_history(sid, cwd)
        except Exception:
            log.debug("history of %s unavailable", sid, exc_info=True)
        at = t.get("created_at") or time.time()
        then = [it for it in items if (it.get("ts") or 0) <= at + 5] or items  # what it had done when it wrote the TODO
        asks = [it.get("text") or "" for it in then if it.get("kind") == "user" and not it.get("command") and (it.get("text") or "").strip()][-3:]
        # its reply in that turn: the last of its messages up to a while after
        reply = next((it.get("text") or "" for it in reversed([it for it in items if (it.get("ts") or 0) <= at + 900] or items)
                      if it.get("kind") == "text" and not it.get("parent") and (it.get("text") or "").strip()), "")
        cards = sorted((board.get("cards") or {}).values(), key=lambda c: -(c.get("updated_at") or 0))
        if not (asks or reply or cards or board.get("summary")):
            return None
        stats = board.get("stats") or {}
        out = [f"# Where TODO {t['id']} came from", ""]
        where = f" It works in `{cwd}` ({hid})" if cwd else ""
        if stats.get("branch"):
            where += f"{',' if where else ' It worked'} on branch {stats['branch']}" + (" (its worktree was removed since)" if stats.get("workspace_gone") else "")
        out.append(f"{name} {'suggested it' if t.get('source') == 'agent' else 'noted it down for the human'} "
                   f"on {time.strftime('%Y-%m-%d %H:%M', time.localtime(at))}.{where}{'.' if where else ''}")
        out += ["", "## The TODO", "", t["text"]] + ([f"\nWhy: {t['why']}"] if t.get("why") else [])
        if board.get("doing") or board.get("summary"):
            out += ["", "## Its status on the board (now)", "", f"{board.get('doing') or '—'}" + (f" — {board['summary']}" if board.get("summary") else "")]
        if asks:
            out += ["", "## What it had been asked (its latest prompts then, oldest first)", ""]
            out += [f"> {_clip(a, 3000)}".replace("\n", "\n> ") + "\n" for a in asks]
        if reply:
            out += ["", "## What it said then", "", _clip(reply, 6000)]
        budget = MAX_CONTEXT_CHARS - sum(len(x) + 1 for x in out)
        if cards:
            out += ["", "## Its cards on the board (now, newest first)"]
            for c in cards:
                body = _clip(_text_of(c.get("html") or ""), MAX_CARD_CHARS)
                if budget < 200:
                    out.append(f"\n… and {len(cards) - cards.index(c)} more")
                    break
                body = _clip(body, budget)
                budget -= len(body) + 80
                out += ["", f"### {c.get('title') or 'Card'}", "", body]
        return _write_materials("\n".join(out) + "\n", f"todo-{t['id']}-context.md")

    # ---- tidying up ------------------------------------------------------------
    async def archive_idle(self) -> list[str]:
        """Archive the TODO agents idle for ``todo_agent.archive_after_days``; a merged worktree goes with each."""
        days = float(config.get("todo_agent.archive_after_days") or 0)
        if days <= 0:
            return []
        mine = {sid for sid, m in self.project.sessions_meta().items() if m.get("todo") and not m.get("archived")}
        if not mine:
            return []
        cutoff = time.time() - days * 86400
        done = []
        for r in await self.sessions.list_sessions(self.project, details=False):
            if r["id"] not in mine or r["live"] in BUSY or r["pending"] or (r["updated_at"] or 0) > cutoff:
                continue
            try:
                res = await self.sessions.set_archived(self.project, r["id"], True)
            except Exception:
                log.exception("archiving the TODO agent %s failed", r["id"])
                continue
            done.append(r["id"])
            gone = " and its worktree removed" if res and res.get("removed") else ""
            self.project.board.log(r["id"], f"Archived{gone}: idle for {days:g} day{'' if days == 1 else 's'}", source="atrium")
        if done:
            self.sessions.invalidate(self.project)
            hub.publish(f"project:{self.project.id}", {"type": "sessions_changed"})
        return done


def todo_note(todo: dict[str, Any], cwd: str) -> str:
    """System-prompt note for the agent of a TODO (its ``todo`` session meta, see ``TodoAgent._workspace``)."""
    branch, base, root = todo.get("branch"), todo.get("base"), todo.get("root")
    here = (gitops._run_sync(cwd, "rev-parse", "--abbrev-ref", "HEAD") or "").strip() if branch else ""
    if branch and root and here == branch:
        off = f"{base} of the project directory {root}" if base else f"the commit the project directory {root} had checked out"
        where = TODO_WHERE_WORKTREE.format(cwd=cwd, branch=branch, off=off, root=root)
        if todo.get("merge") and base:
            finish = TODO_FINISH_MERGE.format(branch=branch, base=base, root=root)
            again = f", commit, and bring them into {base} the same way"
        else:
            finish, again = TODO_FINISH_KEEP.format(branch=branch), ", and commit them"
    else:  # no git, or it was moved off its worktree since
        where, finish, again = TODO_WHERE_DIR.format(cwd=cwd), "", ""
    return TODO_AGENT_APPEND.format(tid=todo.get("id"), where=where, finish=finish, again=again)


def _carry_on(tid: str, why: str | None) -> str:
    return f"[TODO {tid}] Carry on with this TODO where you left off." + (f" (Your last turn ended early: {why}.)" if why else "")


def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def _text_of(fragment: str) -> str:
    """A card's HTML as text: what an agent reads of it."""
    s = re.sub(r"(?is)<(script|style)\b.*?</\1\s*>", " ", fragment)
    s = re.sub(r"(?i)<br\s*/?>|</(p|div|li|tr|h[1-6]|pre|table|ul|ol|summary|details|blockquote)\s*>", "\n", s)
    s = re.sub(r"(?i)</t[dh]\s*>", " | ", s)
    s = html.unescape(re.sub(r"<[^>]+>", "", s))
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    return re.sub(r"\n\s*\n+", "\n", s).strip()
