"""In-process pub/sub hub used to push events to browser websockets.

Topics:
    ``project:<pid>``   board / tracker / session-list / fs / git-view events
    ``session:<sid>``   normalized chat items streamed from a live session
    ``open:<pid>``      no events: a page subscribes while it has the project open (a chat isn't discarded under it)

A page subscribes to a session's topic quietly while it keeps the chat without showing it (one opened lately): it is
sent the chat's items, but nobody is looking at them (``watching``).
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any


class Subscriber:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=5000)
        self.topics: set[str] = set()
        self.quiet: set[str] = set()  # of them, those it keeps up with without showing them
        self.active: bool = False  # the browser page is visible and focused (reported over the socket)
        self.page: str | None = None  # which page it is (one socket per page, a new one when it reconnects)


class Hub:
    def __init__(self) -> None:
        self._subs: dict[str, set[Subscriber]] = defaultdict(set)
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self, sub: Subscriber, topic: str, quiet: bool = False) -> None:
        """Also to make a subscription quiet, or not any more."""
        sub.topics.add(topic)
        (sub.quiet.add if quiet else sub.quiet.discard)(topic)
        self._subs[topic].add(sub)

    def unsubscribe(self, sub: Subscriber, topic: str) -> None:
        sub.topics.discard(topic)
        sub.quiet.discard(topic)
        self._subs[topic].discard(sub)

    def drop(self, sub: Subscriber) -> None:
        for t in list(sub.topics):
            self.unsubscribe(sub, t)

    def pages(self, topic: str) -> set[str | None]:
        """The pages subscribed to ``topic``."""
        return {sub.page for sub in self._subs.get(topic, ())}

    def watching(self, topic: str) -> bool:
        """Whether someone is looking at ``topic``: an active page is subscribed to it, and not quietly."""
        return any(sub.active and topic not in sub.quiet for sub in self._subs.get(topic, ()))

    def publish(self, topic: str, event: dict[str, Any]) -> None:
        """Publish from the event loop thread."""
        msg = {"topic": topic, "ts": time.time(), **event}
        for sub in list(self._subs.get(topic, ())):
            try:
                sub.queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass  # slow consumer; it will resync on reconnect

    def publish_threadsafe(self, topic: str, event: dict[str, Any]) -> None:
        """Publish from a worker thread (e.g. the file watcher)."""
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self.publish, topic, event)


hub = Hub()
