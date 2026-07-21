"""
Donor node registry + job broker.

A "donor node" is a remote worker running on someone else's PC that donates its
Ubisoft account's generating capacity for a specific game. Their credentials never
leave their machine — the node connects OUTBOUND to this backend (so it works
behind home NAT with no port-forwarding), long-polls for jobs scoped to it, runs
DenuvoTicket locally, and returns only the finished tokens.

This registry is the backend half:
  - tracks which nodes are currently connected (online = polled within a TTL),
  - authenticates each node by a per-node shared key (never the master api_key,
    so a donor can only serve jobs — not drain the pool),
  - brokers one job at a time to a node and blocks the job-queue worker until the
    node returns a result (or a timeout).

Online/offline drives stock: while a node is connected its game shows in-stock;
the moment it stops polling, the game goes out-of-stock (see JobQueue).
"""

import logging
import secrets
import threading
import time
from collections import deque
from typing import Optional

logger = logging.getLogger("ubitokeer")


class NodeRegistry:
    def __init__(self, nodes: Optional[dict] = None, online_ttl: float = 45.0):
        # nodes: {node_id: {"key": "<secret>"}} — auth secrets, from config.json.
        self._keys: dict[str, str] = {
            nid: str((cfg or {}).get("key", ""))
            for nid, cfg in (nodes or {}).items()
        }
        self._online_ttl = online_ttl

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._last_seen: dict[str, float] = {}
        # Jobs waiting to be claimed by each node: node_id -> deque[job dict].
        self._queues: dict[str, deque] = {}
        # In-flight results: job_id -> {"event", "result", "error", "node_id"}.
        self._results: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Auth + presence
    # ------------------------------------------------------------------
    def is_known(self, node_id: str) -> bool:
        return node_id in self._keys

    def authenticate(self, node_id: str, key: str) -> bool:
        expected = self._keys.get(node_id)
        if not expected:
            return False
        return secrets.compare_digest(str(key or ""), expected)

    def _touch_locked(self, node_id: str) -> None:
        self._last_seen[node_id] = time.time()

    def is_online(self, node_id: str) -> bool:
        if not node_id:
            return False
        with self._lock:
            return (time.time() - self._last_seen.get(node_id, 0.0)) <= self._online_ttl

    def online_nodes(self) -> list[str]:
        now = time.time()
        with self._lock:
            return [nid for nid, ls in self._last_seen.items()
                    if now - ls <= self._online_ttl]

    # ------------------------------------------------------------------
    # Producer side — called from the JobQueue worker thread
    # ------------------------------------------------------------------
    def dispatch_and_wait(self, node_id: str, job_id: str, uplay_id: str,
                          token_req: str, timeout: float) -> dict:
        """Hand a job to a node and block until it returns a result.

        Returns the node's result dict {denuvo_token, ownership_token, dlc_ids,
        console_output}. Raises TimeoutError if the node never answers (e.g. it
        went offline mid-job) or RuntimeError if the node reported a failure."""
        event = threading.Event()
        with self._cv:
            self._results[job_id] = {"event": event, "result": None,
                                     "error": None, "node_id": node_id}
            self._queues.setdefault(node_id, deque()).append({
                "job_id": job_id, "uplay_id": uplay_id, "token_req": token_req,
            })
            self._cv.notify_all()

        got = event.wait(timeout)

        with self._lock:
            entry = self._results.pop(job_id, None)
            # If it was never claimed (node offline), drop it from the queue so a
            # node coming back later doesn't pick up a job nobody is waiting on.
            q = self._queues.get(node_id)
            if q:
                for item in list(q):
                    if item["job_id"] == job_id:
                        try:
                            q.remove(item)
                        except ValueError:
                            pass

        if not got:
            raise TimeoutError("donor node did not return a result in time")
        if entry and entry["error"]:
            raise RuntimeError(entry["error"])
        if not entry or entry["result"] is None:
            raise RuntimeError("donor node returned no result")
        return entry["result"]

    # ------------------------------------------------------------------
    # Consumer side — called from the node's long-poll / result endpoints
    # ------------------------------------------------------------------
    def poll(self, node_id: str, key: str, wait: float = 25.0) -> Optional[dict]:
        """Long-poll for the next job for this node. Blocks up to `wait` seconds.

        Every poll also refreshes the node's presence, so a node that simply keeps
        polling stays "online" and its game stays in-stock. Returns a job dict
        {job_id, uplay_id, token_req} or None when the wait elapses with no work."""
        if not self.authenticate(node_id, key):
            raise PermissionError("bad node credentials")

        deadline = time.time() + max(0.0, wait)
        with self._cv:
            self._touch_locked(node_id)
            while True:
                q = self._queues.get(node_id)
                if q:
                    return q.popleft()
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                # Cap each wait so presence keeps refreshing even while idle.
                self._cv.wait(timeout=min(remaining, 5.0))
                self._touch_locked(node_id)

    def submit_result(self, node_id: str, key: str, job_id: str,
                      result: Optional[dict] = None, error: Optional[str] = None) -> bool:
        """Deliver a node's generation result (or error) back to the waiting job.
        Returns False if no job by that id is in flight (already timed out)."""
        if not self.authenticate(node_id, key):
            raise PermissionError("bad node credentials")
        with self._lock:
            self._touch_locked(node_id)
            entry = self._results.get(job_id)
            if not entry:
                return False
            # Ignore a result meant for a different node's job.
            if entry.get("node_id") and entry["node_id"] != node_id:
                return False
            entry["result"] = result
            entry["error"] = error
            entry["event"].set()
        return True
