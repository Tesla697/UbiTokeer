import logging
import threading
import time
from collections import deque
from datetime import datetime
from typing import Callable, Optional

from core.accounts import get_accounts_for_uplay_id, has_any_account_for_uplay_id
from core.cli_worker import CliWorker
from core.job import Job, JobStatus
from core.login_keepalive import LoginKeepAlive
from core.node_registry import NodeRegistry
from core.quota import QuotaExceededError, QuotaTracker

logger = logging.getLogger("ubitokeer")

# Map uplay_id to output format. Games not listed default to "token_ini".
OUTPUT_FORMATS = {}


def load_output_formats(formats: dict) -> None:
    """Load output format overrides, e.g. {"4740": "dbdata"}."""
    OUTPUT_FORMATS.update(formats)


class BusyError(Exception):
    pass


def _is_activation_limit(err) -> bool:
    """True when a generation failure means the account's REAL Ubisoft activation
    limit is hit (as opposed to a transient/network error)."""
    s = str(err).lower()
    return (
        "exceed" in s
        or "activation limit" in s
        or "exceededactivations" in s
        or "daily" in s and "limit" in s
    )


class JobQueue:
    def __init__(self, config: dict, on_update: Optional[Callable] = None,
                 nodes: Optional[NodeRegistry] = None):
        self._config = config
        self._on_update = on_update
        # Registry of remote donor nodes (games served from someone else's PC).
        # None = no donor support; every game is served by local accounts.
        self._nodes = nodes
        # How long to wait for a donor node to return a generated token before the
        # job is failed. Covers the node's own process time plus network latency.
        self._node_job_timeout = config.get("node_job_timeout", 180)
        # RLock, not Lock: several helpers below are reachable both directly and
        # from code that already holds this lock, and a plain Lock re-acquired by
        # the same thread deadlocks it permanently — taking the whole queue
        # (reserve/activate/cancel) down with it until the process is restarted.
        self._lock = threading.RLock()
        self._current: Optional[Job] = None
        # FIFO of jobs waiting for the worker. A deque (instead of a single slot)
        # so an activated reservation is never bounced with "queue full" just
        # because one other job is mid-flight — reservations already cap real
        # concurrency by quota, the worker just drains this in order.
        self._pending: deque[Job] = deque()
        self._jobs: dict[str, Job] = {}
        self._condition = threading.Condition(self._lock)
        self._quota = QuotaTracker(daily_limit=config.get("daily_limit", 5))
        self._worker = CliWorker(
            process_timeout=config.get("process_timeout", 90),
        )
        # How long a RESERVED slot may sit before the sweeper reclaims it. Matches
        # the bot's 30-min ticket lifetime so a user's slot stays held for the whole
        # window they have to upload; the bot also cancels explicitly on close.
        self._reservation_ttl = config.get("reservation_ttl", 1800)
        self._max_queue = config.get("max_queue", 50)
        self._last_sweep = 0.0
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        # Keeps idle accounts' LoginStore.dat sessions alive (and flags dead ones
        # early) so an account nobody used for days doesn't fail a real ticket.
        # Only runs while the queue is idle, and never spends quota.
        self._keepalive = LoginKeepAlive(
            worker=self._worker,
            is_busy=lambda: bool(self._current or self._pending),
            interval_seconds=config.get("login_refresh_interval_seconds", 3600),
            stale_seconds=config.get("login_refresh_stale_seconds", 3 * 86400),
            enabled=config.get("login_refresh_enabled", True),
            fail_backoff_seconds=config.get("login_refresh_fail_backoff_seconds", 3600),
            fail_backoff_max_seconds=config.get("login_refresh_fail_backoff_max_seconds", 86400),
        )
        self._keepalive.start()
        logger.info("Job queue started")

    # ------------------------------------------------------------------
    # Account availability (local accounts are always available; a remote
    # donor account is only available while its node is connected)
    # ------------------------------------------------------------------
    def _account_available(self, acc: dict) -> bool:
        if acc.get("remote"):
            return bool(self._nodes and self._nodes.is_online(acc.get("node_id", "")))
        return True

    def _available_accounts(self, uplay_id: str) -> list[dict]:
        """Accounts assigned to uplay_id that can actually serve right now — i.e.
        with any offline donor nodes filtered out. An empty list means the game is
        out of stock (donor offline), so reserve/submit refuse it and the bot sees
        remaining=0."""
        return [a for a in get_accounts_for_uplay_id(uplay_id) if self._account_available(a)]

    # ------------------------------------------------------------------
    # Reservation lifecycle (the admission gate)
    # ------------------------------------------------------------------
    def reserve(self, uplay_id: str) -> Job:
        """Hold a quota slot for uplay_id WITHOUT generating anything yet.

        Raises ValueError if the game isn't configured, or QuotaExceededError if
        every account for it is already used-up or reserved (the caller maps that
        to HTTP 429 so the ticket is refused before any thread opens)."""
        if not has_any_account_for_uplay_id(uplay_id):
            raise ValueError(f"No account assigned to uplay_id={uplay_id}")

        # Offline donor nodes are filtered out here, so a donor-only game whose node
        # is disconnected has no available account → QuotaExceededError → the bot
        # refuses the ticket (out of stock) rather than opening one it can't fulfil.
        accounts = self._available_accounts(uplay_id)
        job = Job(uplay_id=uplay_id, account_email="", accid="", folder="", token_req="")
        job.status = JobStatus.RESERVED

        chosen_email = self._quota.try_reserve(job.id, accounts, uplay_id)
        if not chosen_email:
            raise QuotaExceededError(
                f"All accounts assigned to uplay_id={uplay_id} are at their daily limit"
            )

        acc = next((a for a in accounts if a["email"] == chosen_email), None)
        if acc is None:  # extremely unlikely race: account vanished mid-reserve
            self._quota.release(job.id)
            raise QuotaExceededError(f"No account available for uplay_id={uplay_id}")
        job.account_email = acc["email"]
        job.accid = acc["accid"]
        job.folder = acc["folder"]
        job.node_id = acc.get("node_id") if acc.get("remote") else None

        with self._lock:
            self._jobs[job.id] = job

        self._notify_update()
        logger.info(f"Job {job.id} RESERVED: uplay_id={uplay_id}, account={acc['email']}")
        return job

    def activate(self, job_id: str, token_req: str) -> Job:
        """Promote a RESERVED job to QUEUED, supplying the token request file.

        Idempotent for jobs already queued/processing/done. Raises ValueError for
        an unknown/expired job, BusyError if the queue is full."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise ValueError(f"Unknown job {job_id}")
            if job.status == JobStatus.RESERVED:
                if len(self._pending) >= self._max_queue:
                    raise BusyError("Queue is full. Try again later.")
                job.token_req = token_req
                job.status = JobStatus.QUEUED
                self._pending.append(job)
                self._condition.notify_all()
                logger.info(f"Job {job_id} ACTIVATED -> queued (uplay_id={job.uplay_id})")
            elif job.status in (JobStatus.QUEUED, JobStatus.PROCESSING, JobStatus.DONE):
                pass  # already moving/finished — treat as success
            else:  # FAILED (e.g. reservation already swept/cancelled)
                raise ValueError(f"Job {job_id} is not activatable (status={job.status.value})")

        self._notify_update()
        return job

    def cancel(self, job_id: str, reason: str = "") -> bool:
        """Release a RESERVED/QUEUED job's slot. No-op (returns False) once the
        job is processing or done, so we never release a slot mid-generation."""
        released = False
        with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status in (JobStatus.RESERVED, JobStatus.QUEUED):
                try:
                    self._pending.remove(job)
                except ValueError:
                    pass
                job.status = JobStatus.FAILED
                job.error = f"cancelled: {reason}" if reason else "cancelled"
                self._quota.release(job_id)
                released = True
        if released:
            logger.info(f"Job {job_id} cancelled ({reason or 'no reason'}); slot released")
            self._notify_update()
        return released

    # ------------------------------------------------------------------
    # Direct submit (legacy path: reserve + activate in one shot)
    # ------------------------------------------------------------------
    def submit(self, uplay_id: str, token_req: str) -> Job:
        if not has_any_account_for_uplay_id(uplay_id):
            raise ValueError(f"No account assigned to uplay_id={uplay_id}")

        # Account selection is reservation-aware (get_remaining subtracts held
        # slots) AND availability-aware (offline donor nodes are skipped), so a
        # direct submit won't grab a held slot or route to a disconnected donor.
        account = next(
            (a for a in self._available_accounts(uplay_id)
             if not a.get("track_quota", True) or self._quota.can_generate(a["email"], uplay_id)),
            None,
        )
        if not account:
            raise QuotaExceededError(
                f"Daily token limit reached for all accounts assigned to uplay_id={uplay_id}"
            )

        with self._lock:
            if len(self._pending) >= self._max_queue:
                raise BusyError("Queue is full. Try again later.")

            job = Job(
                uplay_id=uplay_id,
                account_email=account["email"],
                accid=account["accid"],
                folder=account["folder"],
                token_req=token_req,
            )
            job.node_id = account.get("node_id") if account.get("remote") else None
            self._jobs[job.id] = job
            self._pending.append(job)
            self._condition.notify_all()

        self._notify_update()
        logger.info(f"Job {job.id} submitted: uplay_id={uplay_id}, account={account['email']}")
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_state(self) -> dict:
        with self._lock:
            return {
                "current": self._current.to_dict() if self._current else None,
                "pending": self._pending[0].to_dict() if self._pending else None,
                "pending_count": len(self._pending),
            }

    def _worker_loop(self) -> None:
        while self._running:
            job = None
            with self._condition:
                while not self._pending and self._running:
                    self._condition.wait(timeout=1.0)
                    # NOTE: the sweep runs OUTSIDE this block (below). It used to be
                    # called here, while holding the condition's lock, and then took
                    # that same lock again to fail expired jobs — which deadlocked the
                    # worker against itself the first time a reservation went stale,
                    # and with it every reserve/activate/cancel.
                    if self._sweep_due():
                        break

                if not self._running:
                    break

                if self._pending:
                    job = self._pending.popleft()
                    self._current = job

            if job is None:
                # Woke up to sweep, not to work: do it with NO lock held.
                self._maybe_sweep()
                continue

            self._notify_update()
            self._process_job(job)

            with self._lock:
                self._current = None

            self._notify_update()

    def _sweep_due(self) -> bool:
        """True when the throttled sweep window has elapsed. Cheap + lock-free so it
        can be checked from inside the condition wait."""
        return (time.time() - self._last_sweep) >= 30

    def _maybe_sweep(self) -> None:
        """Throttled reservation sweep (~every 30s). MUST be called with no lock
        held — it acquires self._lock to fail expired jobs."""
        now = time.time()
        if now - self._last_sweep < 30:
            return
        self._last_sweep = now
        try:
            stale = self._quota.sweep(self._reservation_ttl)
            if stale:
                with self._lock:
                    for jid in stale:
                        job = self._jobs.get(jid)
                        if job and job.status == JobStatus.RESERVED:
                            job.status = JobStatus.FAILED
                            job.error = "reservation expired"
        except Exception as e:
            logger.error(f"Reservation sweep failed: {e}")

    def _generate_on(self, acc: dict, job: Job) -> dict:
        """Run one generation attempt for `job` on `acc`. A remote (donor) account
        dispatches to its node; a local account runs the CLI here. Both return the
        same {denuvo_token, ownership_token, dlc_ids, console_output} shape."""
        if acc.get("remote"):
            return self._nodes.dispatch_and_wait(
                acc["node_id"], job.id, job.uplay_id, job.token_req, self._node_job_timeout
            )
        return self._worker.generate(
            folder=acc["folder"], accid=acc["accid"],
            uplay_id=job.uplay_id, token_req=job.token_req,
        )

    def _process_job(self, job: Job) -> None:
        logger.info(f"Processing job {job.id}: uplay_id={job.uplay_id}, account={job.account_email}")
        job.status = JobStatus.PROCESSING
        self._notify_update()

        # One unified fallback loop over ALL available accounts for this game —
        # local accounts and remote donor nodes mixed together. The account reserved
        # for this job is tried first, then the rest as fallbacks, so "one account
        # runs out / fails → shift to the next" works identically whether the next
        # account is on our backend or a donor's PC. Offline donor nodes are already
        # filtered out (an offline node is never a fallback target).
        available = self._available_accounts(job.uplay_id)
        primary = next((a for a in available if a["email"] == job.account_email), None)
        ordered = ([primary] if primary else []) + \
                  [a for a in available if a["email"] != job.account_email]

        if not ordered:
            job.status = JobStatus.FAILED
            job.error = "no account available (donor offline?)"
            job.finished_at = datetime.utcnow()
            self._quota.release(job.id)
            logger.error(f"Job {job.id}: no available account for uplay_id={job.uplay_id}")
            self._notify_update()
            return

        last_error = None
        for attempt, acc in enumerate(ordered):
            # Skip a tracked fallback ONLY when it is genuinely USED UP (real
            # recorded usage >= limit). We deliberately use has_real_capacity, NOT
            # can_generate: can_generate also subtracts reservation holds, so with
            # several tickets queued (each holding a slot) it would skip a fallback
            # that still has real unused tokens — making this job give up with "all
            # accounts reached" while free tokens exist. Reservations gate ticket
            # OPENING, not generation; the single worker serialises real usage so
            # this can't oversell past the real limit. The reserved primary
            # (attempt 0) is always tried; untracked/donor accounts are unlimited.
            if attempt > 0 and acc.get("track_quota", True) \
                    and not self._quota.has_real_capacity(acc["email"], job.uplay_id):
                continue

            is_remote = bool(acc.get("remote"))
            job.account_email = acc["email"]
            job.accid = acc.get("accid", "")
            job.folder = acc.get("folder", "")
            job.node_id = acc.get("node_id") if is_remote else None
            if attempt > 0:
                logger.info(f"Job {job.id}: falling back to "
                            f"{'donor ' if is_remote else ''}account {acc['email']}...")

            try:
                result = self._generate_on(acc, job)

                job.denuvo_token = result["denuvo_token"]
                job.ownership_token = result["ownership_token"]
                job.dlc_ids = result.get("dlc_ids")
                job.console_output = result.get("console_output", "")

                # Build formatted output based on game's output format
                output_format = OUTPUT_FORMATS.get(job.uplay_id, "token_ini")
                if output_format == "dbdata":
                    job.dbdata_json = CliWorker.build_dbdata_json(
                        job.denuvo_token, job.ownership_token, job.dlc_ids
                    )
                else:
                    job.token_ini = CliWorker.build_token_ini(
                        job.denuvo_token, job.ownership_token
                    )

                job.status = JobStatus.DONE
                job.finished_at = datetime.utcnow()
                if acc.get("track_quota", True):
                    self._quota.record(acc["email"], job.uplay_id)
                # The reservation has now become a real recorded use — drop the hold
                # so we don't double-count the slot.
                self._quota.release(job.id)
                # A real local generation just proved this login works — no need for
                # the keep-alive to re-launch a CLI for it. (Donor nodes manage their
                # own login, so there's nothing to note here.)
                if not is_remote:
                    self._keepalive.note_used(acc["email"])
                logger.info(f"Job {job.id} completed successfully via "
                            f"{'donor node ' + acc['node_id'] if is_remote else 'account ' + acc['email']} "
                            f"(format={output_format})")
                self._notify_update()
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Job {job.id}: {'donor ' if is_remote else ''}"
                               f"account {acc['email']} failed: {e}")
                # A dead stored session shows up here first — flag it immediately so
                # the health view says "needs re-login" instead of us finding out
                # from the next user whose ticket dies. (Local accounts only — the
                # donor's session lives on their PC.)
                if not is_remote and "authentication failed" in str(e).lower():
                    self._keepalive.note_auth_failed(acc["email"])
                # If Ubisoft says this account's real activation limit is hit, our
                # internal count was wrong (phantom token). Force it to exhausted so
                # it isn't offered to the next user this window.
                if _is_activation_limit(e) and acc.get("track_quota", True):
                    self._quota.exhaust(acc["email"], job.uplay_id)
                continue

        # All accounts failed
        job.status = JobStatus.FAILED
        job.error = str(last_error)
        job.finished_at = datetime.utcnow()
        # Nothing was generated — release the held slot back to the pool.
        self._quota.release(job.id)
        logger.error(f"Job {job.id} failed on all accounts: {last_error}")
        self._notify_update()

    def get_quota_simple(self, uplay_id: str) -> dict:
        # Only count accounts that can serve right now, so a game whose donor node
        # is offline correctly reports remaining=0 (out of stock) to the bot.
        accounts = self._available_accounts(uplay_id)
        return self._quota.get_simple(uplay_id, accounts)

    def get_quota_summary(self) -> dict:
        from core.accounts import read_accounts
        return self._quota.get_summary(read_accounts())

    def get_reservations(self) -> dict:
        """Live reservation snapshot: {'total': N, 'by_uplay': {uplay_id: count}}."""
        return self._quota.reservations_snapshot()

    def get_login_health(self) -> dict:
        """Per-account LoginStore.dat session health (which need a manual re-login)."""
        return self._keepalive.get_health()

    def refresh_logins(self, force: bool = False) -> dict:
        """Refresh stale account sessions now (force=True does every account)."""
        return self._keepalive.refresh_all(force=force)

    def reconcile_reservations(self, active_job_ids, grace_seconds: float = 60.0) -> dict:
        """Release holds with no matching open ticket (the bot supplies the live
        job_ids). Orphaned RESERVED jobs are also marked FAILED so a late
        activate can't resurrect a slot we just handed back to the pool."""
        released = self._quota.reconcile(active_job_ids, grace_seconds)
        if released:
            with self._lock:
                for jid in released:
                    job = self._jobs.get(jid)
                    if job and job.status == JobStatus.RESERVED:
                        job.status = JobStatus.FAILED
                        job.error = "reservation reconciled (ticket no longer open)"
            self._notify_update()
        return {"released": len(released), "job_ids": released}

    def _notify_update(self) -> None:
        if self._on_update:
            try:
                self._on_update()
            except Exception:
                pass

    def update_config(self, config: dict) -> None:
        self._config = config

    def shutdown(self) -> None:
        self._running = False
        try:
            self._keepalive.stop()
        except Exception:
            pass
        with self._condition:
            self._condition.notify_all()
        logger.info("Job queue shut down")
