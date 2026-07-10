import logging
import threading
import time
from collections import deque
from datetime import datetime
from typing import Callable, Optional

from core.accounts import get_account_for_uplay_id, get_accounts_for_uplay_id, has_any_account_for_uplay_id
from core.cli_worker import CliWorker
from core.job import Job, JobStatus
from core.quota import QuotaExceededError, QuotaTracker

logger = logging.getLogger("ubitokeer")

# Map uplay_id to output format. Games not listed default to "token_ini".
OUTPUT_FORMATS = {}


def load_output_formats(formats: dict) -> None:
    """Load output format overrides, e.g. {"4740": "dbdata"}."""
    OUTPUT_FORMATS.update(formats)


class BusyError(Exception):
    pass


class JobQueue:
    def __init__(self, config: dict, on_update: Optional[Callable] = None):
        self._config = config
        self._on_update = on_update
        self._lock = threading.Lock()
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
        # How long a RESERVED slot may sit before the sweeper reclaims it (the
        # bot also cancels explicitly on close, this is the safety net for crashes).
        self._reservation_ttl = config.get("reservation_ttl", 900)
        self._max_queue = config.get("max_queue", 50)
        self._last_sweep = 0.0
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Job queue started")

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

        accounts = get_accounts_for_uplay_id(uplay_id)
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
        # slots), so a direct submit won't grab a slot another ticket is holding.
        account = get_account_for_uplay_id(uplay_id, self._quota)
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
            with self._condition:
                while not self._pending and self._running:
                    self._condition.wait(timeout=1.0)
                    self._maybe_sweep()

                if not self._running:
                    break

                job = self._pending.popleft()
                self._current = job

            self._notify_update()
            self._process_job(job)

            with self._lock:
                self._current = None

            self._notify_update()

    def _maybe_sweep(self) -> None:
        """Throttled reservation sweep (~every 30s) run from the worker's idle wait."""
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

    def _process_job(self, job: Job) -> None:
        logger.info(f"Processing job {job.id}: uplay_id={job.uplay_id}, account={job.account_email}")
        job.status = JobStatus.PROCESSING
        self._notify_update()

        # Build list of accounts to try: primary first, then fallbacks
        all_accounts = get_accounts_for_uplay_id(job.uplay_id)
        accounts_to_try = [{"email": job.account_email, "accid": job.accid, "folder": job.folder}]
        for acc in all_accounts:
            if acc["email"] != job.account_email and (not acc.get("track_quota", True) or self._quota.can_generate(acc["email"], job.uplay_id)):
                accounts_to_try.append(acc)

        last_error = None
        for attempt, acc in enumerate(accounts_to_try):
            if attempt > 0:
                logger.info(f"Job {job.id}: Retrying with fallback account {acc['email']}...")
                job.account_email = acc["email"]
                job.accid = acc["accid"]
                job.folder = acc["folder"]

            try:
                result = self._worker.generate(
                    folder=acc["folder"],
                    accid=acc["accid"],
                    uplay_id=job.uplay_id,
                    token_req=job.token_req,
                )

                job.denuvo_token = result["denuvo_token"]
                job.ownership_token = result["ownership_token"]
                job.dlc_ids = result["dlc_ids"]
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
                logger.info(f"Job {job.id} completed successfully (account {acc['email']}, format={output_format})")
                self._notify_update()
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Job {job.id}: Account {acc['email']} failed: {e}")
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
        accounts = get_accounts_for_uplay_id(uplay_id)
        return self._quota.get_simple(uplay_id, accounts)

    def get_quota_summary(self) -> dict:
        from core.accounts import read_accounts
        return self._quota.get_summary(read_accounts())

    def get_reservations(self) -> dict:
        """Live reservation snapshot: {'total': N, 'by_uplay': {uplay_id: count}}."""
        return self._quota.reservations_snapshot()

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
        with self._condition:
            self._condition.notify_all()
        logger.info("Job queue shut down")
