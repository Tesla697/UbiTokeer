import logging
import threading
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
        self._pending: Optional[Job] = None
        self._jobs: dict[str, Job] = {}
        self._condition = threading.Condition(self._lock)
        self._quota = QuotaTracker(daily_limit=config.get("daily_limit", 5))
        self._worker = CliWorker(
            process_timeout=config.get("process_timeout", 90),
        )
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Job queue started")

    def submit(self, uplay_id: str, token_req: str) -> Job:
        if not has_any_account_for_uplay_id(uplay_id):
            raise ValueError(f"No account assigned to uplay_id={uplay_id}")

        account = get_account_for_uplay_id(uplay_id, self._quota)
        if not account:
            raise QuotaExceededError(
                f"Daily token limit reached for all accounts assigned to uplay_id={uplay_id}"
            )

        with self._lock:
            if self._current is not None and self._pending is not None:
                raise BusyError("Queue is full. Try again later.")

            job = Job(
                uplay_id=uplay_id,
                account_email=account["email"],
                accid=account["accid"],
                folder=account["folder"],
                token_req=token_req,
            )
            self._jobs[job.id] = job
            self._pending = job
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
                "pending": self._pending.to_dict() if self._pending else None,
            }

    def _worker_loop(self) -> None:
        while self._running:
            with self._condition:
                while self._pending is None and self._running:
                    self._condition.wait(timeout=1.0)

                if not self._running:
                    break

                job = self._pending
                self._pending = None
                self._current = job

            self._notify_update()
            self._process_job(job)

            with self._lock:
                self._current = None

            self._notify_update()

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
        logger.error(f"Job {job.id} failed on all accounts: {last_error}")
        self._notify_update()

    def get_quota_simple(self, uplay_id: str) -> dict:
        accounts = get_accounts_for_uplay_id(uplay_id)
        return self._quota.get_simple(uplay_id, accounts)

    def get_quota_summary(self) -> dict:
        from core.accounts import read_accounts
        return self._quota.get_summary(read_accounts())

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
