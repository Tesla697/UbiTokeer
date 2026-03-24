import logging
import threading
from datetime import datetime
from typing import Callable, Optional

from core.accounts import get_account_for_uplay_id, get_accounts_for_uplay_id, has_any_account_for_uplay_id
from core.denuvo_worker import DenuvoWorker, DenuvoWorkerError
from core.job import Job, JobStatus
from core.quota import QuotaExceededError, QuotaTracker

logger = logging.getLogger("ubitokeer")


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
        self._worker_obj = DenuvoWorker(
            activator_path=config["activator_path"],
            token_output_dir=config["token_output_dir"],
            process_timeout=config.get("process_timeout", 60),
        )
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Job queue started")

    def submit(self, uplay_id: str, token_req: str) -> Job:
        """Submit a new token generation job."""
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
                account_number=account["number"],
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

        try:
            token_ini = self._worker_obj.generate_token(
                account_number=job.account_number,
                token_req=job.token_req,
            )
            job.token_ini = token_ini
            job.status = JobStatus.DONE
            job.finished_at = datetime.utcnow()
            self._quota.record(job.account_email, job.uplay_id)
            logger.info(f"Job {job.id} completed successfully")

        except DenuvoWorkerError as e:
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.finished_at = datetime.utcnow()
            logger.error(f"Job {job.id} failed: {e}")

        except Exception as e:
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.finished_at = datetime.utcnow()
            logger.error(f"Job {job.id} unexpected error: {e}")

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

    def shutdown(self) -> None:
        self._running = False
        with self._condition:
            self._condition.notify_all()
        logger.info("Job queue shut down")
