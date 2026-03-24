import uuid
from datetime import datetime
from enum import Enum
from typing import Optional


class JobStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


class Job:
    def __init__(self, uplay_id: str, account_email: str, account_number: int, token_req: str):
        self.id = uuid.uuid4().hex[:8]
        self.uplay_id = uplay_id
        self.account_email = account_email
        self.account_number = account_number
        self.token_req = token_req
        self.status = JobStatus.QUEUED
        self.token_ini: Optional[str] = None
        self.error: Optional[str] = None
        self.created_at = datetime.utcnow()
        self.finished_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        d = {
            "job_id": self.id,
            "uplay_id": self.uplay_id,
            "account_email": self.account_email,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
        }
        if self.token_ini:
            d["token_ini"] = self.token_ini
        if self.error:
            d["error"] = self.error
        if self.finished_at:
            d["finished_at"] = self.finished_at.isoformat()
        return d
