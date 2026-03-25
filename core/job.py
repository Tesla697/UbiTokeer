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
    def __init__(self, uplay_id: str, account_email: str, accid: str,
                 folder: str, token_req: str):
        self.id = uuid.uuid4().hex[:8]
        self.uplay_id = uplay_id
        self.account_email = account_email
        self.accid = accid
        self.folder = folder
        self.token_req = token_req
        self.status = JobStatus.QUEUED
        # Parsed output
        self.denuvo_token: Optional[str] = None
        self.ownership_token: Optional[str] = None
        self.dlc_ids: Optional[list[int]] = None
        self.console_output: Optional[str] = None
        # Formatted output (set by job queue based on output_format)
        self.token_ini: Optional[str] = None
        self.dbdata_json: Optional[str] = None
        # Common
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
        if self.dbdata_json:
            d["dbdata_json"] = self.dbdata_json
        if self.denuvo_token:
            d["denuvo_token"] = self.denuvo_token
        if self.ownership_token:
            d["ownership_token"] = self.ownership_token
        if self.dlc_ids:
            d["dlc_ids"] = self.dlc_ids
        if self.error:
            d["error"] = self.error
        if self.finished_at:
            d["finished_at"] = self.finished_at.isoformat()
        return d
