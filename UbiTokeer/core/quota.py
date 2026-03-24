import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("ubitokeer")

QUOTA_PATH = Path(__file__).parent.parent / "quota.json"
GAME_NAMES_PATH = Path(__file__).parent.parent / "game_names.json"


def load_game_names() -> dict[str, str]:
    if GAME_NAMES_PATH.exists():
        try:
            return json.loads(GAME_NAMES_PATH.read_text())
        except Exception:
            pass
    return {}


class QuotaExceededError(Exception):
    pass


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds >= 3600:
        h, rem = divmod(seconds, 3600)
        return f"{h}h {rem // 60:02d}m"
    if seconds >= 60:
        m, s = divmod(seconds, 60)
        return f"{m}m {s:02d}s"
    return f"{seconds}s"


class QuotaTracker:
    def __init__(self, daily_limit: int = 5):
        self._data: dict = {}
        self._lock = threading.Lock()
        self._daily_limit = daily_limit
        self._load()

    def _key(self, account_email: str, uplay_id: str) -> str:
        return f"{account_email}:{uplay_id}"

    def _load(self) -> None:
        if QUOTA_PATH.exists():
            try:
                self._data = json.loads(QUOTA_PATH.read_text())
                logger.debug(f"Quota loaded: {len(self._data)} entries")
            except Exception as e:
                logger.warning(f"Failed to load quota.json: {e}")
                self._data = {}

    def _save(self) -> None:
        try:
            QUOTA_PATH.write_text(json.dumps(self._data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save quota.json: {e}")

    def get_remaining(self, account_email: str, uplay_id: str) -> int:
        with self._lock:
            key = self._key(account_email, uplay_id)
            entry = self._data.get(key)
            if not entry:
                return self._daily_limit
            if time.time() > entry["window_start"] + 86400:
                del self._data[key]
                return self._daily_limit
            return max(0, self._daily_limit - entry["count"])

    def can_generate(self, account_email: str, uplay_id: str) -> bool:
        return self.get_remaining(account_email, uplay_id) > 0

    def record(self, account_email: str, uplay_id: str) -> None:
        with self._lock:
            key = self._key(account_email, uplay_id)
            entry = self._data.get(key)
            now = time.time()
            if not entry or now > entry["window_start"] + 86400:
                self._data[key] = {"count": 1, "window_start": now}
            else:
                entry["count"] += 1
            remaining = max(0, self._daily_limit - self._data[key]["count"])
            logger.info(
                f"Quota recorded: {account_email}:{uplay_id} — "
                f"{self._data[key]['count']}/{self._daily_limit} used, {remaining} remaining"
            )
            self._save()

    def get_simple(self, uplay_id: str, accounts: list[dict]) -> dict:
        game_names = load_game_names()
        total_remaining = 0
        now = time.time()
        next_slot_ms = None

        for acc in accounts:
            remaining = self.get_remaining(acc["email"], uplay_id)
            total_remaining += remaining
            if remaining == 0:
                key = self._key(acc["email"], uplay_id)
                entry = self._data.get(key)
                if entry:
                    reset_in_ms = int((entry["window_start"] + 86400 - now) * 1000)
                    if next_slot_ms is None or reset_in_ms < next_slot_ms:
                        next_slot_ms = max(0, reset_in_ms)

        resets_in = _format_duration(next_slot_ms / 1000) if next_slot_ms else None
        return {
            "game_name": game_names.get(uplay_id, f"Uplay {uplay_id}"),
            "remaining": total_remaining,
            "next_slot_in_ms": next_slot_ms,
            "resets_in": resets_in,
        }

    def get_summary(self, accounts: list[dict]) -> dict:
        game_names = load_game_names()
        now = time.time()
        result = {}

        # Group accounts by uplay_id
        uplay_map: dict[str, list[dict]] = {}
        for acc in accounts:
            for uid in acc["uplay_ids"]:
                uplay_map.setdefault(uid, []).append(acc)

        with self._lock:
            for uplay_id, accs in uplay_map.items():
                acc_details = []
                total_used = 0
                for acc in accs:
                    key = self._key(acc["email"], uplay_id)
                    entry = self._data.get(key)
                    if entry and now > entry["window_start"] + 86400:
                        entry = None
                    used = entry["count"] if entry else 0
                    remaining = max(0, self._daily_limit - used)
                    resets_at = (
                        datetime.fromtimestamp(
                            entry["window_start"] + 86400, tz=timezone.utc
                        ).isoformat()
                        if entry else None
                    )
                    reset_secs = entry["window_start"] + 86400 - now if entry else None
                    resets_in = _format_duration(reset_secs) if reset_secs is not None else None
                    total_used += used
                    acc_details.append({
                        "email": acc["email"],
                        "used": used,
                        "remaining": remaining,
                        "window_resets_at": resets_at,
                        "resets_in": resets_in,
                    })

                total_remaining = sum(a["remaining"] for a in acc_details)
                result[uplay_id] = {
                    "game_name": game_names.get(uplay_id, f"Uplay {uplay_id}"),
                    "total_used": total_used,
                    "total_remaining": total_remaining,
                    "limit_per_account": self._daily_limit,
                    "accounts": acc_details,
                }

        return result
