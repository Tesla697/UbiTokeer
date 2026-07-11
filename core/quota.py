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
        # Live slot reservations: job_id -> {account_email, uplay_id, created_at}.
        # A reservation holds one slot for an (account, uplay_id) pair without
        # having generated a token yet, so it counts against "remaining" exactly
        # like a used slot until it's released (activated+recorded, cancelled, or
        # swept for being stale). This is what stops the pool from being oversold
        # when many users open tickets at once.
        self._reservations: dict[str, dict] = {}
        self._load()

    def _key(self, account_email: str, uplay_id: str) -> str:
        return f"{account_email}:{uplay_id}"

    # ---- lock-held helpers (caller must hold self._lock) ----
    def _used_locked(self, account_email: str, uplay_id: str) -> int:
        key = self._key(account_email, uplay_id)
        entry = self._data.get(key)
        if not entry:
            return 0
        if time.time() > entry["window_start"] + 86400:
            del self._data[key]
            return 0
        return entry["count"]

    def _reserved_locked(self, account_email: str, uplay_id: str) -> int:
        return sum(
            1 for r in self._reservations.values()
            if r["account_email"] == account_email and r["uplay_id"] == uplay_id
        )

    def reserved_for_uplay(self, uplay_id: str) -> int:
        """How many slots are currently held (reserved, not yet generated) for a game."""
        with self._lock:
            return sum(1 for r in self._reservations.values() if r["uplay_id"] == uplay_id)

    def reservations_snapshot(self) -> dict:
        """{'total': N, 'by_uplay': {uplay_id: count}} of all live reservations."""
        with self._lock:
            by_uplay: dict[str, int] = {}
            for r in self._reservations.values():
                by_uplay[r["uplay_id"]] = by_uplay.get(r["uplay_id"], 0) + 1
            return {"total": len(self._reservations), "by_uplay": by_uplay}

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
            used = self._used_locked(account_email, uplay_id)
            reserved = self._reserved_locked(account_email, uplay_id)
            return max(0, self._daily_limit - used - reserved)

    def can_generate(self, account_email: str, uplay_id: str) -> bool:
        return self.get_remaining(account_email, uplay_id) > 0

    def get_used(self, account_email: str, uplay_id: str) -> int:
        """Real recorded usage (window-aware), NOT counting live reservations.
        This is what the admin Quota panel shows and what +/- move, so transient
        holds can't make the number look stuck."""
        with self._lock:
            return self._used_locked(account_email, uplay_id)

    def get_reserved(self, account_email: str, uplay_id: str) -> int:
        """How many slots this account is currently holding as reservations."""
        with self._lock:
            return self._reserved_locked(account_email, uplay_id)

    def try_reserve(self, job_id: str, accounts: list[dict], uplay_id: str) -> str | None:
        """Atomically hold one slot for uplay_id against the first account with room.

        Returns the chosen account email, or None if every tracked account is at
        its limit (accounting for slots already used AND already reserved). Doing
        the pick + hold under one lock is what makes concurrent ticket opens safe:
        two simultaneous reservations can't both grab the same last slot.
        """
        with self._lock:
            for acc in accounts:
                email = acc["email"]
                if not acc.get("track_quota", True):
                    # Untracked account = unlimited; still record the reservation
                    # so the job has a hold to release later, but it never blocks.
                    self._reservations[job_id] = {
                        "account_email": email, "uplay_id": uplay_id, "created_at": time.time(),
                    }
                    return email
                used = self._used_locked(email, uplay_id)
                reserved = self._reserved_locked(email, uplay_id)
                if self._daily_limit - used - reserved > 0:
                    self._reservations[job_id] = {
                        "account_email": email, "uplay_id": uplay_id, "created_at": time.time(),
                    }
                    logger.info(
                        f"Quota reserved: {email}:{uplay_id} (job {job_id}) — "
                        f"{used} used, {reserved + 1} held, "
                        f"{max(0, self._daily_limit - used - reserved - 1)} free"
                    )
                    return email
            return None

    def release(self, job_id: str) -> bool:
        """Drop a reservation hold (idempotent). Called on activate-complete,
        cancel, or sweep. Safe to call for a job that never reserved."""
        with self._lock:
            r = self._reservations.pop(job_id, None)
        if r:
            logger.info(f"Quota reservation released: job {job_id} ({r['account_email']}:{r['uplay_id']})")
        return r is not None

    def sweep(self, ttl_seconds: float) -> list[str]:
        """Release reservations older than ttl_seconds (a crashed/abandoned open
        that never activated). Returns the swept job_ids."""
        now = time.time()
        with self._lock:
            stale = [
                jid for jid, r in self._reservations.items()
                if now - r["created_at"] > ttl_seconds
            ]
            for jid in stale:
                del self._reservations[jid]
        if stale:
            logger.info(f"Swept {len(stale)} stale reservation(s): {stale}")
        return stale

    def decrement(self, account_email: str, uplay_id: str) -> None:
        with self._lock:
            key = self._key(account_email, uplay_id)
            entry = self._data.get(key)
            if entry and entry["count"] > 0:
                entry["count"] -= 1
                if entry["count"] == 0:
                    del self._data[key]
                self._save()

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

        has_untracked = False
        for acc in accounts:
            if not acc.get("track_quota", True):
                has_untracked = True
                continue
            remaining = self.get_remaining(acc["email"], uplay_id)
            total_remaining += remaining
            if remaining == 0:
                key = self._key(acc["email"], uplay_id)
                entry = self._data.get(key)
                if entry:
                    reset_in_ms = int((entry["window_start"] + 86400 - now) * 1000)
                    if next_slot_ms is None or reset_in_ms < next_slot_ms:
                        next_slot_ms = max(0, reset_in_ms)

        # Untracked accounts always have tokens available
        if has_untracked:
            total_remaining = max(total_remaining, 1)

        resets_in = _format_duration(next_slot_ms / 1000) if next_slot_ms else None
        return {
            "game_name": game_names.get(uplay_id, f"Uplay {uplay_id}"),
            "remaining": total_remaining,
            "reserved": self.reserved_for_uplay(uplay_id),
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
                total_reserved = 0
                for acc in accs:
                    track = acc.get("track_quota", True)
                    key = self._key(acc["email"], uplay_id)
                    entry = self._data.get(key)
                    if entry and now > entry["window_start"] + 86400:
                        entry = None
                    acc_reserved = self._reserved_locked(acc["email"], uplay_id)
                    total_reserved += acc_reserved
                    if not track:
                        # Untracked account — don't show quota numbers (but do show holds)
                        acc_details.append({
                            "email": acc["email"],
                            "used": -1,
                            "remaining": -1,
                            "reserved": acc_reserved,
                            "window_resets_at": None,
                            "resets_in": None,
                            "track_quota": False,
                        })
                        continue
                    used = entry["count"] if entry else 0
                    reserved = acc_reserved
                    remaining = max(0, self._daily_limit - used - reserved)
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
                        "reserved": reserved,
                        "window_resets_at": resets_at,
                        "resets_in": resets_in,
                        "track_quota": True,
                    })

                total_remaining = sum(a["remaining"] for a in acc_details if a["remaining"] >= 0)
                result[uplay_id] = {
                    "game_name": game_names.get(uplay_id, f"Uplay {uplay_id}"),
                    "total_used": total_used,
                    "total_remaining": total_remaining,
                    "total_reserved": total_reserved,
                    "limit_per_account": self._daily_limit,
                    "accounts": acc_details,
                }

        return result
