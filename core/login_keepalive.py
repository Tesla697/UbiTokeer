import json
import logging
import threading
import time
from pathlib import Path

from core.accounts import read_accounts

logger = logging.getLogger("ubitokeer")

HEALTH_PATH = Path(__file__).parent.parent / "login_health.json"


class LoginKeepAlive:
    """Keeps each account's stored LoginStore.dat session alive.

    An account nobody has touched for a while goes stale: its remember-me session
    expires and the next real ticket dies with "Authentication failed" — which we
    only ever found out when a user's activation failed. This walks the account
    list on a timer and re-authenticates anything that hasn't been exercised
    recently, which both rolls the session forward and tells us EARLY (via the
    health file / API) that an account needs a manual re-login.

    Refreshing is free: CliWorker.refresh_login() stops at the appId prompt and
    never sends a ticket request, so no token is minted and no quota is spent.

    Only ever runs while the queue is idle (`is_busy()` is False) so it can never
    contend with a real generation for the same account folder / LoginStore.dat.
    """

    def __init__(
        self,
        worker,
        is_busy,
        interval_seconds: int = 3600,
        stale_seconds: int = 3 * 86400,
        enabled: bool = True,
        fail_backoff_seconds: int = 3600,
        fail_backoff_max_seconds: int = 86400,
    ):
        self._worker = worker
        self._is_busy = is_busy  # callable -> bool
        self._interval = max(60, int(interval_seconds))
        self._stale = max(3600, int(stale_seconds))
        self._enabled = enabled
        self._fail_backoff = max(300, int(fail_backoff_seconds))
        self._fail_backoff_max = max(self._fail_backoff, int(fail_backoff_max_seconds))
        self._health: dict = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._load()

    # ---- persistence -------------------------------------------------
    def _load(self) -> None:
        if HEALTH_PATH.exists():
            try:
                loaded = json.loads(HEALTH_PATH.read_text()) or {}
                if isinstance(loaded, dict):
                    self._health = loaded
            except Exception as e:
                logger.warning(f"Failed to load login_health.json: {e}")
                self._health = {}

    def _save(self) -> None:
        try:
            HEALTH_PATH.write_text(json.dumps(self._health, indent=2))
        except Exception as e:
            logger.error(f"Failed to save login_health.json: {e}")

    # ---- lifecycle ---------------------------------------------------
    def start(self) -> None:
        if not self._enabled:
            logger.info("Login keep-alive disabled by config")
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="login-keepalive")
        self._thread.start()
        logger.info(
            f"Login keep-alive started (every {self._interval}s, refresh accounts idle "
            f">{self._stale // 86400}d)"
        )

    def stop(self) -> None:
        self._running = False

    # ---- health ------------------------------------------------------
    def get_health(self) -> dict:
        """Per-account login health for the GUI/API."""
        now = time.time()
        with self._lock:
            health = dict(self._health)
        out = []
        for acc in read_accounts():
            email = acc.get("email", "")
            h = health.get(email, {})
            last_ok = h.get("last_ok")
            out.append({
                "email": email,
                "name": acc.get("name", email),
                "ok": h.get("ok"),
                "last_ok": last_ok,
                "last_checked": h.get("last_checked"),
                "error": h.get("error"),
                "age_days": round((now - last_ok) / 86400, 2) if last_ok else None,
                "needs_login": h.get("ok") is False,
            })
        return {"accounts": out, "checked_at": now}

    def _mark(self, email: str, ok: bool, reason: str) -> None:
        now = time.time()
        with self._lock:
            entry = self._health.setdefault(email, {})
            entry["last_checked"] = now
            entry["ok"] = ok
            entry["error"] = None if ok else reason
            if ok:
                entry["last_ok"] = now
                entry["fail_count"] = 0
            else:
                entry["fail_count"] = int(entry.get("fail_count", 0)) + 1
            self._save()

    def note_used(self, email: str) -> None:
        """Record that a real generation just authenticated this account, so the
        keep-alive doesn't re-launch a CLI for an account that's already active."""
        self._mark(email, True, "")

    def note_auth_failed(self, email: str, reason: str = "Authentication failed") -> None:
        """A real generation just failed auth for this account — flag it for a
        manual re-login right away instead of waiting for the next sweep."""
        self._mark(email, False, reason)

    # ---- refresh -----------------------------------------------------
    def refresh_account(self, acc: dict) -> dict:
        """Refresh one account now. Returns {"ok", "reason"}."""
        email = acc.get("email", "")
        folder = acc.get("folder", "")
        accid = acc.get("accid", "")
        if not folder or not accid:
            result = {"ok": False, "reason": "account missing folder/accid"}
        else:
            try:
                result = self._worker.refresh_login(folder, accid)
            except Exception as e:
                result = {"ok": False, "reason": f"refresh crashed: {e}"}
        self._mark(email, bool(result.get("ok")), str(result.get("reason", "")))
        if result.get("ok"):
            logger.info(f"Login keep-alive: {email} OK ({result.get('reason')})")
        else:
            logger.warning(
                f"Login keep-alive: {email} FAILED — {result.get('reason')} "
                f"(this account needs a manual re-login)"
            )
        return result

    def refresh_all(self, force: bool = False) -> dict:
        """Refresh every stale account (or all of them when force=True)."""
        refreshed, failed, skipped = [], [], []
        for acc in read_accounts():
            email = acc.get("email", "")
            if not force and not self._is_stale(email):
                skipped.append(email)
                continue
            r = self.refresh_account(acc)
            (refreshed if r.get("ok") else failed).append(email)
        return {"refreshed": refreshed, "failed": failed, "skipped": skipped}

    def _is_stale(self, email: str) -> bool:
        with self._lock:
            h = dict(self._health.get(email) or {})
        now = time.time()

        # A session that just got REJECTED must not be retried every cycle.
        # Hammering Ubisoft's auth with a dead session is the one pattern here
        # that could look like credential-stuffing and get an account flagged,
        # so back off exponentially (1h, 2h, 4h ... capped at 24h) until someone
        # actually re-logs it in. Re-authenticating a VALID session from our own
        # known device is normal traffic; retrying a dead one in a loop is not.
        if h.get("ok") is False:
            fails = max(1, int(h.get("fail_count", 1)))
            backoff = min(self._fail_backoff * (2 ** (fails - 1)), self._fail_backoff_max)
            return (now - h.get("last_checked", 0)) >= backoff

        last_ok = h.get("last_ok")
        if not last_ok:
            return True  # never seen — check it once so we learn its state
        return (now - last_ok) > self._stale

    # ---- loop --------------------------------------------------------
    def _loop(self) -> None:
        # Small delay so startup (and any queued work) settles first.
        time.sleep(30)
        while self._running:
            try:
                for acc in read_accounts():
                    if not self._running:
                        break
                    email = acc.get("email", "")
                    if not self._is_stale(email):
                        continue
                    # Never contend with a real generation for the same folder.
                    if self._is_busy():
                        logger.debug("Login keep-alive: queue busy, deferring refresh")
                        break
                    self.refresh_account(acc)
                    time.sleep(5)  # stagger so we don't hammer Ubisoft auth
            except Exception as e:
                logger.error(f"Login keep-alive loop error: {e}")
            # Sleep in slices so stop() is responsive.
            waited = 0
            while self._running and waited < self._interval:
                time.sleep(min(5, self._interval - waited))
                waited += 5
