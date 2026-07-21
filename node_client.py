"""
UbiTokeer donor node — runs on a DONOR's PC.

A donor lets people generate tokens from their Ubisoft account for a specific
game, WITHOUT handing over their credentials. This client keeps their account and
DenuvoTicket.exe entirely on their machine and connects OUTBOUND to the UbiTokeer
backend (so it works behind a home router with no port-forwarding):

  1. long-poll the backend for a job assigned to this node,
  2. run DenuvoTicket locally against the donor's account (same CliWorker the
     backend uses), producing the tokens for the requester's token_req,
  3. upload ONLY the finished tokens back — never the account/login.

While this client keeps polling, the backend shows the donor's game in-stock; the
moment it stops (PC off/closed), the game goes out-of-stock automatically.

Setup (one time): sign the donor's Ubisoft account into DenuvoTicket.exe once so a
LoginStore session exists, then fill node_config.json and run this.
"""

import json
import logging
import sys
import time
from pathlib import Path

import requests

# Pick the DenuvoTicket driver for this OS. Windows uses winpty (core.cli_worker);
# Linux uses a pexpect PTY (posix_cli_worker) driving the native ./DenuvoTicket
# build. Both expose the same generate()/refresh_login() and return the same result
# shape, so the rest of the node is OS-agnostic.
if sys.platform == "win32":
    from core.cli_worker import CliWorker as TicketWorker
else:
    from posix_cli_worker import PosixCliWorker as TicketWorker

# Resolve paths next to the EXE, not PyInstaller's temp extraction dir. When frozen
# (onefile), __file__ lives in _MEIPExxxx, so the donor's node_config.json sitting
# beside UbiTokeerNode.exe would never be found without this.
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

CONFIG_PATH = BASE_DIR / "node_config.json"

logger = logging.getLogger("ubitokeer.node")


def setup_logging() -> None:
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    try:
        fh = logging.FileHandler(str(BASE_DIR / "node.log"), encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
        logger.addHandler(fh)
    except Exception:
        pass


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise SystemExit(
            f"node_config.json not found next to node_client.\n"
            f"Copy node_config.example.json to {CONFIG_PATH} and fill it in."
        )
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    for required in ("backend_url", "node_id", "key", "folder"):
        if not cfg.get(required):
            raise SystemExit(f"node_config.json is missing '{required}'")
    return cfg


class DonorNode:
    def __init__(self, cfg: dict):
        self._base = cfg["backend_url"].rstrip("/")
        self._node_id = str(cfg["node_id"])
        self._key = str(cfg["key"])
        self._folder = cfg["folder"]
        self._accid = str(cfg.get("accid", ""))
        # Games this node is willing to serve. A job for anything else is refused
        # (defence in depth — the backend already scopes jobs to this node).
        self._uplay_ids = {str(u) for u in cfg.get("uplay_ids", [])}
        self._poll_wait = float(cfg.get("poll_wait", 25))
        self._worker = TicketWorker(process_timeout=int(cfg.get("process_timeout", 120)))
        self._session = requests.Session()

        # Keep the donor's stored session alive between jobs so an idle day doesn't
        # let it go stale and start failing tickets. Costs nothing — refresh_login
        # stops at the appId prompt and never mints a token.
        self._login_refresh_enabled = bool(cfg.get("login_refresh_enabled", True))
        self._login_refresh_interval = float(cfg.get("login_refresh_interval_seconds", 3600))
        self._login_refresh_timeout = int(cfg.get("login_refresh_timeout", 90))
        self._last_refresh = time.time()  # count from startup (we just launched)

    # ------------------------------------------------------------------
    def run(self) -> None:
        logger.info("=" * 60)
        logger.info(f"UbiTokeer donor node '{self._node_id}' starting")
        logger.info(f"Backend : {self._base}")
        logger.info(f"Serving : {', '.join(sorted(self._uplay_ids)) or '(any assigned)'}")
        logger.info(f"Folder  : {self._folder}")
        logger.info("=" * 60)

        backoff = 3.0
        while True:
            try:
                job = self._poll()
                backoff = 3.0  # reset after any successful contact
                if job:
                    self._handle(job)
                else:
                    self._maybe_refresh_login()
            except KeyboardInterrupt:
                logger.info("Shutting down (Ctrl+C).")
                return
            except _AuthError:
                logger.error(
                    "Backend rejected this node's credentials (node_id/key). "
                    "Fix node_config.json — retrying in 30s."
                )
                time.sleep(30)
            except requests.RequestException as e:
                logger.warning(f"Backend unreachable ({e}); retrying in {backoff:.0f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)  # exponential, capped
            except Exception as e:
                logger.error(f"Unexpected error in poll loop: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------------
    def _poll(self) -> dict | None:
        """Long-poll for the next job. Returns the job dict or None (no work)."""
        resp = self._session.post(
            f"{self._base}/node/poll",
            json={"node_id": self._node_id, "key": self._key, "wait": self._poll_wait},
            # Give the server its full long-poll window plus network slack.
            timeout=self._poll_wait + 20,
        )
        if resp.status_code == 401:
            raise _AuthError()
        if resp.status_code == 204:
            return None
        resp.raise_for_status()
        return resp.json()

    def _handle(self, job: dict) -> None:
        job_id = job.get("job_id", "")
        uplay_id = str(job.get("uplay_id", ""))
        token_req = job.get("token_req", "")
        logger.info(f"Job {job_id}: generating for uplay_id={uplay_id}")

        if self._uplay_ids and uplay_id not in self._uplay_ids:
            logger.warning(f"Job {job_id}: uplay_id {uplay_id} not in this node's list — refusing")
            self._post_result(job_id, error=f"node does not serve uplay_id {uplay_id}")
            return

        try:
            result = self._worker.generate(
                folder=self._folder,
                accid=self._accid,
                uplay_id=uplay_id,
                token_req=token_req,
            )
            self._post_result(job_id, result=result)
            self._last_refresh = time.time()  # a real generation proves the login works
            logger.info(f"Job {job_id}: done, tokens uploaded")
        except Exception as e:
            logger.error(f"Job {job_id}: generation failed — {e}")
            self._post_result(job_id, error=str(e))

    def _post_result(self, job_id: str, result: dict | None = None,
                     error: str | None = None) -> None:
        payload = {"node_id": self._node_id, "key": self._key, "job_id": job_id}
        if error is not None:
            payload["error"] = error
        else:
            payload["denuvo_token"] = result.get("denuvo_token")
            payload["ownership_token"] = result.get("ownership_token")
            payload["dlc_ids"] = result.get("dlc_ids")
            payload["console_output"] = result.get("console_output", "")
        try:
            r = self._session.post(f"{self._base}/node/result", json=payload, timeout=30)
            if r.status_code == 401:
                raise _AuthError()
            if r.status_code == 200 and not r.json().get("ok", False):
                logger.warning(f"Job {job_id}: backend no longer waiting (it may have timed out)")
        except _AuthError:
            raise
        except requests.RequestException as e:
            logger.error(f"Job {job_id}: failed to upload result — {e}")

    def _maybe_refresh_login(self) -> None:
        if not self._login_refresh_enabled:
            return
        if time.time() - self._last_refresh < self._login_refresh_interval:
            return
        logger.info("Idle — refreshing the stored Ubisoft session (no token minted)")
        try:
            res = self._worker.refresh_login(self._folder, self._accid,
                                             timeout=self._login_refresh_timeout)
            if res.get("ok"):
                logger.info("Session refreshed.")
            else:
                logger.warning(f"Session refresh: {res.get('reason')}")
        except Exception as e:
            logger.warning(f"Session refresh failed: {e}")
        # Reset the timer regardless, so a failing refresh doesn't spin every loop.
        self._last_refresh = time.time()


class _AuthError(Exception):
    pass


def main() -> None:
    setup_logging()
    cfg = load_config()
    DonorNode(cfg).run()


if __name__ == "__main__":
    main()
