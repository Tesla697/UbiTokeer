"""
POSIX (Linux) DenuvoTicket worker for the donor node.

Mirrors core.cli_worker.CliWorker but drives the Linux DenuvoTicket build over a
pexpect PTY instead of Windows winpty. The interactive prompts and the output
parsing are identical (same DenuvoTicket.dll on both OSes), so the returned result
shape matches the Windows worker exactly:
    {denuvo_token, ownership_token, dlc_ids, console_output}

Requires the Linux DenuvoTicket build (native ./DenuvoTicket ELF launcher) and:
    pip install pexpect
    apt install dotnet-runtime-8.0   # the app is .NET 8 framework-dependent
    # libzstd is resolved from the system lib; if zstd errors: apt install libzstd-dev
"""

import logging
import os
import re
import time
from pathlib import Path

import pexpect

logger = logging.getLogger("ubitokeer.node")


class CliWorkerError(Exception):
    pass


class PosixCliWorker:
    def __init__(self, process_timeout: int = 120):
        self._process_timeout = process_timeout

    # ------------------------------------------------------------------
    def _launcher(self, folder_path: Path) -> str:
        """The native Linux DenuvoTicket ELF (no .exe). Ensure it's executable."""
        exe = folder_path / "DenuvoTicket"
        if not exe.exists():
            raise CliWorkerError(f"DenuvoTicket launcher not found at {exe}")
        try:
            os.chmod(exe, 0o755)  # in case the donor didn't chmod +x
        except OSError:
            pass
        return str(exe)

    def _spawn(self, folder_path: Path, accid: str):
        launcher = self._launcher(folder_path)
        args = ["-remember-me", "-remember-device"]
        if accid:
            args += ["-accid", accid]
        args += ["-usefilestore"]
        logger.info(f"Launching DenuvoTicket (accid={accid[:8] + '...' if accid else 'none'})")
        # DenuvoTicket targets net8.0. .NET only roll-forwards across a MAJOR version
        # (e.g. onto an installed .NET 9) when asked, so set this — otherwise a box
        # that has .NET 9 but not .NET 8 fails with "framework 8.0.0 was not found".
        env = dict(os.environ)
        env.setdefault("DOTNET_ROLL_FORWARD", "Major")
        # Wide terminal to stop the long token lines from wrapping (matches winpty
        # dimensions=(25, 5000) on the Windows worker).
        return pexpect.spawn(launcher, args, cwd=str(folder_path), env=env,
                             encoding="utf-8", codec_errors="replace",
                             timeout=self._process_timeout, dimensions=(25, 5000))

    @staticmethod
    def _pump(child, collected: list, stop_substr, deadline: float) -> bool:
        """Accumulate output until stop_substr appears (case-insensitive), the
        process exits, or the deadline passes. Returns True if stop_substr was seen."""
        while time.time() < deadline:
            try:
                data = child.read_nonblocking(size=4096, timeout=1)
                if data:
                    collected.append(data)
            except pexpect.TIMEOUT:
                pass
            except pexpect.EOF:
                break
            except Exception:
                break
            full = "".join(collected)
            if callable(stop_substr):
                if stop_substr(full):
                    return True
            elif stop_substr.lower() in full.lower():
                return True
        return False

    # ------------------------------------------------------------------
    def generate(self, folder: str, accid: str, uplay_id: str, token_req: str) -> dict:
        """Run the Linux DenuvoTicket CLI and parse tokens from its output."""
        folder_path = Path(folder).resolve()
        child = None
        collected: list[str] = []
        try:
            child = self._spawn(folder_path, accid)
            deadline = time.time() + self._process_timeout

            # Step 1: wait for the appId prompt (only appears after auth succeeded).
            if not self._pump(child, collected, "appId", deadline):
                raise CliWorkerError("Timed out waiting for appId prompt")
            time.sleep(0.5)
            child.sendline(uplay_id)

            # Step 2: wait for the ticket-request prompt (DLC IDs print before this).
            if not self._pump(child, collected, "denuvo ticket request", deadline):
                raise CliWorkerError("Timed out waiting for ticket request prompt")
            time.sleep(1)
            # Strip metadata after '|' (e.g. "base64|1081" -> "base64").
            clean_token_req = token_req.split("|")[0] if "|" in token_req else token_req
            logger.info(f"Sending token_req ({len(clean_token_req)} chars)")
            child.send(clean_token_req)
            time.sleep(1)
            child.sendline("")

            # Step 3: wait for tokens or a failure marker.
            def _done(full: str) -> bool:
                has_denuvo = "DenuvoToken" in full or "GameToken" in full
                has_owner = "OwnershipToken" in full or "OwnershipListToken" in full
                if has_denuvo and has_owner:
                    return True
                for marker in ("ExceededActivations", "Authentication failed",
                               "You are not owning this App"):
                    if marker in full:
                        return True
                return False

            self._pump(child, collected, _done, deadline)
            time.sleep(1)

            full_output = "".join(collected)
            if "Authentication failed" in full_output:
                raise CliWorkerError("Authentication failed — account credentials may be invalid")
            if "ExceededActivations" in full_output:
                raise CliWorkerError("Account has exceeded its activation limit")
            if "You are not owning this App" in full_output:
                raise CliWorkerError("Account does not own this app")

        except CliWorkerError:
            raise
        except Exception as e:
            raise CliWorkerError(f"Failed to run DenuvoTicket: {e}")
        finally:
            self._close(child)

        result = self._parse_output("".join(collected))
        result["console_output"] = "".join(collected)
        return result

    def refresh_login(self, folder: str, accid: str, timeout: int = 90) -> dict:
        """Re-authenticate WITHOUT minting a token — reaching the appId prompt proves
        the stored session still works, so we quit right there. Mirrors CliWorker."""
        folder_path = Path(folder).resolve()
        child = None
        collected: list[str] = []
        try:
            child = self._spawn(folder_path, accid)
            deadline = time.time() + timeout
            reached = self._pump(child, collected, "appId", deadline)
            full = "".join(collected)
            if "authentication failed" in full.lower():
                return {"ok": False, "reason": "Authentication failed — needs manual re-login"}
            if reached:
                time.sleep(2)  # let the refreshed session flush to disk
                return {"ok": True, "reason": "session refreshed"}
            return {"ok": False, "reason": "Timed out waiting for appId prompt (login likely stale)"}
        except Exception as e:
            return {"ok": False, "reason": f"Failed to launch DenuvoTicket: {e}"}
        finally:
            self._close(child)

    @staticmethod
    def _close(child) -> None:
        if not child:
            return
        try:
            if child.isalive():
                child.terminate(force=True)
        except Exception:
            pass
        try:
            child.close(force=True)
        except Exception:
            pass

    @staticmethod
    def _parse_output(output: str) -> dict:
        dlc_match = re.search(r"(?:Associations|Association)[:\s]+([0-9,\s]+)", output)
        if not dlc_match:
            raise CliWorkerError("Could not find DLC/Association IDs in output")
        dlc_str = dlc_match.group(1).strip().rstrip(",")
        dlc_ids = [int(x.strip()) for x in dlc_str.split(",") if x.strip().isdigit()]
        if not dlc_ids:
            raise CliWorkerError("Parsed DLC IDs list is empty")

        denuvo_match = re.search(r"(?:DenuvoToken|GameToken)[:\s]+([A-Za-z0-9_\-+=/.]+)", output)
        if not denuvo_match:
            raise CliWorkerError("Could not find DenuvoToken/GameToken in output")

        ownership_match = re.search(r"Ownership(?:List)?Token[:\s]+([A-Za-z0-9_\-+=/.]+)", output)
        if not ownership_match:
            raise CliWorkerError("Could not find OwnershipToken in output")

        return {
            "denuvo_token": denuvo_match.group(1).strip(),
            "ownership_token": ownership_match.group(1).strip(),
            "dlc_ids": dlc_ids,
        }
