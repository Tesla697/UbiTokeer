import json
import logging
import re
import threading
import time
from pathlib import Path

import winpty

logger = logging.getLogger("ubitokeer")


class CliWorkerError(Exception):
    pass


class CliWorker:
    """
    Worker for the CLI-based DenuvoTicket dumper.
    Launches with -remember-me -remember-device -accid <uuid> -usefilestore.
    Interaction: enter appId -> get DLC IDs -> enter token_req -> get tokens.
    Everything is parsed from console output.
    """

    def __init__(self, process_timeout: int = 90):
        self._process_timeout = process_timeout

    def generate(self, folder: str, accid: str, uplay_id: str, token_req: str) -> dict:
        """
        Run the CLI dumper and parse output.

        Returns dict with:
            - denuvo_token: str
            - ownership_token: str
            - dlc_ids: list[int]
            - console_output: str
        """
        folder_path = Path(folder).resolve()
        exe_path = folder_path / "DenuvoTicket.exe"
        command_txt = folder_path / "command.txt"

        if not exe_path.exists():
            raise CliWorkerError(f"DenuvoTicket.exe not found at {exe_path}")

        logger.info(f"Starting CLI DenuvoTicket (accid={accid[:8]}..., uplay_id={uplay_id})...")

        collected_output = []

        try:
            # Use command from command.txt if it exists, otherwise build default
            if command_txt.exists():
                cmd = command_txt.read_text().strip()
                logger.info(f"Using command from command.txt: {cmd[:80]}...")
            else:
                cmd = f"{exe_path} -remember-me -remember-device -accid {accid} -usefilestore"
            pty = winpty.PtyProcess.spawn(
                cmd,
                cwd=str(folder_path),
            )

            # Background reader thread
            stop_event = threading.Event()

            def _reader():
                while not stop_event.is_set():
                    try:
                        data = pty.read(4096)
                        if data:
                            collected_output.append(data)
                            logger.debug(f"PTY: {data.strip()}")
                    except EOFError:
                        break
                    except Exception:
                        break

            reader_thread = threading.Thread(target=_reader, daemon=True)
            reader_thread.start()

            deadline = time.time() + self._process_timeout

            # Step 1: Wait for appId prompt
            if not self._wait_for_text(collected_output, "appId", deadline):
                raise CliWorkerError("Timed out waiting for appId prompt")

            time.sleep(0.5)
            logger.debug(f"Sending uplay_id: {uplay_id}")
            pty.write(f"{uplay_id}\r\n")

            # Step 2: Wait for ticket request prompt (DLC IDs appear before this)
            if not self._wait_for_text(collected_output, "denuvo ticket request", deadline):
                raise CliWorkerError("Timed out waiting for ticket request prompt")

            time.sleep(0.5)
            logger.debug(f"Sending token_req ({len(token_req)} chars)...")
            # Write in chunks to avoid PTY buffer overflow
            chunk_size = 512
            for i in range(0, len(token_req), chunk_size):
                pty.write(token_req[i:i + chunk_size])
                time.sleep(0.05)
            pty.write("\r\n")

            # Step 3: Wait for tokens to appear in output
            while time.time() < deadline:
                full = "".join(collected_output)
                if "DenuvoToken" in full and "OwnershipToken" in full:
                    logger.info("Tokens detected in output")
                    break
                if not pty.isalive():
                    break
                time.sleep(1)

            # Give it a moment to finish output
            time.sleep(2)
            stop_event.set()

            full_output = "".join(collected_output)
            logger.debug(f"CLI full output:\n{full_output}")

            if "Authentication failed" in full_output:
                raise CliWorkerError("Authentication failed — account credentials may be invalid")

            # Kill if still running
            if pty.isalive():
                try:
                    pty.terminate()
                except Exception:
                    pass

        except CliWorkerError:
            raise
        except Exception as e:
            raise CliWorkerError(f"Failed to run CLI DenuvoTicket: {e}")

        # Parse the output
        full_output = "".join(collected_output)
        result = self._parse_output(full_output)
        result["console_output"] = full_output
        return result

    def _wait_for_text(self, collected: list, text: str, deadline: float) -> bool:
        while time.time() < deadline:
            full = "".join(collected)
            if text.lower() in full.lower():
                return True
            time.sleep(0.5)
        return False

    def _parse_output(self, output: str) -> dict:
        # Parse DLC IDs from "Your owned product Associations: 918, 5900, ..."
        dlc_match = re.search(r"(?:Associations|Association)[:\s]+([0-9,\s]+)", output)
        if not dlc_match:
            raise CliWorkerError("Could not find DLC/Association IDs in output")
        dlc_str = dlc_match.group(1).strip().rstrip(",")
        dlc_ids = [int(x.strip()) for x in dlc_str.split(",") if x.strip().isdigit()]

        if not dlc_ids:
            raise CliWorkerError("Parsed DLC IDs list is empty")

        # Parse DenuvoToken
        denuvo_match = re.search(r"DenuvoToken[:\s]+([A-Za-z0-9_\-+=/.]+)", output)
        if not denuvo_match:
            raise CliWorkerError("Could not find DenuvoToken in output")
        denuvo_token = denuvo_match.group(1).strip()

        # Parse OwnershipToken (might be "OwnershipToken" or "OwnershipListToken")
        ownership_match = re.search(r"Ownership(?:List)?Token[:\s]+([A-Za-z0-9_\-+=/.]+)", output)
        if not ownership_match:
            raise CliWorkerError("Could not find OwnershipToken in output")
        ownership_token = ownership_match.group(1).strip()

        logger.info(f"Parsed: {len(dlc_ids)} DLC IDs, DenuvoToken ({len(denuvo_token)} chars), OwnershipToken ({len(ownership_token)} chars)")

        return {
            "denuvo_token": denuvo_token,
            "ownership_token": ownership_token,
            "dlc_ids": dlc_ids,
        }

    @staticmethod
    def build_token_ini(denuvo_token: str, ownership_token: str) -> str:
        """Build token.ini content."""
        return f"[token]\ntoken={denuvo_token}\nownership={ownership_token}\n"

    @staticmethod
    def build_dbdata_json(denuvo_token: str, ownership_token: str, dlc_ids: list[int]) -> str:
        """Build dbdata.json content."""
        data = {
            "DenuvoToken": denuvo_token,
            "OwnershipListToken": ownership_token,
            "DLCIds": dlc_ids,
        }
        return json.dumps(data, indent=2)
