import json
import logging
import re
import subprocess
from pathlib import Path

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

        # Build command
        if command_txt.exists():
            cmd = command_txt.read_text().strip()
            # Replace relative DenuvoTicket.exe with full path
            cmd = cmd.replace("DenuvoTicket.exe", str(exe_path), 1)
            logger.info(f"Using command from command.txt: {cmd[:80]}...")
        else:
            cmd = f"{exe_path} -remember-me -remember-device -accid {accid} -usefilestore"

        # Prepare stdin: appId + token_req
        stdin_data = f"{uplay_id}\n{token_req}\n"
        logger.debug(f"Sending stdin: uplay_id={uplay_id}, token_req={len(token_req)} chars")

        try:
            proc = subprocess.run(
                cmd,
                input=stdin_data,
                capture_output=True,
                text=True,
                cwd=str(folder_path),
                timeout=self._process_timeout,
            )
            full_output = proc.stdout + "\n" + proc.stderr
            logger.debug(f"CLI full output:\n{full_output}")

        except subprocess.TimeoutExpired:
            raise CliWorkerError(f"Process timed out after {self._process_timeout}s")
        except Exception as e:
            raise CliWorkerError(f"Failed to run CLI DenuvoTicket: {e}")

        if "Authentication failed" in full_output:
            raise CliWorkerError("Authentication failed — account credentials may be invalid")

        if "You are not owning this App" in full_output:
            raise CliWorkerError("Account does not own this app")

        # Parse the output
        result = self._parse_output(full_output)
        result["console_output"] = full_output
        return result

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
