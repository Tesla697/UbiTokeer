import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger("ubitokeer")


class DenuvoWorkerError(Exception):
    pass


class DenuvoWorker:
    def __init__(self, activator_path: str, token_output_dir: str, process_timeout: int = 60):
        self._activator_path = Path(activator_path).resolve()
        self._token_output_dir = Path(token_output_dir).resolve()
        self._process_timeout = process_timeout

        if not self._activator_path.exists():
            raise FileNotFoundError(f"DenuvoTicket.exe not found at {self._activator_path}")

    def generate_token(self, account_number: int, token_req: str) -> str:
        """
        Run DenuvoTicket.exe, select account, feed token_req, return token.ini content.

        Args:
            account_number: The menu number to select the account (1, 2, etc.)
            token_req: The full token request string (including |uplay_id at the end)

        Returns:
            The content of the generated token.ini file
        """
        # Clean up old token.ini before generating
        token_ini_path = self._token_output_dir / "token.ini"
        if token_ini_path.exists():
            try:
                token_ini_path.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete old token.ini: {e}")

        # Build stdin: account number + newline, then token_req + newline
        stdin_data = f"{account_number}\n{token_req}\n"

        logger.info(f"Starting DenuvoTicket.exe (account #{account_number})...")

        try:
            working_dir = self._activator_path.parent

            result = subprocess.run(
                [str(self._activator_path)],
                input=stdin_data,
                capture_output=True,
                text=True,
                timeout=self._process_timeout,
                cwd=str(working_dir),
                env={**os.environ},
            )

            stdout = result.stdout or ""
            stderr = result.stderr or ""

            logger.debug(f"DenuvoTicket stdout:\n{stdout}")
            if stderr:
                logger.debug(f"DenuvoTicket stderr:\n{stderr}")

            # Check for authentication failure
            if "Authentication failed" in stdout or "Authentication failed" in stderr:
                raise DenuvoWorkerError("Authentication failed — account credentials may be invalid")

            if "error" in stdout.lower() and "successful" not in stdout.lower():
                raise DenuvoWorkerError(f"DenuvoTicket reported an error:\n{stdout}")

        except subprocess.TimeoutExpired:
            raise DenuvoWorkerError(
                f"DenuvoTicket.exe timed out after {self._process_timeout}s"
            )
        except FileNotFoundError:
            raise DenuvoWorkerError(
                f"DenuvoTicket.exe not found at {self._activator_path}"
            )

        # Wait briefly for file to be written
        for _ in range(10):
            if token_ini_path.exists() and token_ini_path.stat().st_size > 0:
                break
            time.sleep(0.5)

        # Read the generated token.ini
        if not token_ini_path.exists():
            raise DenuvoWorkerError(
                f"token.ini was not generated. DenuvoTicket output:\n{stdout}"
            )

        content = token_ini_path.read_text(encoding="utf-8")
        if not content.strip():
            raise DenuvoWorkerError("token.ini was generated but is empty")

        logger.info("token.ini generated successfully")
        return content
