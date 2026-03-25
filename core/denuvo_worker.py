import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

logger = logging.getLogger("ubitokeer")


class DenuvoWorkerError(Exception):
    pass


class DenuvoWorker:
    def __init__(self, activator_path: str, token_output_dir: str, process_timeout: int = 60):
        act_path = Path(activator_path).resolve()
        # If given a directory, look for DenuvoTicket.exe inside it
        if act_path.is_dir():
            self._activator_path = act_path / "DenuvoTicket.exe"
        else:
            self._activator_path = act_path
        self._token_output_dir = Path(token_output_dir).resolve()
        self._process_timeout = process_timeout

        if not self._activator_path.exists():
            raise FileNotFoundError(f"DenuvoTicket.exe not found at {self._activator_path}")

    def generate_token(self, account_number: int, token_req: str) -> str:
        """
        Run DenuvoTicket.exe, select account, feed token_req, return token.ini content.

        Uses cmd.exe to pipe a temp file into the exe so that the .NET app gets a
        real console handle (avoids System.IO.IOException on Console.CursorTop).

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

        working_dir = self._activator_path.parent
        stdout_path = working_dir / "_stdout.tmp"
        stderr_path = working_dir / "_stderr.tmp"

        # Write input to a temp file, then use cmd /c to pipe it in
        # This gives the .NET app a real console allocation via cmd.exe
        stdin_tmp = None
        try:
            stdin_tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", dir=str(working_dir),
                delete=False, encoding="utf-8",
            )
            stdin_tmp.write(stdin_data)
            stdin_tmp.close()

            # Use cmd /c with input redirection + output capture
            cmd = (
                f'cmd /c ""{self._activator_path}" < "{stdin_tmp.name}" '
                f'> "{stdout_path}" 2> "{stderr_path}""'
            )

            subprocess.run(
                cmd,
                shell=True,
                timeout=self._process_timeout,
                cwd=str(working_dir),
                env={**os.environ},
            )

            stdout = stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path.exists() else ""
            stderr = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""

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
        finally:
            # Clean up temp files
            for p in [stdin_tmp and Path(stdin_tmp.name), stdout_path, stderr_path]:
                if p and p.exists():
                    try:
                        p.unlink()
                    except Exception:
                        pass

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
