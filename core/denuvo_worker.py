import logging
import time
from pathlib import Path

import winpty

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
        Run DenuvoTicket.exe via a Windows pseudo-console (ConPTY) so the .NET app
        has a real console handle for its interactive menu.

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

        logger.info(f"Starting DenuvoTicket.exe (account #{account_number})...")

        working_dir = str(self._activator_path.parent)
        output_lines = []

        try:
            # Spawn inside a real pseudo-console via pywinpty
            pty = winpty.PtyProcess.spawn(
                f'"{self._activator_path}"',
                cwd=working_dir,
            )

            deadline = time.time() + self._process_timeout

            # Wait for the menu to appear, then send account number
            time.sleep(2)
            self._read_available(pty, output_lines)
            logger.debug(f"Pre-account output: {''.join(output_lines)}")

            pty.write(f"{account_number}\r\n")
            logger.debug(f"Sent account number: {account_number}")

            # Wait for authentication + prompt for token_req
            time.sleep(5)
            self._read_available(pty, output_lines)
            logger.debug(f"Post-account output: {''.join(output_lines[-20:])}")

            # Send the token request
            pty.write(f"{token_req}\r\n")
            logger.debug("Sent token_req data")

            # Wait for processing to complete
            while pty.isalive() and time.time() < deadline:
                time.sleep(1)
                self._read_available(pty, output_lines)

            # Final read
            self._read_available(pty, output_lines)

            full_output = "".join(output_lines)
            logger.debug(f"DenuvoTicket full output:\n{full_output}")

            # Check for errors
            if "Authentication failed" in full_output:
                raise DenuvoWorkerError("Authentication failed — account credentials may be invalid")

            if "error" in full_output.lower() and "successful" not in full_output.lower():
                raise DenuvoWorkerError(f"DenuvoTicket reported an error:\n{full_output}")

            if pty.isalive():
                pty.terminate()

        except DenuvoWorkerError:
            raise
        except Exception as e:
            raise DenuvoWorkerError(f"Failed to run DenuvoTicket.exe: {e}")

        # Wait briefly for file to be written
        for _ in range(10):
            if token_ini_path.exists() and token_ini_path.stat().st_size > 0:
                break
            time.sleep(0.5)

        # Read the generated token.ini
        if not token_ini_path.exists():
            raise DenuvoWorkerError(
                f"token.ini was not generated. DenuvoTicket output:\n{''.join(output_lines)}"
            )

        content = token_ini_path.read_text(encoding="utf-8")
        if not content.strip():
            raise DenuvoWorkerError("token.ini was generated but is empty")

        logger.info("token.ini generated successfully")
        return content

    @staticmethod
    def _read_available(pty, output_lines: list, chunk_size: int = 4096) -> None:
        """Read all currently available output from the PTY."""
        try:
            while True:
                data = pty.read(chunk_size)
                if not data:
                    break
                output_lines.append(data)
        except EOFError:
            pass
        except Exception:
            pass
