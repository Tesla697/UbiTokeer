import logging
import threading
import time
from pathlib import Path

import winpty

logger = logging.getLogger("ubitokeer")


class DenuvoWorkerError(Exception):
    pass


class DenuvoWorker:
    def __init__(self, activator_path: str, token_output_dir: str, process_timeout: int = 90):
        act_path = Path(activator_path).resolve()
        if act_path.is_dir():
            self._activator_path = act_path / "DenuvoTicket.exe"
        else:
            self._activator_path = act_path
        self._token_output_dir = Path(token_output_dir).resolve()
        self._process_timeout = process_timeout

        if not self._activator_path.exists():
            raise FileNotFoundError(f"DenuvoTicket.exe not found at {self._activator_path}")

    def generate_token(self, account_number: int, token_req: str) -> dict:
        """
        Run DenuvoTicket.exe via ConPTY, send account + token_req, wait for token.ini.
        Returns dict with 'token_ini' (file content) and 'console_output' (full PTY output).
        """
        token_ini_path = self._token_output_dir / "token.ini"
        if token_ini_path.exists():
            try:
                token_ini_path.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete old token.ini: {e}")

        logger.info(f"Starting DenuvoTicket.exe (account #{account_number})...")

        working_dir = str(self._activator_path.parent)
        collected_output = []

        try:
            pty = winpty.PtyProcess.spawn(
                str(self._activator_path),
                cwd=working_dir,
            )

            # Background thread to continuously read output (prevents blocking)
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

            # Step 1: Wait for the menu to load
            time.sleep(3)
            logger.debug("Sending account number...")
            pty.write(f"{account_number}\r\n")

            # Step 2: Wait for authentication and token_req prompt
            time.sleep(8)
            logger.debug("Sending token_req...")
            pty.write(f"{token_req}\r\n")

            # Step 3: Wait for token.ini to appear (or process to exit / timeout)
            while time.time() < deadline:
                if token_ini_path.exists() and token_ini_path.stat().st_size > 0:
                    logger.info("token.ini detected on disk")
                    break
                if not pty.isalive():
                    logger.debug("DenuvoTicket process exited")
                    break
                time.sleep(1)

            # Give it a moment to finish writing
            time.sleep(1)

            # Stop the reader thread
            stop_event.set()

            full_output = "".join(collected_output)

            if "Authentication failed" in full_output:
                raise DenuvoWorkerError("Authentication failed — account credentials may be invalid")

            # Kill if still running
            if pty.isalive():
                try:
                    pty.terminate()
                except Exception:
                    pass

        except DenuvoWorkerError:
            raise
        except Exception as e:
            raise DenuvoWorkerError(f"Failed to run DenuvoTicket.exe: {e}")

        # Final check for token.ini
        for _ in range(10):
            if token_ini_path.exists() and token_ini_path.stat().st_size > 0:
                break
            time.sleep(0.5)

        if not token_ini_path.exists():
            raise DenuvoWorkerError(
                f"token.ini was not generated. DenuvoTicket output:\n{''.join(collected_output)}"
            )

        content = token_ini_path.read_text(encoding="utf-8")
        if not content.strip():
            raise DenuvoWorkerError("token.ini was generated but is empty")

        logger.info("token.ini generated successfully")
        return {
            "token_ini": content,
            "console_output": "".join(collected_output),
        }
