"""
UbiTokeer Backend
Entry point — starts the FastAPI server and CustomTkinter GUI.
"""

import json
import logging
import sys
import threading
from pathlib import Path

import uvicorn

from core.job_queue import JobQueue
from gui.app import UbiTokeerApp
from server import api as server_api

CONFIG_PATH = Path(__file__).parent / "config.json"

logger = logging.getLogger("ubitokeer")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except Exception as e:
            logger.error(f"Failed to load config.json: {e}")
    return {
        "port": 8090,
        "activator_path": "activator",
        "token_output_dir": "activator/token",
        "daily_limit": 5,
        "process_timeout": 60,
    }


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(gui_handler: logging.Handler) -> None:
    root = logging.getLogger("ubitokeer")
    root.setLevel(logging.DEBUG)

    # CMD / stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%H:%M:%S")
    )
    root.addHandler(stdout_handler)

    # File handler
    log_path = Path(__file__).parent / "ubitokeer.log"
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(file_handler)

    # GUI handler
    gui_handler.setLevel(logging.DEBUG)
    root.addHandler(gui_handler)

    # Suppress noisy uvicorn logs
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").propagate = False


# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------

class ServerManager:
    def __init__(self, config: dict):
        self._config = config
        self._server_thread: threading.Thread | None = None
        self._uvicorn_server: uvicorn.Server | None = None

    def start(self) -> None:
        if self._server_thread and self._server_thread.is_alive():
            logger.warning("Server is already running")
            return

        port = self._config.get("port", 8090)
        uv_config = uvicorn.Config(
            app=server_api.app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
        self._uvicorn_server = uvicorn.Server(uv_config)

        self._server_thread = threading.Thread(
            target=self._uvicorn_server.run, daemon=True
        )
        self._server_thread.start()
        logger.info(f"API server started on 0.0.0.0:{port}")

    def stop(self) -> None:
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
            logger.info("API server stopped")

    def update_config(self, config: dict) -> None:
        self._config = config


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()

    # Build GUI first so we can grab its log handler
    app = UbiTokeerApp(
        config=config,
        on_save_config=lambda cfg: _on_save_config(cfg, job_queue, server_mgr, app),
        on_toggle_server=lambda running: _on_toggle_server(running, server_mgr),
    )

    setup_logging(app.get_log_handler())
    logger.info("UbiTokeer starting up...")

    # Job queue
    job_queue = JobQueue(
        config=config,
        on_update=lambda: app.after(0, lambda: app.update_queue_state(job_queue.get_state())),
    )

    # Give GUI access to quota tracker
    app.set_quota_tracker(job_queue._quota)

    # Server manager
    server_mgr = ServerManager(config)
    server_api.set_queue(job_queue)

    # Auto-start server
    server_mgr.start()
    app.set_server_running(True)

    logger.info("UbiTokeer ready")

    # Run GUI (blocks until window is closed)
    app.mainloop()

    # Cleanup
    server_mgr.stop()
    job_queue.shutdown()
    logger.info("UbiTokeer shut down")


def _on_save_config(
    new_config: dict,
    job_queue: JobQueue,
    server_mgr: ServerManager,
    app: UbiTokeerApp,
) -> None:
    job_queue.update_config(new_config)
    server_mgr.update_config(new_config)


def _on_toggle_server(running: bool, server_mgr: ServerManager) -> None:
    if running:
        server_mgr.start()
    else:
        server_mgr.stop()


if __name__ == "__main__":
    main()
