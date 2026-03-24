"""
UbiTokeer Backend
Entry point — starts the FastAPI server for Ubisoft Denuvo token generation.
"""

import json
import logging
import sys
import threading
from pathlib import Path

import uvicorn

from core.job_queue import JobQueue
from server import api as server_api

CONFIG_PATH = Path(__file__).parent / "config.json"

logger = logging.getLogger("ubitokeer")


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except Exception as e:
            logger.error(f"Failed to load config.json: {e}")
    return {
        "port": 8090,
        "activator_path": "activator/DenuvoTicket.exe",
        "token_output_dir": "activator/token",
        "daily_limit": 5,
        "process_timeout": 60,
    }


def setup_logging() -> None:
    root = logging.getLogger("ubitokeer")
    root.setLevel(logging.DEBUG)

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

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").propagate = False


def main() -> None:
    setup_logging()
    config = load_config()

    logger.info("UbiTokeer starting up...")

    # Job queue
    queue = JobQueue(config=config)
    server_api.set_queue(queue)

    # Start API server
    port = config.get("port", 8090)
    logger.info(f"Starting API server on 0.0.0.0:{port}")

    try:
        uvicorn.run(
            server_api.app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
    except KeyboardInterrupt:
        pass
    finally:
        queue.shutdown()
        logger.info("UbiTokeer shut down")


if __name__ == "__main__":
    main()
