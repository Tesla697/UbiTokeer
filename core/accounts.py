import json
import logging
from pathlib import Path

logger = logging.getLogger("ubitokeer")

ACCOUNTS_PATH = Path(__file__).parent.parent / "accounts.json"


def read_accounts() -> list[dict]:
    """Load accounts from accounts.json."""
    if not ACCOUNTS_PATH.exists():
        logger.error("accounts.json not found")
        return []
    try:
        data = json.loads(ACCOUNTS_PATH.read_text())
        return data.get("accounts", [])
    except Exception as e:
        logger.error(f"Failed to load accounts.json: {e}")
        return []


def get_account_for_uplay_id(uplay_id: str, quota) -> dict | None:
    """Find an account that has the given uplay_id and has quota remaining."""
    accounts = read_accounts()
    for acc in accounts:
        if uplay_id in acc.get("uplay_ids", []):
            if quota.can_generate(acc["email"], uplay_id):
                return acc
    return None


def get_accounts_for_uplay_id(uplay_id: str) -> list[dict]:
    """Get all accounts assigned to a uplay_id (regardless of quota)."""
    accounts = read_accounts()
    return [acc for acc in accounts if uplay_id in acc.get("uplay_ids", [])]


def has_any_account_for_uplay_id(uplay_id: str) -> bool:
    """Check if any account is assigned to this uplay_id."""
    return len(get_accounts_for_uplay_id(uplay_id)) > 0
