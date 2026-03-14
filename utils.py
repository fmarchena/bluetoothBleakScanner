from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """Returns current UTC time as an ISO-8601 string (second precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


_JSON_NATIVE = (str, int, float, bool, type(None))


def to_json_safe(value: Any) -> Any:
    """Recursively converts types that are not JSON-serializable to safe equivalents."""
    if isinstance(value, _JSON_NATIVE):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(v) for v in value]
    # Fallback: stringify any platform-specific or unknown object
    try:
        return str(value)
    except Exception:
        return None
