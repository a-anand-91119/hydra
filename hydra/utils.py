import re
from datetime import datetime
from typing import Any, Optional


def create_slug(name: str, add_timestamp: bool = False) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip()).lower().strip("-")
    if add_timestamp:
        slug = f"{slug}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    return slug


def safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
