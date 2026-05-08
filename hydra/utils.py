import re
from datetime import datetime


def create_slug(name: str, add_timestamp: bool = False) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip()).lower().strip("-")
    if add_timestamp:
        slug = f"{slug}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    return slug
