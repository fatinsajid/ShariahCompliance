import json
import os
from functools import lru_cache


SCHEMA_DIR = os.path.join(
    os.path.dirname(__file__),
    "schemas"
)


@lru_cache(maxsize=32)
def load_schema(event_type: str, version: str = "v1") -> dict:
    """
    Load schema from disk (cached)
    """
    filename = f"{event_type}_{version}.json"
    path = os.path.join(SCHEMA_DIR, filename)

    if not os.path.exists(path):
        raise ValueError(f"Schema not found: {filename}")

    with open(path, "r") as f:
        return json.load(f)