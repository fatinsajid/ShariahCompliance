import os
import json
import redis
from dotenv import load_dotenv
from schema_registry.validator import validate_event

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

# Singleton Redis client
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def publish_event(event_type: str, payload: dict, version: str = "v1"):
    """
    Publish event to Redis Stream with schema validation
    """

    # ✅ validate BEFORE publishing
    validate_event(event_type, payload, version)

    message = {
        "event_type": event_type,
        "schema_version": version,
        "payload": json.dumps(payload)
    }

    message_id = redis_client.xadd(STREAM_NAME, message)

    return message_id