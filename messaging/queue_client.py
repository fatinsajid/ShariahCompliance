import os
import json
import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

# Singleton Redis client
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def publish_event(event_type: str, payload: dict):
    """
    Publish event to Redis Stream
    """
    message = {
        "event_type": event_type,
        "payload": json.dumps(payload)
    }

    message_id = redis_client.xadd(STREAM_NAME, message)

    return message_id