import os
import json
import redis
import logging
from dotenv import load_dotenv
from supabase import create_client

logger = logging.getLogger("event-publisher")

load_dotenv()  # ✅ ensure env is loaded

STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")


# --------------------------------------------------
# Lazy clients (CRITICAL FIX)
# --------------------------------------------------

_supabase = None
_redis = None


def get_supabase():
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise RuntimeError("SUPABASE_URL or SUPABASE_KEY not set")

        _supabase = create_client(url, key)

    return _supabase


def get_redis():
    global _redis
    if _redis is None:
        redis_url = os.getenv("REDIS_URL")

        if not redis_url:
            raise RuntimeError("REDIS_URL not set")

        _redis = redis.Redis.from_url(redis_url, decode_responses=True)

    return _redis


# --------------------------------------------------
# Publisher
# --------------------------------------------------

def publish_event(tenant_id: str, event_type: str, payload: dict):
    try:
        supabase = get_supabase()
        redis_client = get_redis()

        # 1️⃣ Insert into Supabase
        response = (
            supabase.table("events")
            .insert({
                "tenant_id": tenant_id,
                "event_type": event_type,
                "payload": payload,
                "processed": False,
            })
            .execute()
        )

        event = response.data[0]
        event_id = event["id"]

        # 2️⃣ Push to Redis
        redis_client.xadd(
            STREAM_NAME,
            {
                "event_id": event_id,
                "tenant_id": tenant_id,
                "event_type": event_type,
                "payload": json.dumps(payload),
            },
        )

        logger.info(f"✅ Event published: {event_id}")

    except Exception as e:
        logger.error(f"❌ Event publish failed: {e}")
        raise