# messaging/supabase_to_redis.py

import os
import json
import redis
import logging
from dotenv import load_dotenv
from supabase import create_client

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("supabase-redis-bridge")

# --------------------------------------------------
# Load env
# --------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_EVENTS_TABLE", "events")

REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

# --------------------------------------------------
# Clients
# --------------------------------------------------
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("❌ Supabase credentials missing")

if not REDIS_URL:
    raise RuntimeError("❌ REDIS_URL missing")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# --------------------------------------------------
# Fetch unprocessed events
# --------------------------------------------------
def fetch_unprocessed_events():
    try:
        response = (
            supabase
            .table(SUPABASE_TABLE)
            .select("*")
            .eq("processed", False)
            .order("created_at")
            .limit(100)
            .execute()
        )

        data = response.data or []

        if not isinstance(data, list):
            logger.warning("⚠️ Unexpected Supabase response format")
            return []

        logger.info(f"📥 Fetched {len(data)} unprocessed events")
        return data

    except Exception as e:
        logger.error(f"❌ Supabase fetch failed: {e}")
        return []


# --------------------------------------------------
# Push one event to Redis
# --------------------------------------------------
def push_to_redis(event: dict):
    try:
        event_id = event.get("id")
        tenant_id = event.get("tenant_id")
        event_type = event.get("event_type")
        payload = event.get("payload")

        if not event_id:
            raise ValueError("Event missing id")

        # normalize payload
        if isinstance(payload, str):
            payload_json = payload
        else:
            payload_json = json.dumps(payload)

        message = {
            "event_id": str(event_id),
            "tenant_id": str(tenant_id),
            "event_type": event_type,
            "payload": payload_json,
        }

        redis_id = redis_client.xadd(STREAM_NAME, message)

        logger.info(
            f"✅ Pushed event {event_id} → Redis stream ({redis_id})"
        )

        return True

    except Exception as e:
        logger.error(f"❌ Redis push failed for event {event}: {e}")
        return False


# --------------------------------------------------
# Mark processed in Supabase
# --------------------------------------------------
def mark_processed(event_id: str):
    try:
        supabase.table(SUPABASE_TABLE).update(
            {"processed": True}
        ).eq("id", event_id).execute()

        logger.info(f"✅ Marked processed: {event_id}")

    except Exception as e:
        logger.error(f"❌ Failed to mark processed {event_id}: {e}")


# --------------------------------------------------
# Main loop
# --------------------------------------------------
def main():
    logger.info("🚀 Starting Supabase → Redis bridge...")

    events = fetch_unprocessed_events()

    if not events:
        logger.info("⚠️ No new events to process")
        return

    success_count = 0

    for event in events:
        try:
            ok = push_to_redis(event)
            if ok:
                mark_processed(event["id"])
                success_count += 1

        except Exception as e:
            logger.error(f"❌ Failed processing event: {e}")

    logger.info(f"🏁 Bridge finished. Success: {success_count}/{len(events)}")


# --------------------------------------------------
if __name__ == "__main__":
    main()