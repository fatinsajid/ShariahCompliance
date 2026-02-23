# messaging/supabase_to_redis.py
import os
import json
from supabase import create_client
import redis
from dotenv import load_dotenv

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
# ----------------------------
# Connect to Supabase
# ----------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------------------
# Connect to Redis
# ----------------------------
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------
# Fetch unprocessed events
# ----------------------------
def fetch_unprocessed_events():
    response = (
        supabase
        .table("events")
        .select("*")
        .eq("processed", False)
        .order("created_at", desc=False)
        .execute()
    )

    # Corrected for supabase-py v2+
    data, count = supabase.table("events").select("*") \
        .eq("processed", False) \
        .order("created_at", desc=False) \
        .execute()

    if not data:
        print("⚠️ No unprocessed events found")
        return []

    return data

    return response.data
# ----------------------------
# Push events to Redis
# ----------------------------
def push_to_redis(event_dict):
    """Push a single event dict to Redis stream."""
    event_id = event_dict.get("id", "*")
    # Flatten values to strings for Redis
    flat_event = {k: str(v) for k, v in event_dict.items()}
    redis_client.xadd(STREAM_NAME, flat_event, id=event_id)
# ----------------------------
# Mark event as processed
# ----------------------------
def mark_as_processed(event_id):
    res = (
        supabase
        .table("events")
        .update({"processed": True})
        .eq("id", event_id)
        .execute()
    )
    if res.error:
        print(f"⚠️ Failed to mark {event_id} as processed: {res.error}")

# ----------------------------
# Main loop
# ----------------------------
def main():
    from supabase import create_client
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("🚀 Starting Supabase → Redis bridge...")

    response = supabase.table("events").select("*").execute()
    if response.data is None:
        print("⚠️ No events found")
        return

    events = response.data
    for event in events:
        # If event is a string, parse it
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except Exception as e:
                print(f"❌ Failed to parse event string: {e}")
                continue

        if not isinstance(event, dict):
            print(f"❌ Skipping invalid event: {event}")
            continue

        try:
            push_to_redis(event)
            print(f"✅ Event published: {event.get('id', '<unknown>')}")
        except Exception as e:
            print(f"❌ Failed to process event {event.get('id', '<unknown>')}: {e}")


if __name__ == "__main__":
    main()