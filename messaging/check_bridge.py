import os
import redis
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Environment
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_EVENTS_TABLE", "events")

# ----------------------------
# Redis Client
# ----------------------------
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------
# Supabase Client
# ----------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_supabase():
    print("🔹 Fetching events from Supabase...")
    response = supabase.table(SUPABASE_TABLE).select("*").execute()
    events = response.data
    print(f"✅ {len(events)} events found in Supabase")
    for e in events[-5:]:  # last 5 events
        print(e)
    return events

def check_redis():
    print("🔹 Fetching events from Redis...")
    messages = redis_client.xrevrange(STREAM_NAME, max="+", min="-", count=5)
    print(f"✅ {len(messages)} events found in Redis")
    for msg_id, fields in messages:
        print(f"ID: {msg_id}, Data: {fields}")
    return messages

def main():
    supabase_events = check_supabase()
    redis_events = check_redis()

    if supabase_events and redis_events:
        print("✅ Supabase → Redis bridge seems operational")
    else:
        print("⚠️ Bridge may not be fully working. Check logs")

if __name__ == "__main__":
    main()