import os
import json
import redis
from dotenv import load_dotenv
from dal.db_connector import insert_compliance_record

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")
GROUP_NAME = os.getenv("QUEUE_CONSUMER_GROUP", "shariah-workers")
CONSUMER_NAME = os.getenv("QUEUE_CONSUMER_NAME", "worker-1")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def ensure_group():
    """
    Create consumer group if not exists
    """
    try:
        redis_client.xgroup_create(
            STREAM_NAME,
            GROUP_NAME,
            id="0",
            mkstream=True
        )
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def process_event(event_type: str, payload: dict):
    """
    Route events to handlers
    """
    if event_type == "compliance_check":
        insert_compliance_record(payload)

    elif event_type == "risk_score":
        # future ML hook
        pass

    else:
        print(f"⚠️ Unknown event type: {event_type}")


def start_worker():
    print("🚀 Worker started...")
    ensure_group()

    while True:
        response = redis_client.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=10,
            block=5000
        )

        if not response:
            continue

        for stream, messages in response:
            for message_id, message in messages:
                try:
                    event_type = message["event_type"]
                    payload = json.loads(message["payload"])

                    process_event(event_type, payload)

                    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

                except Exception as e:
                    print(f"❌ Worker error: {e}")