import os
import json
import redis
from dotenv import load_dotenv
from dal.db_connector import insert_compliance_record
from schema_registry.validator import validate_event

load_dotenv()

# ----------------------------
# Redis configuration
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")
GROUP_NAME = os.getenv("QUEUE_CONSUMER_GROUP", "shariah-workers")
CONSUMER_NAME = os.getenv("QUEUE_CONSUMER_NAME", "worker-1")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------
# Ensure consumer group exists
# ----------------------------
def ensure_group():
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print("✅ Consumer group created")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

# ----------------------------
# Event processing
# ----------------------------
def process_event(event_type: str, payload: dict):
    if event_type == "compliance_check":
        insert_compliance_record(payload)
    else:
        print(f"⚠️ Unknown event type: {event_type}")

# ----------------------------
# Worker loop
# ----------------------------
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
                    # ----------------------------
                    # Extract & validate event
                    # ----------------------------
                    event_type = message.get("event_type")
                    tenant_id = message.get("tenant_id")
                    payload_json = message.get("payload")

                    if not all([event_type, tenant_id, payload_json]):
                        raise ValueError("Missing required fields in message")

                    payload = json.loads(payload_json)

                    # Schema validation
                    validate_event(event_type, payload, version="v1")

                    # Process event
                    process_event(event_type, payload)

                    # Acknowledge message
                    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

                except Exception as e:
                    print(f"❌ Worker error on message {message_id}: {e}")

if __name__ == "__main__":
    start_worker()