# messaging/worker.py
import os
import json
import redis
from dotenv import load_dotenv
from dal.db_connector import save_company, save_result, populate_features
from services.compliance_engine import check_shariah_compliance

load_dotenv()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
THRESHOLDS_PATH = os.path.join(BASE_DIR, "config", "shariah_thresholds.json")

with open(THRESHOLDS_PATH, "r") as f:
    THRESHOLDS = json.load(f)

print("✅ Shariah thresholds loaded")
# ----------------------------
# Environment
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")
GROUP_NAME = os.getenv("QUEUE_CONSUMER_GROUP", "shariah-workers")
CONSUMER_NAME = os.getenv("QUEUE_CONSUMER_NAME", "worker-1")

# ----------------------------
# Initialize Redis
# ----------------------------
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# ----------------------------
# Ensure consumer group exists
# ----------------------------
def ensure_group():
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"✅ Consumer group '{GROUP_NAME}' created.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        print(f"⚠️ Consumer group '{GROUP_NAME}' already exists.")


# ----------------------------
# Process a single event
# ----------------------------
def process_event(event: dict):
    tenant_id = event.get("tenant_id")
    company_id = event.get("company_id")
    payload_str = event.get("payload")

    if not all([tenant_id, company_id, payload_str]):
        print(f"⚠️ Event missing required fields: {event}")
        return

    try:
        company = json.loads(payload_str)

        # Run compliance check
        status, violations = check_shariah_compliance(company, THRESHOLDS)

        # Save results to DB
        save_company(company, tenant_id)
        save_result(company_id, tenant_id, status, violations)

        # Update ML features
        populate_features(tenant_id)

        print(f"✅ Processed compliance for company {company_id}, tenant {tenant_id}")

    except Exception as e:
        print(f"❌ Worker failed for company {company_id}, tenant {tenant_id}: {e}")


# ----------------------------
# Main loop
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
                    # Convert all values from Redis (they come as strings) to proper dict
                    event = {k: v for k, v in message.items()}
                    process_event(event)
                    # acknowledge message
                    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                except Exception as e:
                    print(f"❌ Worker error on message {message_id}: {e}")


if __name__ == "__main__":
    start_worker()