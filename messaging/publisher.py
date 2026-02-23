import os
import json
import uuid
import redis
from dotenv import load_dotenv
from dal.db_connector import fetch_companies

load_dotenv()

# ----------------------------
# Redis configuration
# ----------------------------
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")
GROUP_NAME = os.getenv("QUEUE_CONSUMER_GROUP", "shariah-workers")
CONSUMER_NAME = os.getenv("QUEUE_CONSUMER_NAME", "publisher-1")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------
# Publish compliance events
# ----------------------------
def publish_compliance_events():
    tenant_id = str(uuid.uuid4())  # ✅ Dynamic, valid UUID
    print("Using tenant_id:", tenant_id)

    companies = fetch_companies(tenant_id)
    if not companies:
        print("⚠️ No companies to publish")
        return

    for company in companies:
        event = {
            "event_type": "compliance_check",
            "tenant_id": tenant_id,
            "payload": json.dumps(company)
        }
        message_id = redis_client.xadd(STREAM_NAME, event)
        print(f"✅ Event published: {message_id} for company {company['company_id']}")

if __name__ == "__main__":
    publish_compliance_events()