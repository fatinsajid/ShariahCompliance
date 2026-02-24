import os
import json
import redis
from dotenv import load_dotenv
from dal.db_connector import fetch_companies

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def publish_compliance_events(tenant_id: str) -> int:
    """
    Publish compliance events for all companies of a tenant.
    Returns number of events published.
    """
    companies = fetch_companies(tenant_id)

    if not companies:
        return 0

    published = 0

    for company in companies:
        event_payload = {
            "event_type": "compliance_check",
            "schema_version": "v1",
            "tenant_id": tenant_id,
            "company_id": company["company_id"],
            "status": "Pending",
            "violations": []
        }

        redis_client.xadd(
            STREAM_NAME,
            {"payload": json.dumps(event_payload)},
            maxlen=1000
        )

        published += 1

    return published