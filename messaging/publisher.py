# messaging/publisher.py
import os
import json
import redis
from dotenv import load_dotenv
from supabase import create_client
from dal.db_connector import fetch_companies

load_dotenv()

# ----------------------------
# Environment
# ----------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = os.getenv("QUEUE_STREAM_NAME", "shariah-events")

# ----------------------------
# Initialize Redis
# ----------------------------
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------
# Initialize Supabase client
# ----------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ----------------------------
# Fetch tenants dynamically from Supabase
# ----------------------------
def fetch_all_tenants():
    """
    Fetch all tenants from Supabase.
    Assumes table 'tenants' with UUID column 'tenant_id'.
    """
    response = supabase.table("tenants").select("tenant_id").execute()
    if not response.data:
        print("⚠️ No tenants found in Supabase.")
        return []
    # ensure proper UUID strings
    return [t["tenant_id"] for t in response.data]

# ----------------------------
# Publish compliance events to Redis
# ----------------------------
def publish_compliance_events():
    tenants = fetch_all_tenants()
    if not tenants:
        return

    for tenant_id in tenants:
        try:
            companies = fetch_companies(tenant_id)
            if not companies:
                print(f"⚠️ No companies for tenant {tenant_id}")
                continue

            for company in companies:
                event = {
                    "tenant_id": tenant_id,
                    "company_id": company["company_id"],
                    "event_type": "compliance_check",
                    "payload": json.dumps(company)
                }
                # push to Redis stream
                event_id = redis_client.xadd(STREAM_NAME, event)
                print(f"✅ Event published: {event_id} for company {company['company_id']}")

        except Exception as e:
            print(f"❌ Failed to publish for tenant {tenant_id}: {e}")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    print("🚀 Starting publisher...")
    publish_compliance_events()