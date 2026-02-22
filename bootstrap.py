import os
import uuid
import logging
from dal import db_connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------
# Step 0: Initialize DB tables
# -------------------------------
db_connector.initialize_tables()
logger.info("✅ Tables initialized")

# -------------------------------
# Step 1: Determine tenant
# -------------------------------
TENANT_NAME = "local_dev"

# If you have a SUPABASE_USER_ID in env, use it; else generate a UUID
TENANT_ID = os.getenv("DEFAULT_TENANT_ID")
if not TENANT_ID:
    TENANT_ID = db_connector.create_tenant(name=TENANT_NAME)

logger.info(f"✅ Tenant created or exists: {TENANT_ID} ({TENANT_NAME})")

# -------------------------------
# Step 2: Determine user
# -------------------------------
USER_ID = os.getenv("BOOTSTRAP_USER_ID")
if not USER_ID:
    USER_ID = str(uuid.uuid4())
    logger.info(f"Generated temporary user ID for bootstrap: {USER_ID}")

# -------------------------------
# Step 3: Ensure user ↔ tenant mapping
# -------------------------------
db_connector.ensure_user_tenant(USER_ID, TENANT_ID)
logger.info(f"✅ User {USER_ID} assigned to tenant {TENANT_ID}")

# -------------------------------
# Step 4: Populate ML features for this tenant
# -------------------------------
db_connector.populate_features(TENANT_ID)
logger.info("✅ ML features populated")

# -------------------------------
# Step 5: Fetch features (optional check)
# -------------------------------
features_list = db_connector.fetch_features(TENANT_ID)
if not features_list:
    logger.warning(f"⚠️ No companies found for tenant {TENANT_ID}")
else:
    logger.info(f"✅ Fetched {len(features_list)} companies for tenant {TENANT_ID}")

# -------------------------------
# Step 6: Ready message
# -------------------------------
logger.info("Bootstrap complete. System ready for training or API usage.")