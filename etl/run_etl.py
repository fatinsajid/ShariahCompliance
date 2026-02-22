# etl/run_etl.py
import os
from dal.db_connector import (
    fetch_all_tenants,
    fetch_companies,
    populate_features,
    push_features_to_supabase
)

def run_etl():
    """
    Multi-tenant ETL pipeline:
    1. Fetch tenants
    2. Fetch companies per tenant
    3. Populate ML-ready features
    4. Push features to Supabase
    """
    print("🔹 Starting ETL pipeline...")

    tenants = fetch_all_tenants()
    if not tenants:
        print("⚠️ No tenants found. ETL skipped.")
        return

    for tenant_id in tenants:
        print(f"🔹 Processing tenant: {tenant_id}")

        # Fetch companies for this tenant
        companies = fetch_companies(tenant_id)
        if not companies:
            print(f"⚠️ No companies found for tenant {tenant_id}. Skipping.")
            continue

        # Populate ML-ready features
        populate_features(tenant_id)

        # Push features to Supabase
        push_features_to_supabase(tenant_id)

    print("🎉 ETL pipeline completed successfully.")


if __name__ == "__main__":
    run_etl()