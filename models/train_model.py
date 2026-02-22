import os
import joblib
import json
from datetime import datetime
import pandas as pd
from dal import db_connector
from sklearn.ensemble import RandomForestClassifier

# -------------------------------
# Paths & Model Setup
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
model_dir = os.path.join(BASE_DIR, "models")
os.makedirs(model_dir, exist_ok=True)

MODEL_VERSION = "v1"

# -------------------------------
# Step 0: Initialize DB Tables
# -------------------------------
db_connector.initialize_tables()

# -------------------------------
# Step 1: Detect tenants with companies
# -------------------------------
with db_connector.get_cursor() as cur:
    cur.execute("SELECT DISTINCT tenant_id FROM companies;")
    tenant_ids = [r[0] for r in cur.fetchall()]

if not tenant_ids:
    raise ValueError("❌ No tenant data found in DB. Please ensure companies exist.")

print(f"✅ Found tenants: {tenant_ids}")

# -------------------------------
# Step 2: Loop over tenants
# -------------------------------
for TENANT_ID in tenant_ids:
    print(f"\n🔹 Processing tenant: {TENANT_ID}")

    # Populate ML features (idempotent)
    db_connector.populate_features(TENANT_ID)

    # Fetch ML features
    features_list = db_connector.fetch_features(TENANT_ID)
    if not features_list:
        print(f"⚠️ No company features for tenant {TENANT_ID}, skipping...")
        continue

    df = pd.DataFrame(features_list)
    X = df.drop(columns=["company_id"], errors="ignore")

    # -------------------------------
    # Step 3: Load or Train Model
    # -------------------------------
    model_path = os.path.join(model_dir, f"risk_model_{MODEL_VERSION}_{TENANT_ID}.pkl")
    metadata_path = os.path.join(model_dir, f"risk_model_{MODEL_VERSION}_{TENANT_ID}_metadata.json")

    if os.path.exists(model_path):
        model = joblib.load(model_path)
        print(f"✅ Loaded existing model for tenant {TENANT_ID}: {model_path}")
    else:
        # Train with dummy labels for placeholder
        y = [0, 1] * (len(X) // 2) + [0] * (len(X) % 2)
        model = RandomForestClassifier(n_estimators=200, random_state=42)
        model.fit(X, y)
        joblib.dump(model, model_path)
        print(f"✅ New model trained and saved for tenant {TENANT_ID}: {model_path}")

    # -------------------------------
    # Step 4: Generate Risk Scores
    # -------------------------------
    try:
        if hasattr(model, "classes_") and len(model.classes_) > 1:
            risk_scores = model.predict_proba(X)[:, 1]
        else:
            print("⚠️ Single-class model, using predict() as proxy.")
            risk_scores = model.predict(X)
    except AttributeError:
        print("⚠️ Model has no predict_proba. Using predict() instead.")
        risk_scores = model.predict(X)

    df["risk_score"] = risk_scores

    # -------------------------------
    # Step 5: Save Compliance Results
    # -------------------------------
    for idx, row in df.iterrows():
        status = "Non-Compliant" if row["risk_score"] > 0.5 else "Compliant"
        violations = [] if status == "Compliant" else ["High risk score"]
        db_connector.save_result(row["company_id"], TENANT_ID, status, violations)

    print(f"✅ Compliance results saved for tenant {TENANT_ID}")

    # -------------------------------
    # Step 6: Save Model Metadata
    # -------------------------------
    metadata = {
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now().isoformat(),
        "model_type": type(model).__name__,
        "features": list(X.columns),
        "training_rows": len(X),
        "tenant_id": TENANT_ID
    }

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"✅ Metadata saved for tenant {TENANT_ID}: {metadata_path}")

print("\n🎉 All tenants processed successfully.")