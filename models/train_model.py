import os
import joblib
import json
from datetime import datetime
import pandas as pd
from dal import db_connector
from sklearn.ensemble import RandomForestClassifier

# -------------------------------
# Paths
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
model_dir = os.path.join(BASE_DIR, "models")
os.makedirs(model_dir, exist_ok=True)

MODEL_VERSION = "v1"
model_path = os.path.join(model_dir, f"risk_model_{MODEL_VERSION}.pkl")
metadata_path = os.path.join(model_dir, f"risk_model_{MODEL_VERSION}_metadata.json")

# -------------------------------
# Step 0: Initialize DB tables
# -------------------------------
db_connector.initialize_tables()

# -------------------------------
# Step 1: Populate ML features
# -------------------------------
db_connector.populate_features()

# -------------------------------
# Step 2: Fetch ML features
# -------------------------------
features_list = db_connector.fetch_features()
if not features_list:
    raise ValueError("❌ No company data available in local DB to train or predict.")

df = pd.DataFrame(features_list)
X = df.drop(columns=["company_id"], errors="ignore")
# Push ML features to Supabase
try:
    db_connector.push_features_to_supabase()
except Exception as e:
    print(f"⚠️ Could not push to Supabase: {e}")
# -------------------------------
# Step 3: Load or Train Model
# -------------------------------
if os.path.exists(model_path):
    model = joblib.load(model_path)
    print(f"✅ Loaded existing model: {model_path}")
else:
    # Train with dummy labels for placeholder
    y = [0, 1] * (len(X) // 2) + [0] * (len(X) % 2)  # at least 2 classes
    model = RandomForestClassifier(n_estimators=200, random_state=42)
    model.fit(X, y)
    joblib.dump(model, model_path)
    print(f"✅ New model trained and saved: {model_path}")

# -------------------------------
# Step 4: Generate Risk Scores (Safe)
# -------------------------------
try:
    if hasattr(model, "classes_") and len(model.classes_) > 1:
        risk_scores = model.predict_proba(X)[:, 1]
    else:
        print("⚠️ Model trained with single class or no predict_proba. Using predict() as risk proxy.")
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
    db_connector.save_result(row["company_id"], status, violations)

print("✅ Compliance results saved.")

# -------------------------------
# Step 6: Save Model Metadata
# -------------------------------
metadata = {
    "model_version": MODEL_VERSION,
    "trained_at": datetime.now().isoformat(),
    "model_type": type(model).__name__,
    "features": list(X.columns),
    "training_rows": len(X)
}

with open(metadata_path, "w") as f:
    json.dump(metadata, f, indent=2)

print(f"✅ Metadata saved to: {metadata_path}")
