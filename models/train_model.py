import os
import json
from datetime import datetime
import joblib

# --- Resolve project root ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
model_dir = os.path.join(BASE_DIR, "models")
os.makedirs(model_dir, exist_ok=True)

# --- Version (manual for now) ---
MODEL_VERSION = "v1"

# --- Paths ---
model_filename = f"risk_model_{MODEL_VERSION}.pkl"
metadata_filename = f"risk_model_{MODEL_VERSION}_metadata.json"

model_path = os.path.join(model_dir, model_filename)
metadata_path = os.path.join(model_dir, metadata_filename)

# --- Save model ---
joblib.dump(model, model_path)

# --- Build metadata ---
metadata = {
    "model_version": MODEL_VERSION,
    "trained_at": datetime.utcnow().isoformat(),
    "model_type": type(model).__name__,
    "features": list(X.columns),  # assumes pandas DataFrame
    "training_rows": len(X)
}

# --- Save metadata ---
with open(metadata_path, "w") as f:
    json.dump(metadata, f, indent=2)

print(f"✅ Model saved to: {model_path}")
print(f"✅ Metadata saved to: {metadata_path}")
