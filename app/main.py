# app/main.py
import os
import subprocess
import joblib
import pandas as pd
from fastapi import FastAPI
from dal import db_connector

app = FastAPI(title="Shariah Compliance API")

# ----------------------------
# 1️⃣ Paths
# ----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
TRAIN_SCRIPT = os.path.join(PROJECT_ROOT, "models", "train_model.py")

# ----------------------------
# 2️⃣ Ensure model exists
# ----------------------------
if not os.path.exists(MODEL_PATH):
    print("⚠️ Model not found. Training now...")
    subprocess.run(["python", TRAIN_SCRIPT], check=True)
    print("✅ Model trained.")

# ----------------------------
# 3️⃣ Load the trained model
# ----------------------------
model = joblib.load(MODEL_PATH)
print("✅ Model loaded successfully.")

# ----------------------------
# 4️⃣ Predict Endpoint
# ----------------------------
@app.get("/predict/{company_id}")
def predict(company_id: str):
    try:
        # Fetch all companies
        companies = db_connector.fetch_companies()
        company = next((c for c in companies if c["company_id"] == company_id), None)

        if not company:
            return {"error": f"Company {company_id} not found."}

        # Prepare features
        X = pd.DataFrame([{
            "total_assets": company["total_assets"],
            "total_debt": company["total_debt"],
            "total_income": company["total_income"],
            "non_halal_income": company["non_halal_income"],
            "cash_and_interest_securities": company["cash_and_interest_securities"]
        }])

        # Predict risk
        if hasattr(model, "predict_proba"):
            risk_score = model.predict_proba(X)[:, 1][0]
        else:
            risk_score = model.predict(X)[0]

        return {"company_id": company_id, "risk_score": float(risk_score)}

    except Exception as e:
        return {"error": str(e)}

# ----------------------------
# 5️⃣ Health Check
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}
