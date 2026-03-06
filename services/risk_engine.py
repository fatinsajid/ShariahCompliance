# services/risk_engine.py

import os
import joblib
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")

# Load model at module level
try:
    model = joblib.load(MODEL_PATH)
    print("✅ ML risk model loaded successfully")
except Exception as e:
    model = None
    print(f"❌ Failed to load ML model: {e}")


def predict_risk_score(company: dict) -> float:
    """
    Returns the ML-based risk score for a single company dictionary.
    """
    if model is None:
        raise ValueError("ML model is not loaded")

    X = pd.DataFrame([{
        "total_assets": company.get("total_assets", 0),
        "total_debt": company.get("total_debt", 0),
        "total_income": company.get("total_income", 0),
        "non_halal_income": company.get("non_halal_income", 0),
        "cash_and_interest_securities": company.get("cash_and_interest_securities", 0)
    }])

    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(X)
        risk_score = probs[0][1] if probs.shape[1] > 1 else probs[0][0]
    else:
        risk_score = model.predict(X)[0]

    return float(risk_score)