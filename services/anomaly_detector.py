import os
import joblib
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "anomaly_model_v1.pkl")


# --------------------------------------------------
# Load anomaly model safely
# --------------------------------------------------
try:
    anomaly_model = joblib.load(MODEL_PATH)
    print("✅ Anomaly model loaded")
except Exception as e:
    anomaly_model = None
    print(f"⚠️ Anomaly model not available: {e}")


# --------------------------------------------------
# Feature builder
# --------------------------------------------------
def build_feature_vector(company: dict) -> pd.DataFrame:
    """
    Convert company financials into model features.
    Must match training schema.
    """

    return pd.DataFrame([{
        "total_assets": company.get("total_assets", 0),
        "total_debt": company.get("total_debt", 0),
        "total_income": company.get("total_income", 0),
        "non_halal_income": company.get("non_halal_income", 0),
        "cash_and_interest_securities": company.get(
            "cash_and_interest_securities", 0
        ),
    }])


# --------------------------------------------------
# Main anomaly function
# --------------------------------------------------
def detect_anomaly(company: dict) -> dict:
    """
    Returns anomaly flag + score.
    Safe for production and thesis.
    """

    if anomaly_model is None:
        return {
            "anomaly_flag": False,
            "anomaly_score": None,
            "message": "Anomaly model not loaded"
        }

    try:
        X = build_feature_vector(company)

        # IsolationForest style
        if hasattr(anomaly_model, "decision_function"):
            score = float(anomaly_model.decision_function(X)[0])
            pred = anomaly_model.predict(X)[0]  # -1 = anomaly
            is_anomaly = pred == -1

        # fallback generic
        else:
            pred = anomaly_model.predict(X)[0]
            is_anomaly = bool(pred)
            score = None

        return {
            "anomaly_flag": is_anomaly,
            "anomaly_score": score
        }

    except Exception as e:
        return {
            "anomaly_flag": False,
            "anomaly_score": None,
            "error": str(e)
        }
class AnomalyDetector:
    def __init__(self, model_path="models/anomaly_model.pkl"):
        self.model_path = model_path
        self.model = None

    def load(self):
        self.model = joblib.load(self.model_path)
        print("✅ Anomaly model loaded")

    def detect(self, company: dict) -> bool:
        X = [[
            company.get("total_assets", 0),
            company.get("total_debt", 0),
            company.get("non_halal_income", 0)
        ]]
        pred = self.model.predict(X)
        return bool(pred[0])