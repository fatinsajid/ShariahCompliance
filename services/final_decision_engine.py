# services/final_decision_engine.py

import pandas as pd
from dal.db_connector import save_result, populate_features
from services.compliance_engine import check_shariah_compliance
from services.explainability_engine import generate_explanation
from services.anomaly_detector import detect_anomaly
import os
import joblib
import json
import uuid
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
THRESHOLDS_PATH = os.path.join(PROJECT_ROOT, "config", "shariah_thresholds.json")

# Load thresholds once
with open(THRESHOLDS_PATH, "r") as f:
    THRESHOLDS = json.load(f)

# Load ML model once
try:
    model = joblib.load(MODEL_PATH)
    print("✅ ML model loaded")
except Exception as e:
    model = None
    print(f"❌ ML model failed to load: {e}")


class FinalDecisionEngine:
    def __init__(self, tenant_id: str):
        print("🚀 FinalDecisionEngine INIT VERSION 2")
        self.tenant_id = tenant_id

    # ✅ ADD THIS METHOD HERE (inside same class)
    def _prepare_features(self, data):
        processed = []
        for item in data:
            processed.append([
                item.get("risk_score", 0),
                item.get("violations", 0),
                1 if item.get("compliance_status") == "Compliant" else 0
            ])
        return processed

    def evaluate_company(self, company: dict):
        company_id = company.get("company_id") or str(uuid.uuid4())
        company_name = company.get("company_name")
        company_industry = company.get("company_industry")

        # ----------------------------
        # 1️⃣ Compliance Check
        # ----------------------------
        status, violations = check_shariah_compliance(company, THRESHOLDS)

        # ----------------------------
        # 2️⃣ ML Risk Scoring
        # ----------------------------
        risk_score = None
        if model:
            total_assets = company.get("total_assets", 1)
            total_debt = company.get("total_debt", 0)
            total_income = company.get("total_income", 1)
            non_halal_income = company.get("non_halal_income", 0)
            cash = company.get("cash_and_interest_securities", 0)

            X = pd.DataFrame([{
                "debt_ratio": total_debt / max(total_assets, 1),
                "liquidity_ratio": cash / max(total_assets, 1),
                "non_halal_income_ratio": non_halal_income / max(total_income, 1),
                "other_financial_metric1": total_income / max(total_assets, 1),
                "other_financial_metric2": total_debt / max(total_income, 1),
            }])

            try:
                if hasattr(model, "predict_proba"):
                    probs = model.predict_proba(X)
                    risk_score = float(probs[0][1] if probs.shape[1] > 1 else probs[0][0])
                else:
                    risk_score = float(model.predict(X)[0])
            except Exception as e:
                print(f"❌ ML prediction failed: {e}")
                risk_score = None

        # ----------------------------
        # 3️⃣ Anomaly Detection
        # ----------------------------
        anomalies = detect_anomaly(company)

        # ----------------------------
        # 4️⃣ Explainability
        # ----------------------------
        explanation = generate_explanation(company, status, violations, THRESHOLDS)

        # ----------------------------
        # 5️⃣ Save results
        # ----------------------------
        audit_data = {
            "audit_id": str(uuid.uuid4()),
            "tenant_id": self.tenant_id,
            "company_id": company_id,
            "rule_code": "SHARIAH_SCREENING",
            "fatwa_version": None,
            "compliance_status": status,
            "triggered_by": "system",
            "created_at": datetime.utcnow().isoformat(),

            "company_name": company_name,
            "company_industry": company_industry,

            "audit_details": None,
            "violations_count": len(violations) if violations else 0,
            "risk_score": float(risk_score) if risk_score is not None else None,

            "explanation": json.dumps(explanation) if isinstance(explanation, (dict, list)) else explanation,
            "scholar_reviews": None,
            "anomaly_flag": bool(anomalies) if anomalies is not None else None,

            "total_assets": company.get("total_assets"),
            "total_debt": company.get("total_debt"),
            "total_income": company.get("total_income"),
            "non_halal_income": company.get("non_halal_income"),
            "cash_and_interest_securities": company.get("cash_and_interest_securities"),
        }

        save_result(audit_data)
        populate_features(self.tenant_id)

        return {
            "company_id": company_id,
            "status": status,
            "violations": violations,
            "risk_score": risk_score,
            "anomalies": anomalies,
            "explanation": explanation,
        }