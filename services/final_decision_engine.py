# services/final_decision_engine.py

import pandas as pd
from dal.db_connector import (
    save_result,
    populate_features,
    fetch_companies,
)
from services.compliance_engine import check_shariah_compliance
from services.anomaly_detector import detect_anomaly
from services.explainability_engine import generate_explanation
from services.shariah_governance import get_active_fatwa, fatwa_is_approved, log_compliance_decision

import os
import joblib
import json

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "risk_model_v1.pkl")
THRESHOLDS_PATH = os.path.join(PROJECT_ROOT, "config", "shariah_thresholds.json")

# Load thresholds once
with open(THRESHOLDS_PATH, "r") as f:
    THRESHOLDS = json.load(f)

# Load ML model once
try:
    model = joblib.load(MODEL_PATH)
    print("✅ FinalDecisionEngine: ML model loaded")
except Exception as e:
    model = None
    print(f"❌ FinalDecisionEngine: ML model failed to load: {e}")


class FinalDecisionEngine:
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id

    def evaluate_company(self, company: dict):
        """
        Runs full decision pipeline:
        1. Compliance rule check
        2. ML risk scoring
        3. Anomaly detection
        4. Explainability
        5. Shariah governance binding (fatwa check & audit)
        6. Save results to DB
        """
        company_id = company.get("company_id")

        # ----------------------------
        # 1️⃣ Compliance Check
        # ----------------------------
        status, violations = check_shariah_compliance(company, THRESHOLDS)

        # ----------------------------
        # 2️⃣ ML Risk Scoring
        # ----------------------------
        risk_score = None
        if model:
            X = pd.DataFrame([{
                "total_assets": company.get("total_assets", 0),
                "total_debt": company.get("total_debt", 0),
                "total_income": company.get("total_income", 0),
                "non_halal_income": company.get("non_halal_income", 0),
                "cash_and_interest_securities": company.get("cash_and_interest_securities", 0),
            }])
            try:
                if hasattr(model, "predict_proba"):
                    probs = model.predict_proba(X)
                    risk_score = probs[0][1] if probs.shape[1] > 1 else probs[0][0]
                else:
                    risk_score = model.predict(X)[0]
            except Exception as e:
                risk_score = None
                print(f"❌ ML prediction failed: {e}")

        # ----------------------------
        # 3️⃣ Anomaly Detection
        # ----------------------------
        anomalies = detect_anomalies(company)

        # ----------------------------
        # 4️⃣ Explainability
        # ----------------------------
        explanation = generate_explanation(company, status, violations, THRESHOLDS)

        # ----------------------------
        # 5️⃣ Shariah Governance Binding
        # ----------------------------
        rule_code = "SHARIAH_SCREENING"
        fatwa = get_active_fatwa(rule_code, self.tenant_id)

        fatwa_status = None
        if fatwa:
            fatwa_id, fatwa_version, ruling = fatwa
            if not fatwa_is_approved(fatwa_id):
                fatwa_status = "pending_approval"
            else:
                fatwa_status = "approved"

            # log audit trail
            log_compliance_decision(
                tenant_id=self.tenant_id,
                company_id=company_id,
                rule_code=rule_code,
                fatwa_version=fatwa_version,
                status=status,
            )
        else:
            fatwa_status = "none"

        # ----------------------------
        # 6️⃣ Save results
        # ----------------------------
        save_result(company_id, self.tenant_id, status, violations)
        populate_features(self.tenant_id)

        # ----------------------------
        # 7️⃣ Return combined result
        # ----------------------------
        return {
            "company_id": company_id,
            "status": status,
            "violations": violations,
            "risk_score": float(risk_score) if risk_score is not None else None,
            "anomalies": anomalies,
            "explanation": explanation,
            "fatwa_status": fatwa_status,
        }