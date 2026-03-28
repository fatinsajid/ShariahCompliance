# services/final_decision_engine.py

import pandas as pd
from dal.db_connector import (
    save_result,
    populate_features,
    fetch_companies,
)
from services.compliance_engine import check_shariah_compliance
from services.explainability_engine import generate_explanation
from services.shariah_governance import get_active_fatwa, fatwa_is_approved, log_compliance_decision
from services.anomaly_detector import detect_anomaly

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
        company_id = company.get("company_id")

        # 1️⃣ Compliance Check
        status, violations = check_shariah_compliance(company, THRESHOLDS)

        # 2️⃣ ML Risk Scoring (unchanged)
        risk_score = None
        if model:
            X = pd.DataFrame([{
                "debt_ratio": company.get("total_debt", 0) / max(company.get("total_assets", 1), 1),
                "liquidity_ratio": company.get("cash_and_interest_securities", 0) / max(company.get("total_assets", 1),
                                                                                        1),
                "non_halal_income_ratio": company.get("non_halal_income", 0) / max(company.get("total_income", 1), 1),
                "other_financial_metric1": company.get("total_income", 0) / max(company.get("total_assets", 1), 1),
                "other_financial_metric2": company.get("total_debt", 0) / max(company.get("total_income", 1), 1),
            }])
            try:
                risk_score = model.predict_proba(X)[0][1] if hasattr(model, "predict_proba") else model.predict(X)[0]
            except Exception as e:
                print(f"❌ ML prediction failed: {e}")
                risk_score = None

        # 3️⃣ Anomaly Detection
        anomalies = detect_anomaly(company)

        # 4️⃣ Explainability
        explanation = generate_explanation(company, status, violations, THRESHOLDS)

        # 5️⃣ Skipping fatwa logic completely
        fatwa_status = "skipped"

        # 6️⃣ Save results
        save_result(company_id, self.tenant_id, status, violations)
        populate_features(self.tenant_id)

        # 7️⃣ Return combined result
        return {
            "company_id": company_id,
            "status": status,
            "violations": violations,
            "risk_score": float(risk_score) if risk_score is not None else None,
            "anomalies": anomalies,
            "explanation": explanation,
            "fatwa_status": fatwa_status,
        }

        # ----------------------------
        # 3️⃣ Anomaly Detection
        # ----------------------------
        anomalies = detect_anomaly(company)

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