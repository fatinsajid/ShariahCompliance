from services.explainability_engine import generate_explanation
from services.anomaly_detector import AnomalyDetector
from services.shariah_governance import get_active_fatwa, fatwa_is_approved
from services.audit_logger import log_compliance_decision

# Optional: import ML prediction function
from services.risk_engine import predict_risk_score


class FinalDecisionEngine:

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.anomaly_detector = AnomalyDetector()
        self.anomaly_detector.load()  # load pre-trained model

    def evaluate_company(self, company: dict):
        """
        Returns the full compliance result including:
        - risk_score
        - shariah status
        - anomalies
        - fatwa checks
        - explanation
        """

        company_id = company["company_id"]

        # 1️⃣ ML Risk Score
        try:
            risk_score = predict_risk_score(company)
        except Exception as e:
            risk_score = None
            print(f"❌ Risk prediction failed: {e}")

        # 2️⃣ Shariah Compliance Rule Check
        from config.shariah_thresholds import THRESHOLDS
        from services.shariah_governance import check_shariah_compliance
        status, violations = check_shariah_compliance(company, THRESHOLDS)

        # 3️⃣ Explainability
        explanation = generate_explanation(company, status, violations, THRESHOLDS)

        # 4️⃣ Anomaly Detection
        anomaly_flag = self.anomaly_detector.detect(company)

        # 5️⃣ Governance Layer
        rule_code = "SHARIAH_SCREENING"
        fatwa = get_active_fatwa(rule_code, self.tenant_id)

        fatwa_approved = False
        if fatwa:
            fatwa_id, fatwa_version, _ = fatwa
            fatwa_approved = fatwa_is_approved(fatwa_id)

        # 6️⃣ Audit Logging
        log_compliance_decision(
            tenant_id=self.tenant_id,
            company_id=company_id,
            rule_code=rule_code,
            fatwa_version=fatwa[1] if fatwa else None,
            status=status,
            anomaly=anomaly_flag,
            risk_score=risk_score
        )

        return {
            "company_id": company_id,
            "risk_score": risk_score,
            "shariah_status": status,
            "violations": violations,
            "anomaly_flag": anomaly_flag,
            "fatwa_approved": fatwa_approved,
            "explanation": explanation
        }