# services/shariah_governance.py

from services.compliance_engine import check_shariah_compliance
from services.fatwa_registry import attach_fatwa_metadata
from services.scholar_consensus import compute_scholar_consensus
from services.explainability_engine import generate_ml_explanation
from dal.db_connector import fetch_fatwa_by_id



# ================================
# Compliance Confidence
# ================================

def compute_compliance_confidence(
    violations: list,
    scholar_ratio: float
) -> float:
    """
    Hybrid confidence score combining rule violations
    and scholar agreement.
    """

    base = 0.95 if not violations else max(
        0.5,
        0.95 - 0.1 * len(violations)
    )

    adjusted = base * (0.7 + 0.3 * scholar_ratio)

    return round(adjusted, 3)


# ================================
# Master Governance Pipeline
# ================================

def run_shariah_governance(
    company_data: dict,
    scholar_reviews: list
) -> dict:
    """
    End-to-end Shariah governance pipeline.
    """

    # 1. Core compliance check
    status, violations = check_shariah_compliance(company_data)

    # 2. Scholar consensus
    consensus = compute_scholar_consensus(scholar_reviews)

    # 3. Confidence scoring
    confidence = compute_compliance_confidence(
        violations,
        consensus["approval_ratio"]
    )

    # 4. Explainability layer
    ml_exp = generate_ml_explanation(company_data)

    # 5. Build result
    result = {
        "status": status,
        "violations": violations,
        "scholar_consensus": consensus,
        "confidence_score": confidence,
        **ml_exp
    }

    # 6. Attach fatwa trace
    result = attach_fatwa_metadata(result)

    return result

# services/shariah_governance.py

def log_compliance_decision(tenant_id, company_id, rule_code, fatwa_version, status):
    from dal.db_connector import insert_audit_log
    from datetime import datetime

    insert_audit_log({
        "tenant_id": tenant_id,
        "company_id": company_id,
        "rule_code": rule_code,
        "fatwa_version": fatwa_version,
        "status": status,
        "timestamp": datetime.utcnow()
    })