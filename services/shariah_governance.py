# services/shariah_governance.py

from services.compliance_engine import check_shariah_compliance
from services.fatwa_registry import attach_fatwa_metadata
from services.scholar_consensus import compute_scholar_consensus
from services.explainability_engine import generate_ml_explanation
from dal.db_connector import fetch_fatwa_by_id


def fatwa_is_approved(fatwa_id: str, tenant_id: str) -> bool:
    """
    Check whether a fatwa is approved and active.

    Thesis purpose:
    - runtime governance enforcement
    - prevents use of draft/rejected rulings
    - ensures auditability
    """

    try:
        fatwa = fetch_fatwa_by_id(fatwa_id, tenant_id)

        if not fatwa:
            return False

        # expected fields in your fatwa table
        status = fatwa.get("status")
        is_active = fatwa.get("is_active", True)

        return status == "APPROVED" and is_active is True

    except Exception as e:
        print(f"[GOVERNANCE] fatwa approval check failed: {e}")
        return False


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