# services/shariah_governance.py
from typing import Tuple, List

# ----------------------------
# Mocked Fatwa / Scholar Functions
# ----------------------------

def fetch_fatwa_by_id(fatwa_id: str, tenant_id: str) -> dict:
    """Mock fetching a fatwa by ID"""
    return {
        "fatwa_id": fatwa_id,
        "title": "Sample Fatwa",
        "description": "This is a mocked fatwa description"
    }

def create_scholar_review(tenant_id: str, company_id: str, compliance_result_id: int, scholar_id: str) -> str:
    """Mock creating a scholar review"""
    return "review-123"

def fetch_scholar_reviews(company_id: str, tenant_id: str) -> List[dict]:
    """Mock fetching all scholar reviews for a company"""
    return [
        {
            "review_id": "review-123",
            "scholar_id": "scholar-1",
            "compliance_result_id": 1,
            "violation_code": "non_halal_income",
            "status": "pending",
            "created_at": "2026-02-25T00:00:00"
        }
    ]

def check_shariah_compliance(company: dict, thresholds: dict) -> Tuple[str, List[str]]:
    """
    Mock compliance check
    Returns status and list of violations
    """
    violations = []

    if company["non_halal_income"] / max(company["total_income"], 1) > thresholds.get("non_halal_income_ratio", 0.05):
        violations.append("Non-halal income > threshold")

    if company["cash_and_interest_securities"] / max(company["total_assets"], 1) > thresholds.get("interest_assets_ratio", 0.1):
        violations.append("High interest-bearing assets")

    status = "Non-Compliant" if violations else "Compliant"
    return status, violations
def get_audit_trail(company_id, tenant_id):
    # stub implementation — replace with real audit trail logic later
    return [
        {"action": "compliance_checked", "user": "system", "timestamp": "2026-02-25T00:00:00Z"}
    ]