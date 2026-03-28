from services.shariah_rules import SHARIAH_RULES

def check_shariah_compliance(company: dict, thresholds: dict):
    violations = []

    # Use .get() to provide default values if keys are missing
    debt_threshold = thresholds.get("debt_ratio", 0.5)
    non_halal_threshold = thresholds.get("non_halal_income_ratio", 0.05)

    # Check debt ratio
    if company.get("total_debt", 0) / max(company.get("total_assets", 1), 1) > debt_threshold:
        violations.append({
            "rule": "debt_ratio",
            "message": "Debt exceeds threshold",
            "value": company.get("total_debt", 0),
            "threshold": debt_threshold
        })

    # Check non-halal income ratio
    if company.get("non_halal_income", 0) / max(company.get("total_income", 1), 1) > non_halal_threshold:
        violations.append({
            "rule": "non_halal_income_ratio",
            "message": "Non-halal income too high",
            "value": company.get("non_halal_income", 0),
            "threshold": non_halal_threshold
        })

    status = "non-compliant" if violations else "compliant"
    return status, violations