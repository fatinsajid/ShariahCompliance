from services.shariah_rules import SHARIAH_RULES
def check_shariah_compliance(company, thresholds):
    violations = []

    if company.get("total_debt", 0) / max(company.get("total_assets", 1), 1) > THRESHOLDS["debt_ratio"]:
        violations.append({
            "rule": "debt_ratio",
            "message": "Debt exceeds threshold",
            "value": company.get("total_debt"),
            "threshold": THRESHOLDS["debt_ratio"]
        })

    if company.get("non_halal_income", 0) / max(company.get("total_income", 1), 1) > THRESHOLDS[
        "non_halal_income_ratio"]:
        violations.append({
            "rule": "non_halal_income_ratio",
            "message": "Non-halal income too high",
            "value": company.get("non_halal_income"),
            "threshold": THRESHOLDS["non_halal_income_ratio"]
        })

    # Return status + standardized violations
    status = "non-compliant" if violations else "compliant"
    return status, violations