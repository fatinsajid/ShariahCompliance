from services.shariah_rules import SHARIAH_RULES
def check_shariah_compliance(company: dict, thresholds: dict):
    violations = []
    debt_threshold = thresholds.get("debt_ratio", 0.5)
    non_halal_threshold = thresholds.get("non_halal_income_ratio", 0.05)

    if company.get("total_debt", 0) / max(company.get("total_assets", 1), 1) > thresholds["debt_ratio"]:
        violations.append({
            "rule": "debt_ratio",
            "message": "Debt exceeds threshold",
            "value": company.get("total_debt"),
            "threshold": thresholds["debt_ratio"]
        })

    if company.get("non_halal_income", 0) / max(company.get("total_income", 1), 1) > thresholds[
        "non_halal_income_ratio"]:
        violations.append({
            "rule": "non_halal_income_ratio",
            "message": "Non-halal income too high",
            "value": company.get("non_halal_income"),
            "threshold": thresholds["non_halal_income_ratio"]
        })

    status = "non-compliant" if violations else "compliant"
    return status, violations