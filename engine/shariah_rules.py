def evaluate_shariah_compliance(company, thresholds):
    """
    Evaluates Shariah compliance based on AAOIFI quantitative screening.
    """

    violations = []

    debt_ratio = company["total_debt"] / company["total_assets"]
    non_halal_income_ratio = company["non_halal_income"] / company["total_income"]

    if debt_ratio > thresholds["debt_ratio_max"]:
        violations.append(
            f"Debt ratio {debt_ratio:.2%} exceeds allowed {thresholds['debt_ratio_max']:.2%}"
        )

    if non_halal_income_ratio > thresholds["non_halal_income_ratio_max"]:
        violations.append(
            f"Non-halal income ratio {non_halal_income_ratio:.2%} exceeds allowed {thresholds['non_halal_income_ratio_max']:.2%}"
        )

    compliance_status = "COMPLIANT" if not violations else "NON-COMPLIANT"

    return {
        "company_id": company["company_id"],
        "compliance_status": compliance_status,
        "violations": "; ".join(violations)
    }
