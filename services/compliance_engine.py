def check_shariah_compliance(company, thresholds):
    violations = []

    # Qualitative Screening
    if company["sector"] in thresholds["prohibited_sectors"]:
        violations.append("Prohibited business activity")

    # Quantitative Screening
    debt_ratio = company["total_debt"] / company["total_assets"]
    non_halal_ratio = company["non_halal_income"] / company["total_income"]
    liquidity_ratio = company["cash_and_interest_securities"] / company["total_assets"]

    if debt_ratio > thresholds["debt_ratio_max"]:
        violations.append("Debt ratio exceeds 30%")

    if non_halal_ratio > thresholds["non_halal_income_ratio_max"]:
        violations.append("Non-halal income exceeds 5%")

    if liquidity_ratio > thresholds["liquidity_ratio_max"]:
        violations.append("Liquidity ratio exceeds 30%")

    status = "COMPLIANT" if not violations else "NON-COMPLIANT"

    return status, violations
